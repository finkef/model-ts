import { randomUUID } from "node:crypto"
import { DynamoDB, Operation } from "@model-ts/dynamodb"
import { Clock, Effect, Layer, Stream } from "effect"
import {
  badRequest,
  StorageError,
  TransportError,
  ProtocolClose,
} from "../Errors.ts"
import {
  OperationGeneration,
  type ConnectionId,
  type OperationId,
  type Topic,
} from "../Identifiers.ts"
import {
  Connection,
  Subscription,
  SubscriptionOperation,
  type OperationRequest,
} from "../Models.ts"
import { Storage, type StorageService } from "../Storage.ts"

const liveConnection = Effect.fn("GraphQL.connection")(function* (
  id: ConnectionId,
) {
  const connection = yield* Connection.get(Connection.key(id), {
    consistentRead: true,
  }).pipe(
    Effect.catchTag("ItemNotFoundError", () =>
      Effect.fail(
        new TransportError({
          message: "Connection not found",
          code: "UNAUTHORIZED",
          status: 401,
        }),
      ),
    ),
  )
  const now = yield* Clock.currentTimeMillis
  if (connection.ttl <= now / 1000)
    return yield* new TransportError({
      message: "Connection expired",
      code: "UNAUTHORIZED",
      status: 401,
    })
  return connection
})

const beginInit = Effect.fn("GraphQL.beginInit")(function* (
  connection: Connection,
) {
  if (connection.initReceived)
    return yield* new ProtocolClose({ reason: "TooManyInitRequests" })
  return yield* connection
    .update({ initReceived: true })
    .pipe(
      Effect.catchTag("RaceConditionError", () =>
        Effect.fail(new ProtocolClose({ reason: "TooManyInitRequests" })),
      ),
    )
})

const operations = (id: ConnectionId) =>
  SubscriptionOperation.query({
    KeyConditionExpression: "PK = :pk AND begins_with(SK, :prefix)",
    ExpressionAttributeValues: {
      ":pk": Connection.key(id).PK,
      ":prefix": "OPERATION#",
    },
    ConsistentRead: true,
    fetchAllPages: true,
  })

const removeOperation = (operation: SubscriptionOperation) => [
  Operation.delete(operation),
  ...operation.topics.map((topic) =>
    Operation.delete(
      Subscription.key(topic, operation.connectionId, operation.operationId),
    ),
  ),
]

/** Delete the connection first: a racing registration must fail its connection version check. */
const disconnect = Effect.fn("GraphQL.disconnect")(function* (
  id: ConnectionId,
) {
  yield* Connection.delete(Connection.key(id))
  const existing = yield* operations(id)
  yield* DynamoDB.bulk(existing.map(removeOperation))
})

/** Reserve before application work; connection versions serialize competing reservations. */
const reserve = Effect.fn("GraphQL.reserve")(function* (
  id: ConnectionId,
  operationId: OperationId,
  request: OperationRequest,
  maxOperations: number,
) {
  const generation = OperationGeneration.make(randomUUID())
  for (let attempt = 0; ; attempt++) {
    const done = yield* Effect.gen(function* () {
      const connection = yield* liveConnection(id)
      const existing = yield* operations(id)
      const previous = existing.find(
        (operation) => operation.operationId === operationId,
      )
      if (previous)
        return yield* new ProtocolClose({ reason: "DuplicateSubscriber" })
      if (existing.length >= maxOperations)
        return yield* badRequest("Too many subscription operations")
      const pending = new SubscriptionOperation({
        connectionId: id,
        operationId,
        request,
        generation,
        state: "pending",
        topics: [],
        ttl: connection.ttl,
      })
      const [, guard] = connection.applyUpdate({
        initialized: connection.initialized,
      })
      yield* DynamoDB.bulk([
        [...(Array.isArray(guard) ? guard : [guard]), pending.operation("put")],
      ])
      return true
    }).pipe(
      Effect.catchTag("TransactionCanceledError", (error) =>
        attempt < 3 ? Effect.succeed(false) : Effect.fail(error),
      ),
    )
    if (done) return generation
  }
})

const loadOperation = (id: ConnectionId, operationId: OperationId) =>
  SubscriptionOperation.get(SubscriptionOperation.key(id, operationId), {
    consistentRead: true,
  }).pipe(Effect.catchTag("ItemNotFoundError", () => Effect.succeed(undefined)))

/** A stale setup may retry contention, but can never reserve a new generation. */
const activate = Effect.fn("GraphQL.activate")(function* (
  id: ConnectionId,
  operationId: OperationId,
  generation: OperationGeneration,
  topics: ReadonlyArray<Topic>,
) {
  if (topics.length === 0 || topics.length > 20)
    return yield* badRequest("Subscriptions require between 1 and 20 topics")
  for (let attempt = 0; ; attempt++) {
    const result = yield* Effect.gen(function* () {
      const connection = yield* liveConnection(id)
      const pending = yield* loadOperation(id, operationId)
      if (
        !pending ||
        pending.generation !== generation ||
        pending.state !== "pending"
      )
        return "stale" as const
      const active = new SubscriptionOperation({
        ...pending,
        topics,
        state: "active",
      })
      const [, guard] = connection.applyUpdate({
        initialized: connection.initialized,
      })
      yield* DynamoDB.bulk([
        [
          ...(Array.isArray(guard) ? guard : [guard]),
          active.operation("put", {
            ConditionExpression:
              "#generation = :generation AND #state = :pending",
            ExpressionAttributeNames: {
              "#generation": "generation",
              "#state": "state",
            },
            ExpressionAttributeValues: {
              ":generation": generation,
              ":pending": "pending",
            },
          }),
          ...topics.map((topic) =>
            new Subscription({
              topic,
              connectionId: id,
              operationId,
              ttl: connection.ttl,
            }).operation("put", { ignoreExistence: true }),
          ),
        ],
      ])
      return "active" as const
    }).pipe(
      Effect.catchTag("TransactionCanceledError", (error) =>
        attempt < 3 ? Effect.succeed("retry" as const) : Effect.fail(error),
      ),
    )
    if (result !== "retry") return result === "active"
  }
})

const stop = Effect.fn("GraphQL.stop")(function* (
  id: ConnectionId,
  operationId: OperationId,
  generation?: OperationGeneration,
) {
  for (let attempt = 0; ; attempt++) {
    const done = yield* Effect.gen(function* () {
      const connection = yield* Connection.get(Connection.key(id), {
        consistentRead: true,
      }).pipe(
        Effect.catchTag("ItemNotFoundError", () => Effect.succeed(undefined)),
      )
      const operation = yield* SubscriptionOperation.get(
        SubscriptionOperation.key(id, operationId),
        { consistentRead: true },
      ).pipe(
        Effect.catchTag("ItemNotFoundError", () => Effect.succeed(undefined)),
      )
      if (
        !operation ||
        (generation !== undefined && operation.generation !== generation)
      )
        return "stale" as const
      const guard = connection?.applyUpdate({
        initialized: connection.initialized,
      })[1]
      yield* DynamoDB.bulk([
        [
          ...(guard ? (Array.isArray(guard) ? guard : [guard]) : []),
          ...removeOperation(operation),
        ],
      ])
      return "removed" as const
    }).pipe(
      Effect.catchTag("TransactionCanceledError", (error) =>
        attempt < 3 ? Effect.succeed("retry" as const) : Effect.fail(error),
      ),
    )
    if (done !== "retry") return done === "removed"
  }
})

/** A bounded page is loaded at a time; expired rows never reach the delivery loop. */
const subscribers = (topic: Topic, now: number) =>
  Subscription.stream({
    KeyConditionExpression: "PK = :pk",
    ExpressionAttributeValues: { ":pk": Subscription.partitionKey(topic) },
    ConsistentRead: true,
    Limit: 50,
  }).pipe(Stream.filter((subscription) => subscription.ttl > now / 1000))

export const storageLayer = Layer.effect(
  Storage,
  Effect.gen(function* () {
    const database = yield* DynamoDB
    const failure = <E>(operation: string, cause: E) =>
      cause instanceof TransportError || cause instanceof ProtocolClose
        ? cause
        : new StorageError({ operation, cause })
    const run = <A, E>(
      operation: string,
      effect: Effect.Effect<A, E, DynamoDB>,
    ) =>
      effect.pipe(
        Effect.provideService(DynamoDB, database),
        Effect.mapError((cause) => failure(operation, cause)),
        Effect.withSpan(`GraphQL.storage.${operation}`),
      )
    return {
      create: (connection) => run("create", connection.put()),
      beginInit: (connection) => run("beginInit", beginInit(connection)),
      update: (connection, changes) =>
        run("update", connection.update(changes)),
      liveConnection: (id) => run("liveConnection", liveConnection(id)),
      operation: (id, operationId) =>
        run("operation", loadOperation(id, operationId)),
      disconnect: (id) => run("disconnect", disconnect(id)),
      reserve: (id, operationId, request, maxOperations) =>
        run("reserve", reserve(id, operationId, request, maxOperations)),
      activate: (id, operationId, generation, topics) =>
        run("activate", activate(id, operationId, generation, topics)),
      stop: (id, operationId, generation) =>
        run("stop", stop(id, operationId, generation)),
      subscribers: (topic, now) =>
        subscribers(topic, now).pipe(
          Stream.provideService(DynamoDB, database),
          Stream.mapError((cause) => failure("subscribers", cause)),
        ),
    } satisfies StorageService
  }),
)
