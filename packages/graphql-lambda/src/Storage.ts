import { randomUUID } from "node:crypto"
import { DynamoDB, Operation } from "@model-ts/dynamodb"
import { Clock, Context, Effect, Layer, Stream } from "effect"
import { badRequest, StorageError, TransportError } from "./Errors.ts"
import { OperationGeneration } from "./Identifiers.ts"
import type { ConnectionId, OperationId, Topic } from "./Identifiers.ts"
import {
  Connection,
  Subscription,
  SubscriptionOperation,
  type OperationRequest,
  type JsonObject,
} from "./Models.ts"

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

const operations = (id: ConnectionId) =>
  SubscriptionOperation.query({
    KeyConditionExpression: "PK = :pk AND begins_with(SK, :prefix)",
    ExpressionAttributeValues: {
      ":pk": Connection.key(id).PK,
      ":prefix": "OPERATION#",
    },
    ConsistentRead: true,
    FetchAllPages: true,
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

/** Reserve before application work; replacing an ID invalidates the previous generation. */
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
      if (!previous && existing.length >= maxOperations)
        return yield* badRequest("Too many subscription operations")
      const pending = new SubscriptionOperation({
        connectionId: id,
        operationId,
        request,
        generation,
        state: "pending",
        topics: previous?.topics ?? [],
        ttl: connection.ttl,
      })
      const [, guard] = connection.applyUpdate({
        initialized: connection.initialized,
      })
      yield* DynamoDB.bulk([
        [
          ...(Array.isArray(guard) ? guard : [guard]),
          pending.operation("put", { IgnoreExistence: true }),
        ],
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
          ...pending.topics
            .filter((topic) => !topics.includes(topic))
            .map((topic) =>
              Operation.delete(Subscription.key(topic, id, operationId)),
            ),
          ...topics.map((topic) =>
            new Subscription({
              topic,
              connectionId: id,
              operationId,
              ttl: connection.ttl,
            }).operation("put", { IgnoreExistence: true }),
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

export type StorageFailure = StorageError | TransportError

/** Storage owns consistency, expiry, operation limits and complete connection cleanup. */
export interface StorageService {
  readonly create: (
    connection: Connection,
  ) => Effect.Effect<Connection, StorageFailure>
  readonly update: (
    connection: Connection,
    changes: {
      readonly context?: JsonObject
      readonly initParams?: JsonObject
      readonly initialized?: boolean
    },
  ) => Effect.Effect<Connection, StorageFailure>
  readonly liveConnection: (
    id: ConnectionId,
  ) => Effect.Effect<Connection, StorageFailure>
  readonly operation: (
    id: ConnectionId,
    operationId: OperationId,
  ) => Effect.Effect<SubscriptionOperation | undefined, StorageFailure>
  readonly disconnect: (id: ConnectionId) => Effect.Effect<void, StorageFailure>
  readonly reserve: (
    id: ConnectionId,
    operationId: OperationId,
    request: OperationRequest,
    maxOperations: number,
  ) => Effect.Effect<OperationGeneration, StorageFailure>
  readonly activate: (
    id: ConnectionId,
    operationId: OperationId,
    generation: OperationGeneration,
    topics: ReadonlyArray<Topic>,
  ) => Effect.Effect<boolean, StorageFailure>
  readonly stop: (
    id: ConnectionId,
    operationId: OperationId,
    generation?: OperationGeneration,
  ) => Effect.Effect<boolean, StorageFailure>
  readonly subscribers: (
    topic: Topic,
    now: number,
  ) => Stream.Stream<Subscription, StorageFailure>
}

export class Storage extends Context.Service<Storage, StorageService>()(
  "@model-ts/graphql-lambda/Storage",
) {
  /** Uses the application's table service; no client or table is created here. */
  static readonly layer = Layer.effect(
    Storage,
    Effect.gen(function* () {
      const database = yield* DynamoDB
      const failure = (operation: string, cause: unknown) =>
        cause instanceof TransportError
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
}
