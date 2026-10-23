import { unmarshall } from "@aws-sdk/util-dynamodb"
import { Cause, Clock, Effect, Option, Schema, Stream } from "effect"
import type { GraphQLFormattedError } from "graphql"
import {
  badRequest,
  DeliveryError,
  formatError as defaultFormatError,
  toGraphQLError,
} from "../Errors.ts"
import type { Gateway } from "../Gateway.ts"
import type { Topic } from "../Identifiers.ts"
import {
  PublishedEvent,
  type Connection,
  type SubscriptionOperation,
} from "../Models.ts"
import type { StreamEvent, StreamRecord } from "../Request.ts"
import { Storage, type StorageFailure } from "../Storage.ts"
import type { WebSocketOptions } from "../WebSocket.ts"
import { decodePersisted } from "./persisted.ts"
import { close, send } from "./protocol.ts"
import { parseJson, prepare } from "./request.ts"
import type { DeliveryRequirements } from "./requirements.ts"
import { operationPubSub } from "./source.ts"

const PublishedTag = Schema.Struct({
  _tag: Schema.Struct({ S: Schema.Literal("GraphQLPublishedEvent") }),
})
const StringAttribute = Schema.Struct({ S: Schema.String })
const PublishedImage = Schema.Struct({
  _tag: StringAttribute,
  id: StringAttribute,
  topic: StringAttribute,
  payload: StringAttribute,
  ttl: Schema.Struct({ N: Schema.String }),
})

type DeliveryFailure = Schema.SchemaError | StorageFailure | DeliveryError

/** Delivery reconstructs current context for each subscriber. */
const deliverEvent = <C, L, P, E, R>(
  options: WebSocketOptions<C, L, P, E, R>,
  event: PublishedEvent,
  record: StreamRecord,
  lambdaContext: L,
) =>
  Effect.gen(function* () {
    const storage = yield* Storage
    const now = yield* Clock.currentTimeMillis
    if (event.ttl <= now / 1000) return
    const payload = yield* parseJson(event.payload).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)),
    )
    const failures: Array<DeliveryError["failures"][number]> = []
    yield* storage.subscribers(event.topic, now).pipe(
      Stream.runForEach((subscription) =>
        Effect.gen(function* () {
          const connection = yield* storage
            .liveConnection(subscription.connectionId)
            .pipe(
              Effect.catchTag("TransportError", () =>
                storage
                  .disconnect(subscription.connectionId)
                  .pipe(Effect.as(undefined)),
              ),
            )
          if (!connection) return
          const operation = yield* storage.operation(
            connection.id,
            subscription.operationId,
          )
          if (
            !operation ||
            operation.state !== "active" ||
            operation.ttl <= now / 1000 ||
            !operation.topics.includes(event.topic)
          )
            return
          yield* deliverOperation(
            options,
            connection,
            operation,
            { topic: event.topic, payload },
            record,
            lambdaContext,
          )
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.gen(function* () {
              if (Cause.hasInterrupts(cause)) return yield* Effect.interrupt
              failures.push({
                connectionId: subscription.connectionId,
                operationId: subscription.operationId,
                cause: Cause.squash(cause),
              })
              yield* Effect.logError(
                "GraphQL subscriber delivery failed",
                cause,
              )
            }),
          ),
        ),
      ),
    )
    if (failures.length)
      return yield* new DeliveryError({ eventId: event.id, failures })
  }).pipe(Effect.withSpan("GraphQL.deliver"))

/**
 * Enable ReportBatchItemFailures on the event source. Failed records retry; successful
 * subscribers may receive duplicates. Configure retry age/count and a DLQ in CDK.
 */
export function streamHandler<
  C,
  L,
  P,
  const O extends WebSocketOptions<C, L, P, unknown, unknown>,
>(
  options: O & WebSocketOptions<C, L, P, unknown, unknown>,
): (
  event: StreamEvent,
  lambdaContext: L,
) => Effect.Effect<
  { batchItemFailures: Array<{ itemIdentifier: string }> },
  DeliveryFailure,
  DeliveryRequirements<O> | Storage | Gateway
>
export function streamHandler<C, L, P>(
  options: WebSocketOptions<C, L, P, unknown, unknown>,
) {
  const process = Effect.fn("GraphQL.streamRecord")(function* (
    record: StreamRecord,
    lambdaContext: L,
  ) {
    if (record.eventName !== "INSERT" || !record.dynamodb?.NewImage) return
    // Accept Lambda's wire JSON; only our event model's scalar attributes are needed.
    // Validating this boundary also avoids casts between Lambda and SDK binary types.
    if (!Schema.is(PublishedTag)(record.dynamodb.NewImage)) return
    const image = yield* Schema.decodeUnknownEffect(PublishedImage)(
      record.dynamodb.NewImage,
    )
    const item = yield* Effect.try({
      try: () => unmarshall(image),
      catch: () => badRequest("Invalid DynamoDB stream image"),
    })
    const event = yield* Schema.decodeUnknownEffect(PublishedEvent)(item)
    yield* deliverEvent(options, event, record, lambdaContext)
  })
  return Effect.fn("GraphQL.stream")(function* (
    event: StreamEvent,
    lambdaContext: L,
  ) {
    const batchItemFailures: Array<{ itemIdentifier: string }> = []
    for (const record of event.Records)
      yield* process(record, lambdaContext).pipe(
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            if (Cause.hasInterrupts(cause)) return yield* Effect.interrupt
            yield* Effect.logError("GraphQL stream record failed", cause)
            const sequence = record.dynamodb?.SequenceNumber
            if (!sequence) return yield* Effect.failCause(cause)
            batchItemFailures.push({ itemIdentifier: sequence })
          }),
        ),
      )
    return { batchItemFailures }
  })
}

/** Same validation, context factory, subscribe guards and field authorization as registration. */
const deliverOperation = <C, L, P, E, R>(
  options: WebSocketOptions<C, L, P, E, R>,
  connection: Connection,
  operation: SubscriptionOperation,
  event: { readonly topic: Topic; readonly payload: Schema.Json },
  record: StreamRecord,
  lambdaContext: L,
) =>
  Effect.gen(function* () {
    const storage = yield* Storage
    const persisted = yield* decodePersisted(
      options.persistedContext,
      connection.context,
    ).pipe(
      Effect.map(Option.some),
      Effect.catchTag("PersistedContextError", (error) =>
        Effect.gen(function* () {
          yield* Effect.logError(
            "GraphQL persisted context no longer decodes",
            error,
          )
          // Retain state if closing fails so stream delivery can retry DeleteConnection.
          yield* close(connection, "Internal")
          yield* storage.disconnect(connection.id)
          return Option.none<P>()
        }),
      ),
    )
    if (Option.isNone(persisted)) return
    const format = options.formatError ?? defaultFormatError
    const result = yield* Effect.gen(function* () {
      const prepared = yield* prepare(
        options.executor,
        operation.request,
        options,
      )
      if (prepared.errors.length) return { errors: prepared.errors.map(format) }
      const { pubSub } = operationPubSub(event)
      const context = yield* options.context({
        event: record,
        lambdaContext,
        connection,
        persisted: persisted.value,
        pubSub,
        response: { headers: {}, cookies: [] },
      })
      const result = yield* options.executor
        .subscribe({
          document: prepared.document,
          request: operation.request,
          context,
        })
        .pipe(Stream.runHead)
      return Option.getOrUndefined(result)
    }).pipe(
      Effect.catch((error) =>
        Effect.succeed({ errors: [format(toGraphQLError(error))] }),
      ),
      Effect.catchCause((cause) =>
        Effect.gen(function* () {
          if (Cause.hasInterrupts(cause)) return yield* Effect.interrupt
          yield* Effect.logError("GraphQL subscriber execution failed", cause)
          // Do not invoke application formatters again: they can be the defect's source.
          return {
            errors: [
              {
                message: "Internal server error",
                extensions: { code: "INTERNAL_SERVER_ERROR" },
              },
            ],
          }
        }),
      ),
    )
    // Infrastructure failures still reach the stream retry policy. Only execution failures
    // become GraphQL results; retrying a permission denial cannot make it succeed.
    const current = yield* storage.operation(
      connection.id,
      operation.operationId,
    )
    if (
      !result ||
      current?.generation !== operation.generation ||
      current.state !== "active"
    )
      return
    const failOperation = (errors: ReadonlyArray<GraphQLFormattedError>) =>
      Effect.gen(function* () {
        // Keep the operation until the terminal frame is accepted so gateway failures retry.
        yield* send(connection, {
          type: "error",
          id: operation.operationId,
          payload: errors,
        })
        yield* storage.stop(
          connection.id,
          operation.operationId,
          operation.generation,
        )
      })
    yield* (
      (!("data" in result) || result.data === undefined) &&
      result.errors?.length
        ? failOperation(result.errors)
        : send(connection, {
            type: "next",
            id: operation.operationId,
            payload: result,
          })
    ).pipe(
      Effect.catchTag("MessageTooLarge", (error) =>
        // Retrying a result that cannot fit a frame only duplicates healthy deliveries.
        failOperation([defaultFormatError(toGraphQLError(error))]).pipe(
          Effect.catchTag("MessageTooLarge", () =>
            Effect.gen(function* () {
              // An ID alone can exceed a frame; only socket closure can terminate it.
              yield* close(connection, "Internal")
              yield* storage.disconnect(connection.id)
            }),
          ),
        ),
      ),
    )
  })
