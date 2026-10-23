import { unmarshall } from "@aws-sdk/util-dynamodb"
import { Clock, Effect, Schema, Stream } from "effect"
import { badRequest, DeliveryError } from "./Errors.ts"
import type { Gateway } from "./Gateway.ts"
import type { Topic } from "./Identifiers.ts"
import { createPublishedEvent } from "./internal/publication.ts"
import { parseJson } from "./internal/request.ts"
import type { DeliveryRequirements } from "./internal/requirements.ts"
import { PublishedEvent } from "./Models.ts"
import type { DirectEvent, StreamEvent, StreamRecord } from "./Request.ts"
import { Storage, type StorageFailure } from "./Storage.ts"
import { deliverOperation, type WebSocketOptions } from "./WebSocket.ts"

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

/** Both entry points use this loop, including context reconstruction and Gone cleanup. */
const deliverEvent = <C, L, E, R>(
  options: WebSocketOptions<C, L, E, R>,
  event: PublishedEvent,
  record: StreamRecord | DirectEvent,
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
          Effect.catch((error) =>
            Effect.gen(function* () {
              failures.push({
                connectionId: subscription.connectionId,
                operationId: subscription.operationId,
                cause: error,
              })
              yield* Effect.logError(
                "GraphQL subscriber delivery failed",
                error,
              )
            }),
          ),
        ),
      ),
    )
    if (failures.length)
      return yield* new DeliveryError({ eventId: event.id, failures })
  }).pipe(Effect.withSpan("GraphQL.deliver"))

/** Deliver within the publishing invocation, without writing an Events row. */
export function createDirectPublisher<
  C,
  L,
  const O extends WebSocketOptions<C, L, unknown, unknown>,
>(
  options: O & WebSocketOptions<C, L, unknown, unknown>,
): (
  topic: Topic,
  payload: unknown,
  lambdaContext: L,
  ttlSeconds?: number,
) => Effect.Effect<
  PublishedEvent,
  DeliveryFailure,
  DeliveryRequirements<O> | Storage | Gateway
>
export function createDirectPublisher<C, L>(
  options: WebSocketOptions<C, L, unknown, unknown>,
) {
  return Effect.fn("GraphQL.publishDirect")(function* (
    topic: Topic,
    payload: unknown,
    lambdaContext: L,
    ttlSeconds = 7200,
  ) {
    const event = yield* createPublishedEvent(topic, payload, ttlSeconds)
    yield* deliverEvent(
      options,
      event,
      { delivery: "direct", publishedEvent: event },
      lambdaContext,
    )
    return event
  })
}

/**
 * Enable ReportBatchItemFailures on the event source. Failed records retry; successful
 * subscribers may receive duplicates. Configure retry age/count and a DLQ in CDK.
 */
export function createStreamHandler<
  C,
  L,
  const O extends WebSocketOptions<C, L, unknown, unknown>,
>(
  options: O & WebSocketOptions<C, L, unknown, unknown>,
): (
  event: StreamEvent,
  lambdaContext: L,
) => Effect.Effect<
  { batchItemFailures: Array<{ itemIdentifier: string }> },
  DeliveryFailure,
  DeliveryRequirements<O> | Storage | Gateway
>
export function createStreamHandler<C, L>(
  options: WebSocketOptions<C, L, unknown, unknown>,
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
        Effect.catch((error) =>
          Effect.gen(function* () {
            yield* Effect.logError("GraphQL stream record failed", error)
            const sequence = record.dynamodb?.SequenceNumber
            if (!sequence) return yield* error
            batchItemFailures.push({ itemIdentifier: sequence })
          }),
        ),
      )
    return { batchItemFailures }
  })
}
