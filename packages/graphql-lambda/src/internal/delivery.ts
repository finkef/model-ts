import { unmarshall } from "@aws-sdk/util-dynamodb"
import { Cause, Clock, Effect, Schema, Stream } from "effect"
import { badRequest, DeliveryError } from "../Errors.ts"
import type { Gateway } from "../Gateway.ts"
import { PublishedEvent } from "../Models.ts"
import type { StreamEvent, StreamRecord } from "../Request.ts"
import { Storage, type StorageFailure } from "../Storage.ts"
import { deliverOperation, type WebSocketOptions } from "../WebSocket.ts"
import { parseJson } from "./request.ts"
import type { DeliveryRequirements } from "./requirements.ts"

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
