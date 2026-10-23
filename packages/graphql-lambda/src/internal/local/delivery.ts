import type { StreamRecord } from "@model-ts/dynamodb/Stream"
import { Cause, Context, Deferred, Effect, Layer, Schema, Stream } from "effect"
import type { StreamEvent } from "../../Request.ts"
import { Sandbox } from "../../Sandbox.ts"
import {
  invocationContext,
  type ContextOptions,
  invocation,
  type LambdaContext,
} from "./context.ts"

export class DeliveryStoppedError extends Schema.TaggedError<DeliveryStoppedError>()(
  "DeliveryStoppedError",
  { cause: Schema.Defect() },
) {}

export type StreamHandler<L = LambdaContext, E = never, R = never> = (
  event: StreamEvent,
  context: L,
) => Effect.Effect<
  {
    readonly batchItemFailures: ReadonlyArray<{
      readonly itemIdentifier: string
    }>
  },
  E,
  R
>

export interface DeliveryOptions {
  /** Maximum records per invocation. Defaults to 100. */
  readonly batchSize?: number
}

export class Delivery extends Context.Service<
  Delivery,
  {
    /** Wait for an attempt of every write committed before this Effect starts. */
    readonly settled: Effect.Effect<void, DeliveryStoppedError>
  }
>()("@model-ts/graphql-lambda/Local/Delivery") {}

const MarkerKey = Schema.Struct({ PK: Schema.Struct({ S: Schema.String }) })
const MarkerPrefix = "GRAPHQL#LOCAL#DELIVERY#"
const Marker = Schema.Struct({
  PK: Schema.Struct({ S: Schema.String }),
  barrier: Schema.Struct({ S: Schema.String }),
})

export const delivery = <L = LambdaContext, E = never, R = never>(
  handler: StreamHandler<L, E, R>,
  ...[options]: [LambdaContext] extends [L]
    ? [options?: DeliveryOptions & ContextOptions<L>]
    : [options: DeliveryOptions & ContextOptions<L>]
): Layer.Layer<Delivery, never, R | Sandbox> =>
  makeDelivery(
    (event) => handler(event, invocationContext<L>(options, invocation(event))),
    options,
  )

export const makeDelivery = <E, R>(
  handler: (event: StreamEvent) => Effect.Effect<
    {
      readonly batchItemFailures: ReadonlyArray<{
        readonly itemIdentifier: string
      }>
    },
    E,
    R
  >,
  options: DeliveryOptions = {},
): Layer.Layer<Delivery, never, R | Sandbox> =>
  Layer.effect(
    Delivery,
    Effect.gen(function* () {
      const batchSize = options.batchSize ?? 100
      if (!Number.isSafeInteger(batchSize) || batchSize <= 0)
        return yield* Effect.die(
          new Error("batchSize must be a positive integer"),
        )
      const sandbox = yield* Sandbox
      // Subscribe before returning the layer, even if the consumer has not run yet.
      const changes = yield* sandbox.changes
      const scope = yield* Effect.scope
      const stopped = yield* Deferred.make<never, DeliveryStoppedError>()
      // The scope can close before the forked consumer gets its first turn.
      yield* Effect.addFinalizer(() =>
        Deferred.fail(
          stopped,
          new DeliveryStoppedError({
            cause: new Error("Delivery scope closed"),
          }),
        ),
      )
      const barriers = new Map<string, Deferred.Deferred<void>>()
      // Queue emptiness cannot fence a polled stream: committed writes may not
      // have reached the consumer. A table marker orders settled after them.
      const key = `${MarkerPrefix}${crypto.randomUUID()}`
      const finish = Effect.fn("GraphQL.Local.batch")(function* (
        batch: ReadonlyArray<StreamRecord>,
      ) {
        // All local delivery markers are private, including other instances'
        // markers and REMOVE records produced by cleanup.
        const records = batch.filter((record) => {
          const keys = record.dynamodb.Keys
          return !(
            Schema.is(MarkerKey)(keys) && keys.PK.S.startsWith(MarkerPrefix)
          )
        })
        const result = yield* Effect.exit(
          records.length
            ? Effect.suspend(() => handler({ Records: records }))
            : Effect.succeed({ batchItemFailures: [] }),
        )
        if (result._tag === "Failure") {
          if (Cause.hasInterrupts(result.cause)) return yield* Effect.interrupt
          yield* Effect.logWarning("GraphQL local delivery failed", {
            sequenceNumbers: records.map(
              (record) => record.dynamodb.SequenceNumber,
            ),
            cause: result.cause,
          })
        } else if (result.value.batchItemFailures.length) {
          yield* Effect.logWarning(
            "GraphQL local delivery reported failed records",
            {
              sequenceNumbers: result.value.batchItemFailures.map(
                (failure) => failure.itemIdentifier,
              ),
            },
          )
        }
        for (const record of batch) {
          const image = record.dynamodb.NewImage
          if (Schema.is(Marker)(image) && image.PK.S === key) {
            const barrier = barriers.get(image.barrier.S)
            if (barrier) yield* Deferred.succeed(barrier, undefined)
          }
        }
      })
      yield* changes.pipe(
        Stream.runForEachArray((records) =>
          Effect.gen(function* () {
            for (let i = 0; i < records.length; i += batchSize)
              yield* finish(records.slice(i, i + batchSize))
          }),
        ),
        Effect.catchCause((cause) =>
          Cause.hasInterrupts(cause)
            ? Effect.interrupt
            : Effect.logWarning("GraphQL local change stream stopped", cause),
        ),
        Effect.ensuring(
          Deferred.fail(
            stopped,
            new DeliveryStoppedError({
              cause: new Error("Delivery scope closed or change stream ended"),
            }),
          ),
        ),
        Effect.forkIn(scope),
      )
      return Delivery.of({
        settled: Effect.fn("GraphQL.Local.settled")(function* () {
          if (yield* Deferred.isDone(stopped))
            return yield* Deferred.await(stopped)
          const id = crypto.randomUUID()
          const barrier = yield* Deferred.make<void>()
          yield* Effect.acquireUseRelease(
            // Acquisition is masked so interruption cannot race a late marker
            // commit against cleanup and leave a row behind.
            Effect.gen(function* () {
              barriers.set(id, barrier)
              yield* sandbox
                .seed({ PK: key, SK: id, barrier: id })
                .pipe(Effect.orDie)
            }),
            () =>
              Deferred.await(barrier).pipe(
                Effect.raceFirst(Deferred.await(stopped)),
              ),
            () => sandbox.remove({ PK: key, SK: id }),
          ).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                barriers.delete(id)
              }),
            ),
          )
        })(),
      })
    }),
  )
