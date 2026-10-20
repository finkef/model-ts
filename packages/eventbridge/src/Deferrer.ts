/**
 * "Publish this event later" as a service. The `defer` provider calls it, and
 * the layer decides how a delayed event is carried out: by publishing an
 * envelope event that a scheduler picks up, or by recording it in tests.
 *
 * ```ts
 * // Production: wrap the event in your scheduler's envelope and publish it.
 * const DeferrerLive = Deferrer.layerEventBridge(
 *   (deferred) => new DeferredEventEnvelope(deferred),
 * )
 *
 * yield* reminder.defer(tomorrow)
 * ```
 *
 * @since 7.0.0
 */
import { Context, Effect, Layer } from "effect"
import { EventEncodeError, type PublishErrors } from "./Errors.ts"
import { EventBridge, type PublishableEvent } from "./EventBridge.ts"

/**
 * An event to publish at `timestamp`, with its encoded detail.
 *
 * @since 7.0.0
 */
export interface DeferredEvent {
  readonly timestamp: Date
  readonly event: {
    readonly source: string
    readonly detailType: string
    readonly detail: unknown
  }
}

/**
 * What `Deferrer.layerMemory` has deferred so far.
 *
 * @since 7.0.0
 */
export class DeferredEvents extends Context.Service<
  DeferredEvents,
  { readonly all: Effect.Effect<ReadonlyArray<DeferredEvent>> }
>()("@model-ts/eventbridge/DeferredEvents") {}

/** @since 7.0.0 */
export interface DeferrerService {
  readonly defer: (
    event: PublishableEvent,
    timestamp: Date,
  ) => Effect.Effect<void, PublishErrors>
}

const toDeferred = (event: PublishableEvent, timestamp: Date) =>
  Effect.try({
    try: (): DeferredEvent => {
      if (Number.isNaN(timestamp.getTime())) {
        throw new RangeError("The timestamp is an invalid Date")
      }
      return {
        timestamp,
        event: {
          source: event.source,
          detailType: event.detailType,
          detail: event.encode(),
        },
      }
    },
    catch: (cause) =>
      new EventEncodeError({ detailType: event.detailType, cause }),
  })

/**
 * Builds a `Deferrer` whose `defer` encodes the event, then hands it to
 * `schedule`. Every implementation shares the span and its attributes.
 */
const make = (
  schedule: (deferred: DeferredEvent) => Effect.Effect<void, PublishErrors>,
) =>
  Deferrer.of({
    defer: Effect.fn("Deferrer.defer")(function* (event, timestamp) {
      yield* Effect.annotateCurrentSpan(
        "model_ts.detail_type",
        event.detailType,
      )
      const deferred = yield* toDeferred(event, timestamp)
      yield* Effect.annotateCurrentSpan(
        "model_ts.deferred_until",
        deferred.timestamp.toISOString(),
      )
      yield* schedule(deferred)
    }),
  })

/**
 * @since 7.0.0
 */
export class Deferrer extends Context.Service<Deferrer, DeferrerService>()(
  "@model-ts/eventbridge/Deferrer",
) {
  /**
   * Publishes `envelope(deferred)` on the bus right away; whatever consumes
   * that envelope publishes the original event at its timestamp.
   */
  static readonly layerEventBridge = (
    envelope: (deferred: DeferredEvent) => PublishableEvent,
  ): Layer.Layer<Deferrer, never, EventBridge> =>
    Layer.effect(
      Deferrer,
      Effect.gen(function* () {
        const bus = yield* EventBridge
        return make((deferred) =>
          Effect.asVoid(bus.publish([envelope(deferred)])),
        )
      }),
    )

  /** Records deferred events, readable through `DeferredEvents`. */
  static readonly layerMemory: Layer.Layer<Deferrer | DeferredEvents> =
    Layer.effectContext(
      Effect.sync(() => {
        const deferred: Array<DeferredEvent> = []
        return Context.make(
          Deferrer,
          make((entry) =>
            Effect.sync(() => {
              deferred.push(entry)
            }),
          ),
        ).pipe(
          Context.add(
            DeferredEvents,
            DeferredEvents.of({ all: Effect.sync(() => [...deferred]) }),
          ),
        )
      }),
    )
}
