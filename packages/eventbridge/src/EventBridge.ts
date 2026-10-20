/**
 * The event bus as an Effect service. Publishing requires `EventBridge`, so
 * the bus is chosen where the program runs: `EventBridge.layer(...)` in
 * production, `EventBridge.layerMemory` in tests.
 *
 * @since 7.0.0
 */
import {
  EventBridgeClient,
  type EventBridgeClientConfig,
  PutEventsCommand,
  type PutEventsRequestEntry,
} from "@aws-sdk/client-eventbridge"
import { type Config, Context, Effect, Layer } from "effect"
import {
  EventBridgeError,
  EventEncodeError,
  PublishError,
  type PublishErrors,
  type ResultEntry,
} from "./Errors.ts"

/**
 * Anything that can be published: an address and an encoder. Every model
 * with `source` and `detailType` getters qualifies.
 *
 * @since 7.0.0
 */
export interface PublishableEvent {
  readonly source: string
  readonly detailType: string
  encode(): unknown
}

/** @since 7.0.0 */
export interface EventBridgeOptions {
  readonly eventBusName: string
  /** Passed to `new EventBridgeClient(...)`. */
  readonly client?: EventBridgeClientConfig | undefined
}

/** @since 7.0.0 */
export interface EventBridgeService {
  readonly eventBusName: string
  /**
   * Publishes events in PutEvents calls of up to 10 entries, in order.
   * Fails with `PublishError` if any entry was rejected.
   */
  readonly publish: (
    events: ReadonlyArray<PublishableEvent>,
  ) => Effect.Effect<ReadonlyArray<ResultEntry>, PublishErrors>
}

/**
 * An event as the in-memory bus recorded it, with the decoded `detail`.
 *
 * @since 7.0.0
 */
export interface PublishedEvent {
  readonly eventBusName: string
  readonly source: string
  readonly detailType: string
  readonly detail: unknown
}

/**
 * What `EventBridge.layerMemory` has published so far.
 *
 * @since 7.0.0
 */
export class PublishedEvents extends Context.Service<
  PublishedEvents,
  { readonly all: Effect.Effect<ReadonlyArray<PublishedEvent>> }
>()("@model-ts/eventbridge/PublishedEvents") {}

/**
 * @since 7.0.0
 */
export class EventBridge extends Context.Service<
  EventBridge,
  EventBridgeService
>()("@model-ts/eventbridge/EventBridge") {
  /** Publishes to `eventBusName`. The client is destroyed when the scope closes. */
  static readonly layer = (
    options: EventBridgeOptions,
  ): Layer.Layer<EventBridge> =>
    Layer.effect(
      EventBridge,
      Effect.gen(function* () {
        const client = yield* Effect.acquireRelease(
          Effect.sync(() => new EventBridgeClient(options.client ?? {})),
          (client) => Effect.sync(() => client.destroy()),
        )
        return make(options.eventBusName, (entries) =>
          Effect.tryPromise({
            try: (signal) =>
              client.send(new PutEventsCommand({ Entries: [...entries] }), {
                abortSignal: signal,
              }),
            catch: (cause) => new EventBridgeError({ cause }),
          }),
        )
      }),
    )

  /** Like `layer`, with options read from `Config` when the layer is built. */
  static readonly layerConfig = (
    options: Config.Config<EventBridgeOptions>,
  ): Layer.Layer<EventBridge, Config.ConfigError> =>
    Layer.unwrap(
      Effect.gen(function* () {
        return EventBridge.layer(yield* options)
      }),
    )

  /**
   * A bus that keeps every event in memory, readable through
   * `PublishedEvents`. Events are encoded and batched exactly as by `layer`.
   */
  static readonly layerMemory: Layer.Layer<EventBridge | PublishedEvents> =
    Layer.effectContext(
      Effect.sync(() => {
        const published: Array<PublishedEvent> = []
        let nextId = 0
        const bus = make("memory", (entries) =>
          Effect.sync(() => {
            for (const entry of entries) {
              published.push({
                eventBusName: entry.EventBusName!,
                source: entry.Source!,
                detailType: entry.DetailType!,
                detail: JSON.parse(entry.Detail!),
              })
            }
            return {
              FailedEntryCount: 0,
              Entries: entries.map(() => ({ EventId: `memory-${++nextId}` })),
            }
          }),
        )
        return Context.make(EventBridge, bus).pipe(
          Context.add(
            PublishedEvents,
            PublishedEvents.of({ all: Effect.sync(() => [...published]) }),
          ),
        )
      }),
    )
}

/** PutEvents accepts at most 10 entries per call. */
const MAX_ENTRIES = 10

type PutEvents = (
  entries: ReadonlyArray<PutEventsRequestEntry>,
) => Effect.Effect<
  {
    readonly FailedEntryCount?: number | undefined
    readonly Entries?: ReadonlyArray<ResultEntry> | undefined
  },
  EventBridgeError
>

const make = (eventBusName: string, putEvents: PutEvents) => {
  const toEntry = (event: PublishableEvent) =>
    Effect.try({
      try: (): PutEventsRequestEntry => ({
        EventBusName: eventBusName,
        Source: event.source,
        DetailType: event.detailType,
        Detail: JSON.stringify(event.encode()),
      }),
      catch: (cause) =>
        new EventEncodeError({ detailType: event.detailType, cause }),
    })

  const send = (entries: ReadonlyArray<PutEventsRequestEntry>) =>
    putEvents(entries).pipe(
      Effect.withSpan("EventBridge.PutEvents", {
        kind: "producer",
        attributes: {
          "rpc.system": "aws-api",
          "rpc.service": "EventBridge",
          "rpc.method": "PutEvents",
          "messaging.system": "aws.eventbridge",
          "messaging.operation.name": "PutEvents",
          "messaging.destination.name": eventBusName,
          "messaging.batch.message_count": entries.length,
          "model_ts.detail_types": [
            ...new Set(entries.map((entry) => entry.DetailType!)),
          ],
        },
      }),
    )

  const publish = Effect.fn("EventBridge.publish")(function* (
    events: ReadonlyArray<PublishableEvent>,
  ) {
    yield* Effect.annotateCurrentSpan({
      "messaging.batch.message_count": events.length,
    })
    // Encode everything first, so a bad event fails before anything is sent.
    const entries = yield* Effect.forEach(events, toEntry)
    const results: Array<ResultEntry> = []
    let failedEntryCount = 0
    for (let i = 0; i < entries.length; i += MAX_ENTRIES) {
      const output = yield* send(entries.slice(i, i + MAX_ENTRIES))
      failedEntryCount += output.FailedEntryCount ?? 0
      results.push(...(output.Entries ?? []))
    }
    if (failedEntryCount > 0) {
      return yield* new PublishError({ failedEntryCount, entries: results })
    }
    return results
  })

  return EventBridge.of({ eventBusName, publish })
}

/**
 * Publishes several events at once, in PutEvents calls of up to 10.
 *
 * ```ts
 * yield* publish(new PostPublished({ post }), new FeedUpdated({ feed }))
 * ```
 *
 * @since 7.0.0
 */
export const publish = (
  ...events: ReadonlyArray<PublishableEvent>
): Effect.Effect<ReadonlyArray<ResultEntry>, PublishErrors, EventBridge> =>
  EventBridge.use((bus) => bus.publish(events))
