/**
 * Providers for event models: `eventBridge` adds `publish()`, `defer` adds
 * `defer(timestamp)`. Use one or both:
 *
 * ```ts
 * class PostPublished extends model<PostPublished>()(
 *   "PostPublished",
 *   [eventBridge, defer],
 *   { post: Post },
 * ) {
 *   get source() { return "blog" }
 *   get detailType() { return "blog.post.published" }
 * }
 *
 * yield* new PostPublished({ post }).publish()
 * ```
 *
 * @since 7.0.0
 */
import { makeProvider, type ProviderMembers } from "@model-ts/core"
import type { Effect } from "effect"
import { Deferrer } from "./Deferrer.ts"
import type { PublishErrors, ResultEntry } from "./Errors.ts"
import { EventBridge, type PublishableEvent } from "./EventBridge.ts"

/**
 * What a model needs before it can be published: getters (or fields) for
 * the EventBridge source and detail type.
 *
 * @since 7.0.0
 */
export interface EventAddress {
  readonly source: string
  readonly detailType: string
}

/** @since 7.0.0 */
export interface EventBridgeInstance<Self> {
  /** Publishes this event now. */
  publish(
    this: Self & EventAddress,
  ): Effect.Effect<ReadonlyArray<ResultEntry>, PublishErrors, EventBridge>
}

/** @since 7.0.0 */
export interface EventBridgeMembers extends ProviderMembers {
  readonly instance: EventBridgeInstance<this["Self"]>
}

/** @since 7.0.0 */
export const eventBridge = makeProvider<EventBridgeMembers>()({
  instanceProps: {
    publish() {
      const event = this as PublishableEvent
      return EventBridge.use((bus) => bus.publish([event]))
    },
  },
})

/** @since 7.0.0 */
export interface DeferInstance<Self> {
  /** Publishes this event at `timestamp`, through the `Deferrer` service. */
  defer(
    this: Self & EventAddress,
    timestamp: Date,
  ): Effect.Effect<void, PublishErrors, Deferrer>
}

/** @since 7.0.0 */
export interface DeferMembers extends ProviderMembers {
  readonly instance: DeferInstance<this["Self"]>
}

/** @since 7.0.0 */
export const defer = makeProvider<DeferMembers>()({
  instanceProps: {
    defer(timestamp) {
      const event = this as PublishableEvent
      return Deferrer.use((deferrer) => deferrer.defer(event, timestamp))
    },
  },
})
