/**
 * The member types of `EventBridge.provider`, which adds `publish()`:
 *
 * ```ts
 * class PostPublished extends Model.Class<PostPublished>()(
 *   "PostPublished",
 *   EventBridge.provider,
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
import type { Provider } from "@model-ts/core"
import type { Effect } from "effect"
import type { PublishErrors, ResultEntry } from "./Errors.ts"
import type { EventBridge } from "./EventBridge.ts"

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
export interface EventBridgeMembers extends Provider.Members {
  readonly instance: EventBridgeInstance<this["Self"]>
}
