import { model } from "@model-ts/core"
import { Schema } from "effect"
import { defer, type DeferredEvent, eventBridge } from "../src/index.ts"

export class PostPublished extends model<PostPublished>()(
  "PostPublished",
  [eventBridge, defer],
  {
    postId: Schema.String,
    publishedAt: Schema.DateFromString,
  },
) {
  get source() {
    return "blog"
  }
  get detailType() {
    return "blog.post.published"
  }
}

export const postPublished = (n: number) =>
  new PostPublished({
    postId: `post_${String(n).padStart(3, "0")}`,
    publishedAt: new Date("2026-01-01T00:00:00.000Z"),
  })

/** An event whose `postId` cannot be encoded, built past the type system. */
export const unencodable = () =>
  new PostPublished({
    postId: 42 as unknown as string,
    publishedAt: new Date("2026-01-01T00:00:00.000Z"),
  })

/** The envelope a scheduler would consume to publish deferred events. */
export class DeferredEnvelope extends model<DeferredEnvelope>()(
  "DeferredEnvelope",
  {
    timestamp: Schema.DateFromString,
    event: Schema.Struct({
      source: Schema.String,
      detailType: Schema.String,
      detail: Schema.Unknown,
    }),
  },
) {
  get source() {
    return "scheduler"
  }
  get detailType() {
    return "scheduler.deferred"
  }
}

export const envelope = (deferred: DeferredEvent) =>
  new DeferredEnvelope(deferred)
