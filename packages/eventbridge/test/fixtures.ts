import { Model } from "@model-ts/core"
import { Schema } from "effect"
import { EventBridge } from "../src/index.ts"

export class PostPublished extends Model.Class<PostPublished>()(
  "PostPublished",
  EventBridge.provider,
  {
    postId: Schema.String.pipe(Schema.check(Schema.isPattern(/^post_/))),
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

/**
 * An event whose `postId` fails its pattern. Construction skips filters, so
 * the event is first checked when it is encoded.
 */
export const unencodable = () =>
  new PostPublished({
    postId: "42",
    publishedAt: new Date("2026-01-01T00:00:00.000Z"),
  })
