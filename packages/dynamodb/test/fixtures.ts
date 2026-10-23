import { Model } from "@model-ts/core"
import { Effect, Schema } from "effect"
import { DynamoDB } from "../src/index.ts"

export const PostId = Schema.String.pipe(Schema.brand("PostId"))
export type PostId = typeof PostId.Type

export class Post extends Model.Class<Post>()("Post", DynamoDB.provider, {
  id: PostId,
  authorId: Schema.String,
  title: Schema.String,
  publishedAt: Schema.NullOr(Schema.DateFromString),
  views: Schema.Number.pipe(
    Schema.withDecodingDefaultKey(Effect.succeed(0)),
    Schema.withConstructorDefault(Effect.succeed(0)),
  ),
}) {
  get PK() {
    return `POST#${this.id}`
  }
  get SK() {
    return "POST"
  }
  get GSI2PK() {
    return `AUTHOR#${this.authorId}`
  }
  get GSI2SK() {
    return `POST#${this.id}`
  }

  static key(id: PostId) {
    return { PK: `POST#${id}`, SK: "POST" }
  }
}

export class Comment extends Model.Class<Comment>()(
  "Comment",
  DynamoDB.provider,
  {
    postId: PostId,
    id: Schema.String,
    body: Schema.String,
  },
) {
  get PK() {
    return `POST#${this.postId}`
  }
  get SK() {
    return `COMMENT#${this.id}`
  }
}

/** A post and its comments share a partition. */
export const PostContent = Model.Union([Post, Comment], DynamoDB.provider)

/** A model that keys itself by a field, so updates can move it. */
export class Slug extends Model.Class<Slug>()("Slug", DynamoDB.provider, {
  slug: Schema.String,
  postId: PostId,
}) {
  get PK() {
    return `SLUG#${this.slug}`
  }
  get SK() {
    return "SLUG"
  }
}

export const makePost = (n: number, values: Partial<Post> = {}) =>
  new Post({
    id: PostId.make(`post_${String(n).padStart(3, "0")}`),
    authorId: "ada",
    title: `Post ${n}`,
    publishedAt: null,
    ...values,
  })

export const makeComment = (post: Post, n: number) =>
  new Comment({ postId: post.id, id: String(n), body: `Comment ${n}` })

export const byAuthor = (authorId: string) => ({
  IndexName: "GSI2",
  KeyConditionExpression: "GSI2PK = :pk",
  ExpressionAttributeValues: { ":pk": `AUTHOR#${authorId}` },
})

export const inPartition = (post: Post) => ({
  KeyConditionExpression: "PK = :pk",
  ExpressionAttributeValues: { ":pk": post.PK },
})
