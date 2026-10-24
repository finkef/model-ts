/**
 * Type-level tests, checked by `bun run typecheck`. Every `@ts-expect-error`
 * must stay an error, or the build fails.
 */
import { Model } from "@model-ts/core"
import { Effect, Option, Schema } from "effect"
import { DynamoDB, Operation, type Recovered } from "../src/index.ts"
import {
  type Comment,
  inPartition,
  makePost,
  Post,
  PostContent,
} from "./fixtures.ts"

// A model whose fields all have defaults works with the DynamoDB statics.
class Counter extends Model.Class<Counter>()("Counter", DynamoDB.provider, {
  count: Schema.Number.pipe(Schema.withConstructorDefault(Effect.succeed(0))),
}) {
  get PK() {
    return "COUNTER"
  }
  get SK() {
    return "COUNTER"
  }
}
export const counter: Effect.Effect<Counter, DynamoDB.GetError, DynamoDB> =
  DynamoDB.load(Counter, { PK: "COUNTER", SK: "COUNTER" })
export const counted = DynamoDB.update(new Counter(), { count: 1 })

// Query groups cannot take the names of the result's own members.
// @ts-expect-error "meta" is taken by the result
DynamoDB.query(inPartition(makePost(1)), { meta: Post })
// @ts-expect-error "_unknown" is taken by the result
DynamoDB.query(inPartition(makePost(1)), { _unknown: Post })

// Writes take model instances; a bare key has no schema to encode it with.
// A table must say how it writes cursors.
// @ts-expect-error cursors is required
DynamoDB.layer({ tableName: "posts" })
DynamoDB.layer({ tableName: "posts", cursors: "plain" })

// Writes take model instances; a bare key has no schema to encode it with.
// @ts-expect-error a plain key is not a model instance
DynamoDB.put({ PK: "POST#1", SK: "POST" })
// @ts-expect-error a plain key is not a model instance
Operation.put({ PK: "POST#1", SK: "POST" })

// Raw updates take the model's fields and GSI attributes, nothing else.
DynamoDB.updateRaw(Post, makePost(1), { views: 1, GSI2SK: null })
// @ts-expect-error not a field of Post
DynamoDB.updateRaw(Post, makePost(1), { typo: 42 })
// @ts-expect-error PK is a key, not a field
Operation.updateRaw(Post, makePost(1), { PK: "POST#2" })

// loadOption keeps the model type, for unions and recovered items too, and a
// missing item is None, so ItemNotFoundError is not in its error channel.
export const maybePost: Effect.Effect<
  Option.Option<Post>,
  DynamoDB.LoadOptionError,
  DynamoDB
> = Post.loadOption(makePost(1))
export const maybeContent: Effect.Effect<
  Option.Option<Post | Comment>,
  DynamoDB.LoadOptionError,
  DynamoDB
> = PostContent.loadOption(makePost(1))
export const maybeRecovered: Effect.Effect<
  Option.Option<Post & Recovered>,
  DynamoDB.LoadOptionError,
  DynamoDB
> = DynamoDB.loadOption(Post, makePost(1), { recover: true })
Post.loadOption(makePost(1)).pipe(
  // @ts-expect-error loadOption cannot fail with ItemNotFoundError
  Effect.catchTag("ItemNotFoundError", () => Effect.succeed(Option.none())),
)
