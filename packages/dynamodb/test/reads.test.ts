import { describe, expect } from "@effect/vitest"
import { Effect, Exit, Option } from "effect"
import { docVersion, DynamoDB, ItemDecodeError } from "../src/index.ts"
import { Sandbox } from "../src/sandbox.ts"
import { makePost, Post, PostId } from "./fixtures.ts"
import { it, recordSpans } from "./utils.ts"

describe("get", () => {
  it.effect("decodes the stored item into the model", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1, { publishedAt: new Date(0) }))

      const post = yield* Post.get(Post.key(PostId.make("post_001")))

      expect(post).toBeInstanceOf(Post)
      expect(post.publishedAt).toEqual(new Date(0))
      expect(post.GSI2PK).toBe("AUTHOR#ada")
      expect(docVersion(post)).toBe(0)
    }),
  )

  it.effect("fails with ItemNotFoundError for a missing key", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        Post.get({ PK: "POST#nope", SK: "POST" }),
      )
      expect(error._tag).toBe("ItemNotFoundError")
      expect(error.message).toBe("No item found at POST#nope / POST")
    }),
  )

  it.effect(
    "fails with ItemDecodeError when the stored item no longer matches",
    () =>
      Effect.gen(function* () {
        const sandbox = yield* Sandbox
        yield* sandbox.seed({
          PK: "POST#post_001",
          SK: "POST",
          _tag: "Post",
          id: 1,
        })

        const error = yield* Effect.flip(
          Post.get(Post.key(PostId.make("post_001"))),
        )

        expect(error).toBeInstanceOf(ItemDecodeError)
        expect(error.message).toMatch(/^Stored item does not match Post: /)
      }),
  )
})

describe("load", () => {
  it.effect("batches concurrent loads into one BatchGetItem", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1), makePost(2), makePost(3))

      const { value, spans } = yield* recordSpans(
        Effect.forEach(
          ["post_001", "post_002", "post_003", "post_001"] as const,
          (id) => Post.load(Post.key(PostId.make(id))),
          { concurrency: "unbounded" },
        ),
      )

      expect(value.map((post) => post.id)).toEqual([
        "post_001",
        "post_002",
        "post_003",
        "post_001",
      ])
      const calls = spans.filter((span) => span.name.startsWith("DynamoDB."))
      expect(calls.map((span) => span.name)).toEqual(["DynamoDB.BatchGetItem"])
      expect(calls[0]!.attributes.get("aws.dynamodb.keys_count")).toBe(3)

      // The batch span links to every load it served.
      const batch = spans.find((span) => span.name === "Dynamo.batchGet")!
      const loads = spans.filter((span) => span.name === "Dynamo.load")
      expect(batch.links.map((link) => link.span.spanId).sort()).toEqual(
        loads.map((span) => span.spanId).sort(),
      )
      expect(Option.getOrUndefined(calls[0]!.parent)?.spanId).toBe(batch.spanId)
    }),
  )

  it.effect("keeps keys apart whatever characters they contain", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(
        { PK: "a::b", SK: "c", value: 1 },
        { PK: "a", SK: "b::c", value: 2 },
      )

      const items = yield* Effect.all(
        [
          DynamoDB.use((db) => db.getBatched({ PK: "a::b", SK: "c" })),
          DynamoDB.use((db) => db.getBatched({ PK: "a", SK: "b::c" })),
        ],
        { concurrency: "unbounded" },
      )
      expect(items.map((item) => item?.value)).toEqual([1, 2])
    }),
  )

  it.effect("fails with ItemNotFoundError for a missing key", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        Post.load({ PK: "POST#nope", SK: "POST" }),
      )
      expect(Exit.isFailure(exit)).toBe(true)
      const error = yield* Effect.flip(
        Post.load({ PK: "POST#nope", SK: "POST" }),
      )
      expect(error._tag).toBe("ItemNotFoundError")
    }),
  )

  it.effect("recovers a soft-deleted item when asked to", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      const post = makePost(1)
      yield* sandbox.seed(post)
      yield* post.softDelete()

      const live = yield* Effect.flip(Post.load(post))
      const recovered = yield* Post.load(post, { recover: true })

      expect(live._tag).toBe("ItemNotFoundError")
      expect(recovered.isDeleted).toBe(true)
      expect(recovered.title).toBe("Post 1")
    }),
  )

  it.effect("loadOption returns none for a missing key", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))

      expect(Option.isSome(yield* Post.loadOption(makePost(1)))).toBe(true)
      expect(yield* Post.loadOption(makePost(2))).toEqual(Option.none())
    }),
  )

  it.effect("loadMany loads every key in one batch", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1), makePost(2))

      const posts = yield* Post.loadMany([makePost(2), makePost(1)])

      expect(posts.map((post) => post.title)).toEqual(["Post 2", "Post 1"])
    }),
  )
})
