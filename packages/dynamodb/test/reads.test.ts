import { marshall } from "@aws-sdk/util-dynamodb"
import { describe, expect, it as vitest } from "@effect/vitest"
import { Effect, Exit, Option } from "effect"
import {
  docVersion,
  DynamoDB,
  ItemDecodeError,
  ItemNotFoundError,
} from "../src/index.ts"
import { Sandbox } from "../src/Sandbox.ts"
import { makeComment, makePost, Post, PostContent, PostId } from "./fixtures.ts"
import { it, recordSpans } from "./utils.ts"

describe("load with unprocessed keys", () => {
  vitest.live("reads keys that BatchGetItem left unprocessed again", () =>
    Effect.gen(function* () {
      const post = makePost(1)
      const stored = marshall({ ...post.encode(), PK: post.PK, SK: post.SK })
      let calls = 0
      // The first BatchGetItem leaves every key unprocessed, as under throttling.
      const respond = (body: any) => {
        calls++
        const keys = body.RequestItems.posts.Keys
        return calls === 1
          ? {
              Responses: { posts: [] },
              UnprocessedKeys: { posts: { Keys: keys } },
            }
          : { Responses: { posts: [stored] }, UnprocessedKeys: {} }
      }
      const table = DynamoDB.layer({
        tableName: "posts",
        cursors: "plain",
        client: {
          region: "local",
          credentials: { accessKeyId: "test", secretAccessKey: "test" },
          endpoint: "http://dynamodb.test",
          maxAttempts: 1,
          requestHandler: {
            handle: async (request: { body?: string }) => ({
              response: {
                statusCode: 200,
                headers: { "content-type": "application/x-amz-json-1.0" },
                body: new TextEncoder().encode(
                  JSON.stringify(respond(JSON.parse(request.body ?? "{}"))),
                ),
              },
            }),
          } as never,
        },
      })

      const loaded = yield* Post.load(post).pipe(Effect.provide(table))

      expect(loaded.title).toBe(post.title)
      expect(calls).toBe(2)
    }),
  )
})

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
        expect(error.message).toContain(
          "Stored item at POST#post_001 / POST does not match Post: ",
        )
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
      const calls = spans.filter((span) => span.kind === "client")
      expect(calls.map((span) => span.name)).toEqual(["DynamoDB.BatchGetItem"])
      expect(calls[0]!.attributes.get("aws.dynamodb.keys_count")).toBe(3)

      // The batch span links to every load it served.
      const batch = spans.find((span) => span.name === "DynamoDB.batchGet")!
      const loads = spans.filter((span) => span.name === "DynamoDB.load")
      expect(batch.links.map((link) => link.span.spanId).toSorted()).toEqual(
        loads.map((span) => span.spanId).toSorted(),
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

  it.effect("loadMany loads every key in one batch", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1), makePost(2))

      const posts = yield* Post.loadMany([makePost(2), makePost(1)])

      expect(posts.map((post) => post.title)).toEqual(["Post 2", "Post 1"])
    }),
  )
})

describe("loadOption", () => {
  it.effect("returns Some for a stored item and None for a missing key", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))

      const found = yield* Post.loadOption(makePost(1))
      const missing = yield* Post.loadOption({ PK: "POST#nope", SK: "POST" })

      expect(Option.getOrThrow(found)).toBeInstanceOf(Post)
      expect(Option.getOrThrow(found).title).toBe("Post 1")
      expect(Option.isNone(missing)).toBe(true)
    }),
  )

  it.effect("shares one BatchGetItem with concurrent loads", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))

      const { value, spans } = yield* recordSpans(
        Effect.all(
          [
            Post.load(makePost(1)),
            Post.loadOption(makePost(1)),
            Post.loadOption({ PK: "POST#nope", SK: "POST" }),
          ],
          { concurrency: "unbounded" },
        ),
      )

      expect(value[0].title).toBe("Post 1")
      expect(Option.map(value[1], (post) => post.title)).toEqual(
        Option.some("Post 1"),
      )
      expect(Option.isNone(value[2])).toBe(true)
      const calls = spans.filter((span) => span.kind === "client")
      expect(calls.map((span) => span.name)).toEqual(["DynamoDB.BatchGetItem"])
      expect(
        spans.filter((span) => span.name === "DynamoDB.loadOption"),
      ).toHaveLength(2)
    }),
  )

  it.effect("recovers a soft-deleted item when asked to", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      const post = makePost(1)
      yield* sandbox.seed(post)
      yield* post.softDelete()

      const live = yield* Post.loadOption(post)
      const recovered = yield* Post.loadOption(post, { recover: true })

      expect(Option.isNone(live)).toBe(true)
      expect(Option.getOrThrow(recovered).isDeleted).toBe(true)
    }),
  )

  it.effect("still fails when the stored item does not decode", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed({
        PK: "POST#post_001",
        SK: "POST",
        _tag: "Post",
        id: 1,
      })

      const error = yield* Effect.flip(
        Post.loadOption(Post.key(PostId.make("post_001"))),
      )

      expect(error).toBeInstanceOf(ItemDecodeError)
    }),
  )
})

describe("where", () => {
  it.effect("passes a matching item through and keeps the batch", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1), makePost(2))
      const byAda = Post.where((post) => post.authorId === "ada")

      const { value, spans } = yield* recordSpans(
        Effect.all(
          [
            Post.load(makePost(1)).pipe(byAda),
            Post.load(makePost(2)).pipe(byAda),
          ],
          { concurrency: "unbounded" },
        ),
      )

      expect(value.map((post) => post.title)).toEqual(["Post 1", "Post 2"])
      expect(value[0]).toBeInstanceOf(Post)
      const calls = spans.filter((span) => span.kind === "client")
      expect(calls.map((span) => span.name)).toEqual(["DynamoDB.BatchGetItem"])
    }),
  )

  it.effect("fails a rejected item with ItemNotFoundError for its key", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))

      const error = yield* Effect.flip(
        Post.load(makePost(1)).pipe(
          Post.where((post) => post.authorId === "grace"),
        ),
      )

      expect(error).toBeInstanceOf(ItemNotFoundError)
      expect(error).toMatchObject({
        key: { PK: "POST#post_001", SK: "POST" },
      })
    }),
  )

  it.effect("turns a rejected Option into None", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      const post = makePost(1)
      const comment = makeComment(post, 1)
      yield* sandbox.seed(post, comment)

      const rejected = yield* Post.loadOption(post).pipe(
        Post.where((post) => post.authorId === "grace"),
      )
      const kept = yield* PostContent.loadOption(comment).pipe(
        PostContent.where((item) => item._tag === "Comment"),
      )

      expect(Option.isNone(rejected)).toBe(true)
      expect(Option.map(kept, (item) => item._tag)).toEqual(
        Option.some("Comment"),
      )
    }),
  )

  it.effect("chains: every step must hold", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))
      const byAda = Post.where((post) => post.authorId === "ada")
      const published = Post.where((post) => post.publishedAt !== null)

      const once = yield* Post.load(makePost(1)).pipe(
        byAda,
        Post.where((post) => post.title === "Post 1"),
      )
      const error = yield* Effect.flip(
        Post.load(makePost(1)).pipe(byAda, published),
      )
      const none = yield* Post.loadOption(makePost(1)).pipe(byAda, published)

      expect(once.title).toBe("Post 1")
      expect(error._tag).toBe("ItemNotFoundError")
      expect(Option.isNone(none)).toBe(true)
    }),
  )
})
