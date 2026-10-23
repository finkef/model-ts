import { describe, expect, it as vitest } from "@effect/vitest"
import { Effect } from "effect"
import { docVersion, DynamoDB, RequestCache } from "../src/index.ts"
import { ItemCache } from "../src/internal/cache.ts"
import { Sandbox } from "../src/Sandbox.ts"
import { makePost, Post, PostId, Slug } from "./fixtures.ts"
import { it, recordSpans } from "./utils.ts"

/** Runs `effect` with a request cache and returns the keys each BatchGetItem read. */
const batches = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.map(recordSpans(effect), ({ value, spans }) => ({
    value,
    reads: spans
      .filter((span) => span.name === "DynamoDB.BatchGetItem")
      .map((span) => span.attributes.get("aws.dynamodb.keys_count")),
  }))

const cached = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  batches(effect.pipe(DynamoDB.withRequestCache))

const key = Post.key(PostId.make("post_001"))

describe("request cache", () => {
  it.effect("reads each key once per request", () =>
    Effect.gen(function* () {
      yield* (yield* Sandbox).seed(makePost(1))

      const { value, reads } = yield* cached(
        Effect.gen(function* () {
          const first = yield* Post.load(key)
          const second = yield* Post.load(key)
          const missing = yield* Effect.flip(Post.load({ PK: "nope", SK: "-" }))
          const again = yield* Effect.flip(Post.load({ PK: "nope", SK: "-" }))
          return { first, second, missing, again }
        }),
      )

      expect(reads).toEqual([1, 1])
      expect(value.second).toEqual(value.first)
      expect(docVersion(value.second)).toBe(0)
      expect(value.again._tag).toBe("ItemNotFoundError")
    }),
  )

  it.effect("reads every time without a cache", () =>
    Effect.gen(function* () {
      yield* (yield* Sandbox).seed(makePost(1))
      const { reads } = yield* batches(
        Effect.andThen(Post.load(key), Post.load(key)),
      )
      expect(reads).toEqual([1, 1])
    }),
  )

  it.effect("batches the keys it does not know", () =>
    Effect.gen(function* () {
      yield* (yield* Sandbox).seed(makePost(1), makePost(2), makePost(3))
      const keys = [1, 2, 3].map((n) => Post.key(PostId.make(`post_00${n}`)))

      const { value, reads } = yield* cached(
        Effect.andThen(Post.load(key), Post.loadMany(keys)),
      )

      expect(reads).toEqual([1, 2])
      expect(value.map((post) => post.title)).toEqual([
        "Post 1",
        "Post 2",
        "Post 3",
      ])
    }),
  )

  it.effect("returns written items without reading", () =>
    Effect.gen(function* () {
      const { value, reads } = yield* cached(
        Effect.gen(function* () {
          const post = yield* makePost(1).put()
          const updated = yield* post.update({ title: "Updated" })
          const loaded = yield* Post.load(key)
          // The cached item carries the version, so the next update is guarded.
          yield* loaded.update({ title: "Again" })
          yield* Post.updateRaw(key, { views: 7 })
          return { updated, raw: yield* Post.load(key) }
        }),
      )

      expect(reads).toEqual([])
      expect(value.raw).toMatchObject({ title: "Again", views: 7 })
      expect(docVersion(value.raw)).toBe(3)
    }),
  )

  it.effect("knows deleted and moved keys", () =>
    Effect.gen(function* () {
      yield* (yield* Sandbox).seed(
        makePost(1),
        makePost(2),
        new Slug({ slug: "old", postId: PostId.make("post_001") }),
      )
      const { value, reads } = yield* cached(
        Effect.gen(function* () {
          const post = yield* Post.load(key)
          yield* post.softDelete()
          const live = yield* Effect.flip(Post.load(key))
          const recovered = yield* Post.load(key, { recover: true })

          const other = Post.key(PostId.make("post_002"))
          yield* Post.delete(other)
          const deleted = yield* Effect.flip(Post.load(other))

          const slug = yield* Slug.load({ PK: "SLUG#old", SK: "SLUG" })
          const moved = yield* slug.update({ slug: "new" })
          const old = yield* Effect.flip(Slug.load(slug))
          return {
            live,
            recovered,
            deleted,
            moved,
            old,
            slug: yield* Slug.load(moved),
          }
        }),
      )

      // Only the first loads of the post and the slug read the table.
      expect(reads).toEqual([1, 1])
      expect(value.live._tag).toBe("ItemNotFoundError")
      expect(value.recovered).toMatchObject({
        title: "Post 1",
        isDeleted: true,
      })
      expect(value.deleted._tag).toBe("ItemNotFoundError")
      expect(value.old._tag).toBe("ItemNotFoundError")
      expect(value.slug).toEqual(value.moved)
    }),
  )

  it.effect("forgets the keys of a failed write", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))

      const { value, reads } = yield* cached(
        Effect.gen(function* () {
          const post = yield* Post.load(key)
          // Someone else updates the post after this request read it.
          yield* sandbox.seed({
            ...(yield* sandbox.get(key)),
            title: "Theirs",
            _docVersion: 1,
          })
          const error = yield* Effect.flip(post.update({ title: "Mine" }))
          return { error, fresh: yield* Post.load(key) }
        }),
      )

      expect(value.error._tag).toBe("RaceConditionError")
      expect(reads).toEqual([1, 1])
      expect(value.fresh.title).toBe("Theirs")
    }),
  )

  it.effect("records a bulk rollback and forgets the failed keys", () =>
    Effect.gen(function* () {
      yield* (yield* Sandbox).seed(makePost(0))
      const posts = Array.from({ length: 100 }, (_, i) => makePost(i + 1))

      const { value, reads } = yield* cached(
        Effect.gen(function* () {
          const existing = Post.key(PostId.make("post_000"))
          yield* Post.load(existing)
          // 100 posts commit in the first transaction; the second conflicts
          // and the first is rolled back.
          const error = yield* Effect.flip(
            DynamoDB.bulk([
              ...posts.map((post) => post.operation("put")),
              makePost(0).operation("put"),
            ]),
          )
          return {
            error,
            rolledBack: yield* Effect.flip(Post.load(key)),
            failed: yield* Post.load(existing),
          }
        }),
      )

      expect(value.error._tag).toBe("TransactionCanceledError")
      // The rollback's committed delete is known; the key of the cancelled
      // transaction is read again.
      expect(value.rolledBack._tag).toBe("ItemNotFoundError")
      expect(value.failed.title).toBe("Post 0")
      expect(reads).toEqual([1, 1])
    }),
  )

  it.effect("stores the items of a committed bulk write", () =>
    Effect.gen(function* () {
      yield* (yield* Sandbox).seed(makePost(2), makePost(3))
      const other = Post.key(PostId.make("post_002"))
      const third = Post.key(PostId.make("post_003"))

      const { value, reads } = yield* cached(
        Effect.gen(function* () {
          const post = yield* Post.load(other)
          const [updated, operation] = post.applyUpdate({ title: "Bulk" })
          yield* DynamoDB.bulk([
            makePost(1).operation("put"),
            operation,
            Post.operation("updateRaw", third, { views: 1 }),
          ])
          return {
            updated,
            loaded: yield* Post.load(other),
            put: yield* Post.load(key),
            raw: yield* Post.load(third),
          }
        }),
      )

      // The transaction's UpdateItem returns nothing, so post_003 is read.
      expect(reads).toEqual([1, 1])
      expect(value.loaded).toEqual(value.updated)
      expect(value.put.title).toBe("Post 1")
      expect(value.raw.views).toBe(1)
    }),
  )

  it.effect("reads consistently on request and refreshes the entry", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))

      const { value, reads } = yield* cached(
        Effect.gen(function* () {
          yield* Post.load(key)
          yield* sandbox.seed(makePost(1, { title: "Changed" }))
          const stale = yield* Post.load(key)
          const consistent = yield* Post.load(key, { consistentRead: true })
          return { stale, consistent, after: yield* Post.load(key) }
        }),
      )

      expect(reads).toEqual([1, 1])
      expect(value.stale.title).toBe("Post 1")
      expect(value.consistent.title).toBe("Changed")
      expect(value.after.title).toBe("Changed")
    }),
  )

  it.effect("forgets a key on request", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))

      const { value, reads } = yield* cached(
        Effect.gen(function* () {
          yield* Post.load(key)
          yield* sandbox.seed(makePost(1, { title: "Changed" }))
          yield* RequestCache.invalidate(key)
          const invalidated = yield* Post.load(key)
          yield* RequestCache.clear
          return { invalidated, cleared: yield* Post.load(key) }
        }),
      )

      expect(reads).toEqual([1, 1, 1])
      expect(value.invalidated.title).toBe("Changed")
    }),
  )
})

describe("ItemCache", () => {
  vitest("keeps a write over a read that started before it", () => {
    const cache = new ItemCache()
    const token = cache.token()
    cache.set("posts", key, { ...key, title: "Written" })
    cache.fill("posts", key, token, { ...key, title: "Read" })
    expect(cache.lookup("posts", key)?.item?.title).toBe("Written")

    const late = cache.token()
    cache.clear()
    cache.fill("posts", key, late, { ...key, title: "Read" })
    expect(cache.lookup("posts", key)).toBeUndefined()
  })

  vitest("stores what DynamoDB would store", () => {
    const cache = new ItemCache()
    const item = {
      ...key,
      GSI1PK: undefined,
      tags: ["a", undefined],
      meta: { a: undefined, b: 1 },
    }
    cache.set("posts", key, item)
    item.meta.b = 2
    expect(cache.lookup("posts", key)?.item).toEqual({
      ...key,
      tags: ["a"],
      meta: { b: 1 },
    })
    expect(cache.lookup("other", key)).toBeUndefined()
  })
})
