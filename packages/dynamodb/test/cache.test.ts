import { describe, expect, it as vitest } from "@effect/vitest"
import { Model } from "@model-ts/core"
import { Deferred, Effect, Fiber, Schema } from "effect"
import {
  docVersion,
  DynamoDB,
  type DynamoDBError,
  RequestCache,
} from "../src/index.ts"
import { ItemCache, type SharedRead } from "../src/internal/cache.ts"
import { makeMemoryEngine } from "../src/internal/memory/engine.ts"
import { memoryClientConfig } from "../src/internal/memory/handler.ts"
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

/** A model with a field the schema doesn't copy, so aliasing would show. */
class Doc extends Model.Class<Doc>()("Doc", DynamoDB.provider, {
  id: Schema.String,
  data: Schema.Unknown,
}) {
  get PK() {
    return `DOC#${this.id}`
  }
  get SK() {
    return "DOC"
  }
}
const docKey = { PK: "DOC#1", SK: "DOC" }

interface HttpRequest {
  readonly headers: Record<string, string>
}

/**
 * An in-memory table whose responses can be held after DynamoDB applied the
 * request, to put writes and reads in flight at chosen moments.
 */
const gatedTable = (tableName = "posts") => {
  const config = memoryClientConfig(makeMemoryEngine())
  const handler = config.requestHandler as unknown as {
    readonly handle: (request: HttpRequest) => Promise<unknown>
  }
  const holding = new Set<string>()
  const held: Array<() => void> = []
  const calls: Array<string> = []
  const handle = async (request: HttpRequest) => {
    const target = request.headers["x-amz-target"] ?? ""
    const operation = target.slice(target.indexOf(".") + 1)
    calls.push(operation)
    const response = await handler.handle(request)
    if (holding.has(operation)) {
      await new Promise<void>((resolve) => held.push(resolve))
    }
    return response
  }
  const until = (done: () => boolean): Effect.Effect<void> =>
    Effect.suspend(() =>
      done() ? Effect.void : Effect.andThen(Effect.sleep(1), until(done)),
    )
  return {
    layer: DynamoDB.layer({
      tableName,
      cursors: "plain",
      client: { ...config, requestHandler: { ...handler, handle } as never },
    }),
    calls,
    hold: (operation: string) => Effect.sync(() => holding.add(operation)),
    /** Waits until `count` responses are held. */
    held: (count: number) => until(() => held.length >= count),
    release: Effect.sync(() => {
      holding.clear()
      for (const resolve of held.splice(0)) resolve()
    }),
  }
}

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

  it.effect("loads what a fresh read returns after a write", () =>
    Effect.gen(function* () {
      const data = {
        set: new Set(["a", undefined]),
        map: new Map([["a", 1]]),
        big: 1n,
        list: ["a", undefined],
        nested: { gone: undefined, n: 1 },
      }
      const { value } = yield* cached(
        Effect.gen(function* () {
          yield* new Doc({ id: "1", data }).put()
          return yield* Doc.load(docKey)
        }),
      )
      const fresh = yield* Doc.get(docKey)

      expect(value.data).toEqual(fresh.data)
      expect(fresh.data).toEqual({
        set: new Set(["a"]),
        map: { a: 1 },
        big: 1,
        list: ["a"],
        nested: { n: 1 },
      })
    }),
  )

  it.effect("keeps its items apart from the instances it returns", () =>
    Effect.gen(function* () {
      yield* (yield* Sandbox).seed(new Doc({ id: "1", data: { n: 1 } }))

      const { value } = yield* cached(
        Effect.gen(function* () {
          const loaded = yield* Doc.load(docKey)
          Object.assign(loaded.data as object, { n: 2 })
          const reloaded = yield* Doc.load(docKey)

          const written = new Doc({ id: "2", data: { n: 1 } })
          yield* written.put()
          Object.assign(written.data as object, { n: 2 })
          return { reloaded, written: yield* Doc.load(written) }
        }),
      )

      expect(value.reloaded.data).toEqual({ n: 1 })
      expect(value.written.data).toEqual({ n: 1 })
    }),
  )

  vitest.live("forgets a key that two writes changed at once", () => {
    const table = gatedTable()
    return Effect.gen(function* () {
      // DynamoDB applies the first put, but its response arrives last.
      yield* table.hold("PutItem")
      const first = yield* Effect.forkChild(
        makePost(1, { title: "First" }).put({ ignoreExistence: true }),
      )
      yield* table.held(1)
      const second = yield* Effect.forkChild(
        makePost(1, { title: "Second" }).put({ ignoreExistence: true }),
      )
      yield* table.held(2)
      yield* table.release
      yield* Fiber.join(first)
      yield* Fiber.join(second)

      const sent = table.calls.length
      expect((yield* Post.load(key)).title).toBe("Second")
      expect(table.calls.slice(sent)).toEqual(["BatchGetItem"])
    }).pipe(DynamoDB.withRequestCache, Effect.provide(table.layer))
  })

  vitest.live("shares a read in flight with later loads of its key", () => {
    const table = gatedTable()
    return Effect.gen(function* () {
      yield* makePost(1).put()
      yield* Effect.gen(function* () {
        yield* table.hold("BatchGetItem")
        const first = yield* Effect.forkChild(Post.load(key))
        yield* table.held(1)
        const second = yield* Effect.forkChild(Post.load(key))
        yield* Effect.sleep(10)
        yield* table.release
        expect((yield* Fiber.join(first)).title).toBe("Post 1")
        expect((yield* Fiber.join(second)).title).toBe("Post 1")
      }).pipe(DynamoDB.withRequestCache)
      expect(table.calls).toEqual(["PutItem", "BatchGetItem"])
    }).pipe(Effect.provide(table.layer))
  })

  vitest.live("reads again when the load it waited for is interrupted", () => {
    const table = gatedTable()
    return Effect.gen(function* () {
      yield* makePost(1).put()
      yield* Effect.gen(function* () {
        yield* table.hold("BatchGetItem")
        const first = yield* Effect.forkChild(Post.load(key))
        yield* table.held(1)
        const second = yield* Effect.forkChild(Post.load(key))
        yield* Effect.sleep(10)
        yield* Effect.forkChild(Fiber.interrupt(first))
        yield* Effect.sleep(10)
        yield* table.release
        expect((yield* Fiber.join(second)).title).toBe("Post 1")
      }).pipe(DynamoDB.withRequestCache)
    }).pipe(Effect.provide(table.layer))
  })

  vitest.live("keeps tables of the same name apart", () => {
    const a = gatedTable("posts")
    const b = gatedTable("posts")
    return Effect.gen(function* () {
      const cache = yield* RequestCache.make
      const run = <A, E>(
        table: typeof a,
        effect: Effect.Effect<A, E, DynamoDB>,
      ) =>
        effect.pipe(
          Effect.provideService(RequestCache, cache),
          Effect.provide(table.layer),
        )
      yield* run(a, makePost(1, { title: "A" }).put())
      yield* run(b, makePost(1, { title: "B" }).put())

      expect((yield* run(a, Post.load(key))).title).toBe("A")
      expect((yield* run(b, Post.load(key))).title).toBe("B")
    })
  })
})

describe("ItemCache", () => {
  const table = {}
  const item = (title: string) => ({ ...key, title })
  const titleOf = (cache: ItemCache) => cache.lookup(table, key)?.item?.title

  vitest.effect("keeps a write over a read that started before it", () =>
    Effect.gen(function* () {
      const cache = new ItemCache()
      const pending = yield* Deferred.make<SharedRead, DynamoDBError>()
      const shared = { _tag: "Shared", pending } as const
      const token = cache.beginRead(table, key, shared)
      cache.endWrite(cache.beginWrite(table, [key]), [
        { _tag: "Stored", key, item: item("Written") },
      ])
      cache.endRead(table, key, token, shared, { item: item("Read") })
      expect(titleOf(cache)).toBe("Written")

      const consistent = { _tag: "Consistent" } as const
      const late = cache.beginRead(table, key, consistent)
      cache.clear()
      cache.endRead(table, key, late, consistent, { item: item("Read") })
      expect(cache.lookup(table, key)).toBeUndefined()
    }),
  )

  vitest("keeps a consistent read over an older one", () => {
    const cache = new ItemCache()
    const consistent = { _tag: "Consistent" } as const
    const older = cache.beginRead(table, key, consistent)
    const newer = cache.beginRead(table, key, consistent)
    cache.endRead(table, key, newer, consistent, { item: item("New") })
    cache.endRead(table, key, older, consistent, { item: item("Old") })
    expect(titleOf(cache)).toBe("New")
  })

  vitest("forgets a key whose writes overlapped", () => {
    const cache = new ItemCache()
    const first = cache.beginWrite(table, [key])
    const second = cache.beginWrite(table, [key])
    cache.endWrite(second, [{ _tag: "Stored", key, item: item("Second") }])
    cache.endWrite(first, [{ _tag: "Stored", key, item: item("First") }])
    expect(cache.lookup(table, key)).toBeUndefined()

    // Once no write is in flight, the next one is recorded again.
    cache.endWrite(cache.beginWrite(table, [key]), [
      { _tag: "Stored", key, item: item("Third") },
    ])
    expect(titleOf(cache)).toBe("Third")
    expect(cache.lookup({}, key)).toBeUndefined()
  })
})
