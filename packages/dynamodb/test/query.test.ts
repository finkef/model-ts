import { describe, expect } from "@effect/vitest"
import { Model } from "@model-ts/core"
import { Cause, Effect, Exit, Redacted, Schema, Stream } from "effect"
import { DynamoDB } from "../src/index.ts"
import { Sandbox, type SandboxOptions } from "../src/Sandbox.ts"
import {
  byAuthor,
  Comment,
  inPartition,
  makeComment,
  makePost,
  Post,
  PostContent,
} from "./fixtures.ts"
import { it, recordSpans, testEffect, testTable } from "./utils.ts"

describe("query", () => {
  it.effect("returns the model's items with the query meta", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(
        makePost(1),
        makePost(2),
        makePost(3, { authorId: "grace" }),
      )

      const posts = yield* Post.query(byAuthor("ada"))

      expect(posts.map((post) => post.id)).toEqual(["post_001", "post_002"])
      expect(posts.meta.lastEvaluatedKey).toBeUndefined()
    }),
  )

  it.effect("groups items by model and keeps unknown ones apart", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      const post = makePost(1)
      yield* sandbox.seed(post, makeComment(post, 1), makeComment(post, 2), {
        PK: post.PK,
        SK: "OTHER",
        _tag: "Other",
      })

      const result = yield* DynamoDB.query(inPartition(post), {
        posts: Post,
        comments: Comment,
      })

      expect(result.posts).toHaveLength(1)
      expect(result.comments.map((comment) => comment.body)).toEqual([
        "Comment 1",
        "Comment 2",
      ])
      expect(result._unknown).toEqual([
        { PK: post.PK, SK: "OTHER", _tag: "Other" },
      ])
    }),
  )

  it.effect("fails on an item tagged with the model that does not decode", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      const post = makePost(1)
      yield* sandbox.seed(post, {
        PK: post.PK,
        SK: "COMMENT#broken",
        _tag: "Comment",
        postId: post.id,
      })

      const error = yield* Effect.flip(Comment.query(inPartition(post)))

      expect(error._tag).toBe("ItemDecodeError")
      expect(error).toMatchObject({
        model: "Comment",
        key: { PK: post.PK, SK: "COMMENT#broken" },
      })
    }),
  )

  it.effect("decodes union members by tag", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      const post = makePost(1)
      yield* sandbox.seed(post, makeComment(post, 1))

      const items = yield* PostContent.query(inPartition(post))

      expect(items.map((item) => item.constructor)).toEqual([Comment, Post])
    }),
  )

  it.effect("follows every page with fetchAllPages", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(...[1, 2, 3, 4, 5].map((n) => makePost(n)))

      const firstPage = yield* Post.query({ ...byAuthor("ada"), Limit: 2 })
      const all = yield* Post.query({
        ...byAuthor("ada"),
        Limit: 2,
        fetchAllPages: true,
      })

      expect(firstPage).toHaveLength(2)
      expect(firstPage.meta.lastEvaluatedKey).toBeDefined()
      expect(all).toHaveLength(5)
    }),
  )
})

describe("stream", () => {
  it.effect("reads page by page as the stream is consumed", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(...[1, 2, 3, 4, 5].map((n) => makePost(n)))

      const titles = yield* Post.stream({ ...byAuthor("ada"), Limit: 2 }).pipe(
        Stream.map((post) => post.title),
        Stream.runCollect,
      )

      expect(titles).toEqual(["Post 1", "Post 2", "Post 3", "Post 4", "Post 5"])
    }),
  )

  it.effect("stops early without reading further pages", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(...[1, 2, 3, 4, 5].map((n) => makePost(n)))

      const first = yield* Post.stream({ ...byAuthor("ada"), Limit: 2 }).pipe(
        Stream.take(1),
        Stream.runCollect,
      )

      expect(first).toHaveLength(1)
    }),
  )
})

describe("paginate", () => {
  const seedPosts = Effect.gen(function* () {
    const sandbox = yield* Sandbox
    yield* sandbox.seed(...[1, 2, 3, 4, 5].map((n) => makePost(n)))
  })

  it.effect("pages forward with first and after", () =>
    Effect.gen(function* () {
      yield* seedPosts
      const first = yield* Post.paginate({ first: 2 }, byAuthor("ada"))
      const second = yield* Post.paginate(
        { first: 2, after: first.pageInfo.endCursor },
        byAuthor("ada"),
      )

      expect(first.edges.map((edge) => edge.node.id)).toEqual([
        "post_001",
        "post_002",
      ])
      expect(first.pageInfo).toMatchObject({
        hasNextPage: true,
        hasPreviousPage: false,
      })
      expect(second.edges.map((edge) => edge.node.id)).toEqual([
        "post_003",
        "post_004",
      ])
    }),
  )

  it.effect("pages backward with last and before, in ascending order", () =>
    Effect.gen(function* () {
      yield* seedPosts
      const forward = yield* Post.paginate({ first: 4 }, byAuthor("ada"))
      const before = forward.edges[3]!.cursor

      const page = yield* Post.paginate({ last: 2, before }, byAuthor("ada"))

      expect(page.edges.map((edge) => edge.node.id)).toEqual([
        "post_002",
        "post_003",
      ])
      expect(page.pageInfo).toMatchObject({
        hasPreviousPage: true,
        hasNextPage: false,
      })
    }),
  )

  it.effect("uses the item's cursor", () =>
    Effect.gen(function* () {
      yield* seedPosts
      const page = yield* Post.paginate({ first: 1 }, byAuthor("ada"))
      expect(page.edges[0]!.cursor).toBe(yield* page.edges[0]!.node.cursor())
    }),
  )

  it.effect("keeps reading when a filter leaves a Query short", () =>
    Effect.gen(function* () {
      yield* seedPosts
      const { spans, value: page } = yield* recordSpans(
        Post.paginate(
          { first: 1 },
          {
            ...byAuthor("ada"),
            FilterExpression: "title >= :title",
            ExpressionAttributeValues: {
              ":pk": "AUTHOR#ada",
              ":title": "Post 4",
            },
          },
        ),
      )

      expect(page.edges.map((edge) => edge.node.id)).toEqual(["post_004"])
      expect(page.pageInfo.hasNextPage).toBe(true)
      // Limit 2 reads posts 1-2, 3-4, then 5: three Queries.
      expect(
        spans.filter((span) => span.name === "DynamoDB.Query"),
      ).toHaveLength(3)
    }),
  )

  it.effect("caps the page size at the table's limit", () =>
    Effect.gen(function* () {
      yield* seedPosts
      const db = yield* DynamoDB
      const page = yield* Post.paginate({ first: 1000 }, byAuthor("ada"))
      expect(db.pagination).toEqual({ limit: 50, default: 20 })
      expect(page.edges).toHaveLength(5)
    }),
  )

  it.effect("fails with PaginationError for conflicting arguments", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        Post.paginate({ first: 1, last: 1 }, byAuthor("ada")),
      )
      expect(error).toMatchObject({
        _tag: "PaginationError",
        message: 'Only one of "first" and "last" can be specified',
      })
    }),
  )

  it.effect("fails with PaginationError for an unreadable cursor", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        Post.paginate({ after: "not a cursor" }, byAuthor("ada")),
      )
      expect(error._tag).toBe("PaginationError")
    }),
  )

  it.effect("builds cursors from the keys an item is stored with", () =>
    Effect.gen(function* () {
      yield* seedPosts
      // post_001 now sorts last in GSI2, but its getter still says otherwise.
      yield* Post.updateRaw(makePost(1), { GSI2SK: "ZZZ" })
      const all = yield* Post.paginate({ first: 5 }, byAuthor("ada"))
      expect(all.edges.map((edge) => edge.node.id).at(-1)).toBe("post_001")

      const after = yield* Post.paginate(
        { first: 5, after: all.pageInfo.endCursor },
        byAuthor("ada"),
      )
      expect(after.edges).toEqual([])
    }),
  )

  it.effect("rejects counts that are not non-negative integers", () =>
    Effect.gen(function* () {
      for (const input of [
        { first: 1.5 },
        { last: -1 },
        { first: 0, last: 0 },
      ]) {
        const error = yield* Effect.flip(Post.paginate(input, byAuthor("ada")))
        expect(error._tag).toBe("PaginationError")
      }
    }),
  )
})

describe("cursors", () => {
  const key = new TextEncoder().encode("0123456789abcdef0123456789abcdef")
  const rotated = new TextEncoder().encode("fedcba9876543210fedcba9876543210")
  const encrypted = testEffect(testTable({ cursors: { encryptionKey: key } }))

  /** The cursor a table with these options gives post_001. */
  const cursorWith = (options: SandboxOptions) =>
    makePost(1)
      .cursor()
      .pipe(Effect.provide(Sandbox.layerMemory(options)))

  encrypted.effect("encrypts cursors and reads them back", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(...[1, 2, 3].map((n) => makePost(n)))

      const first = yield* Post.paginate({ first: 1 }, byAuthor("ada"))
      const cursor = first.pageInfo.endCursor!
      const second = yield* Post.paginate(
        { first: 1, after: cursor },
        byAuthor("ada"),
      )

      expect(() =>
        JSON.parse(Buffer.from(cursor, "base64").toString()),
      ).toThrow()
      expect(second.edges.map((edge) => edge.node.id)).toEqual(["post_002"])
      expect(yield* (yield* Post.get(makePost(1))).cursor()).toBe(cursor)
    }),
  )

  encrypted.effect("rejects a cursor that was altered", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(...[1, 2, 3].map((n) => makePost(n)))
      const first = yield* Post.paginate({ first: 1 }, byAuthor("ada"))
      const bytes = Buffer.from(first.pageInfo.endCursor!, "base64")
      bytes[12]! ^= 1 // the first byte after the 12-byte IV

      const error = yield* Effect.flip(
        Post.paginate(
          { first: 1, after: bytes.toString("base64") },
          byAuthor("ada"),
        ),
      )

      expect(error._tag).toBe("PaginationError")
    }),
  )

  it.effect("rejects the cursor of another query", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(...[1, 2].map((n) => makePost(n)))
      const page = yield* Post.paginate({ first: 1 }, byAuthor("ada"))

      const error = yield* Effect.flip(
        Post.paginate(
          { first: 1, after: page.pageInfo.endCursor },
          byAuthor("grace"),
        ),
      )

      expect(error).toMatchObject({
        _tag: "PaginationError",
        message: "The cursor does not belong to this query",
      })
    }),
  )

  it.effect("takes a key as bytes, base64 or Redacted", () =>
    Effect.gen(function* () {
      const base64 = Buffer.from(key).toString("base64")
      const cursors = [
        yield* cursorWith({ cursors: { encryptionKey: key } }),
        yield* cursorWith({ cursors: { encryptionKey: base64 } }),
        yield* cursorWith({
          cursors: { encryptionKey: Redacted.make(base64) },
        }),
      ]
      expect(new Set(cursors).size).toBe(1)
    }),
  )

  it.effect("reads cursors made with a previous key", () =>
    Effect.gen(function* () {
      const old = yield* cursorWith({ cursors: { encryptionKey: key } })
      const rotation = {
        cursors: { encryptionKey: rotated, previousKeys: [key] },
      }

      const page = yield* Effect.gen(function* () {
        const sandbox = yield* Sandbox
        yield* sandbox.seed(...[1, 2, 3].map((n) => makePost(n)))
        return yield* Post.paginate({ first: 1, after: old }, byAuthor("ada"))
      }).pipe(Effect.provide(Sandbox.layerMemory(rotation)))

      expect(page.edges.map((edge) => edge.node.id)).toEqual(["post_002"])
      expect(yield* cursorWith(rotation)).not.toBe(old)
    }),
  )

  it.effect("writes plain cursors as base64 JSON of the item's keys", () =>
    Effect.gen(function* () {
      const cursor = yield* cursorWith({ cursors: "plain" })
      const post = makePost(1)
      expect(JSON.parse(Buffer.from(cursor, "base64").toString())).toEqual({
        PK: post.PK,
        SK: post.SK,
        GSI2PK: post.GSI2PK,
        GSI2SK: post.GSI2SK,
      })
    }),
  )

  it.effect("gives the same cursors in every sandbox", () =>
    Effect.gen(function* () {
      const first = yield* cursorWith({})
      expect(yield* cursorWith({})).toBe(first)
      expect(() =>
        JSON.parse(Buffer.from(first, "base64").toString()),
      ).toThrow()
    }),
  )

  it.effect("refuses a key that is not 32 bytes", () =>
    Effect.gen(function* () {
      for (const cursors of [
        { encryptionKey: new Uint8Array(16) },
        { encryptionKey: "not a key" },
        {
          encryptionKey: key,
          previousKeys: [Buffer.from(key).toString("hex")],
        },
      ]) {
        const exit = yield* Effect.exit(
          Effect.provide(
            Effect.void,
            DynamoDB.layer({ tableName: "posts", cursors }),
          ),
        )
        expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true)
      }
    }),
  )
})

describe("GSI1", () => {
  /** Keyed on GSI1, which reads its own GSI1PK and GSI1SK. */
  class Follow extends Model.Class<Follow>()("Follow", DynamoDB.provider, {
    userId: Schema.String,
    targetId: Schema.String,
  }) {
    get PK() {
      return `USER#${this.userId}`
    }
    get SK() {
      return `FOLLOWS#${this.targetId}`
    }
    get GSI1PK() {
      return `FOLLOWERS#${this.targetId}`
    }
    get GSI1SK() {
      return `USER#${this.userId}`
    }
  }

  const followersOf = (targetId: string) => ({
    IndexName: "GSI1",
    KeyConditionExpression: "GSI1PK = :pk",
    ExpressionAttributeValues: { ":pk": `FOLLOWERS#${targetId}` },
  })

  const seedFollows = Effect.gen(function* () {
    const sandbox = yield* Sandbox
    yield* sandbox.seed(
      ...["1", "2", "3"].map((userId) => new Follow({ userId, targetId: "9" })),
      new Follow({ userId: "1", targetId: "8" }),
    )
  })

  /** Plain cursors, so the test can read what they carry. */
  const plain = testEffect(testTable({ cursors: "plain" }))

  it.effect("queries by GSI1PK and GSI1SK", () =>
    Effect.gen(function* () {
      yield* seedFollows
      const follows = yield* Follow.query(followersOf("9"))
      expect(follows.map((follow) => follow.userId)).toEqual(["1", "2", "3"])
    }),
  )

  plain.effect("paginates with cursors that carry the GSI1 keys", () =>
    Effect.gen(function* () {
      yield* seedFollows
      const first = yield* Follow.paginate({ first: 2 }, followersOf("9"))
      const cursor = first.pageInfo.endCursor!
      const second = yield* Follow.paginate(
        { first: 2, after: cursor },
        followersOf("9"),
      )

      expect(
        JSON.parse(Buffer.from(cursor, "base64").toString()),
      ).toMatchObject({ GSI1PK: "FOLLOWERS#9", GSI1SK: "USER#2" })
      expect(second.edges.map((edge) => edge.node.userId)).toEqual(["3"])
      expect(second.pageInfo.hasNextPage).toBe(false)
    }),
  )
})

describe("extended models", () => {
  class Note extends Model.Class<Note>()("Note", DynamoDB.provider, {
    id: Schema.String,
  }) {
    get PK() {
      return "NOTES"
    }
    get SK() {
      return `NOTE#${this.id}`
    }
  }
  class TitledNote extends Note.extend<TitledNote>("TitledNote")({
    title: Schema.String,
  }) {}

  it.effect("queries the rows of a class made with extend", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(new TitledNote({ id: "1", title: "Hello" }))
      const notes = yield* DynamoDB.query(
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "NOTES" },
        },
        { notes: TitledNote },
      )
      expect(notes.notes.map((note) => note.title)).toEqual(["Hello"])
    }),
  )
})
