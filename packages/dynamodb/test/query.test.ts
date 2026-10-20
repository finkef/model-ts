import { describe, expect } from "@effect/vitest"
import { Effect, Stream } from "effect"
import { Dynamo, DynamoDB } from "../src/index.ts"
import { Sandbox } from "../src/sandbox.ts"
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

      const result = yield* Dynamo.query(inPartition(post), {
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

  it.effect("decodes union members by tag", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      const post = makePost(1)
      yield* sandbox.seed(post, makeComment(post, 1))

      const items = yield* PostContent.query(inPartition(post))

      expect(items.map((item) => item.constructor)).toEqual([Comment, Post])
    }),
  )

  it.effect("follows every page with FetchAllPages", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(...[1, 2, 3, 4, 5].map((n) => makePost(n)))

      const firstPage = yield* Post.query({ ...byAuthor("ada"), Limit: 2 })
      const all = yield* Post.query({
        ...byAuthor("ada"),
        Limit: 2,
        FetchAllPages: true,
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
})

describe("paginate with cursorEncryptionKey", () => {
  const encrypted = testEffect(
    testTable({
      cursorEncryptionKey: new TextEncoder().encode(
        "0123456789abcdef0123456789abcdef",
      ),
    }),
  )

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
    }),
  )
})
