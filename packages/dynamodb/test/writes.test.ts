import { describe, expect } from "@effect/vitest"
import { GetCommand, PutCommand } from "@aws-sdk/lib-dynamodb"
import { Effect } from "effect"
import { docVersion, DynamoDB, RaceConditionError } from "../src/index.ts"
import { Sandbox } from "../src/sandbox.ts"
import { makePost, Post, PostId, Slug } from "./fixtures.ts"
import { it } from "./utils.ts"

describe("put", () => {
  it.effect("stores the encoded item with its keys and version", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      const before = yield* sandbox.snapshot

      const post = yield* makePost(1, { publishedAt: new Date(0) }).put()

      expect(docVersion(post)).toBe(0)
      expect(yield* sandbox.diff(before)).toMatchInlineSnapshot(`
        "+ [POST#post_001 / POST]
        +   PK: "POST#post_001"
        +   SK: "POST"
        +   GSI2PK: "AUTHOR#ada"
        +   GSI2SK: "POST#post_001"
        +   _docVersion: 0
        +   _tag: "Post"
        +   authorId: "ada"
        +   id: "post_001"
        +   publishedAt: "1970-01-01T00:00:00.000Z"
        +   title: "Post 1"
        +   views: 0"
      `)
    }),
  )

  it.effect("fails with KeyExistsError when the key is taken", () =>
    Effect.gen(function* () {
      yield* makePost(1).put()
      const error = yield* Effect.flip(makePost(1).put())
      expect(error._tag).toBe("KeyExistsError")
      expect(error.message).toBe(
        "An item already exists at POST#post_001 / POST",
      )
    }),
  )

  it.effect("overwrites with IgnoreExistence", () =>
    Effect.gen(function* () {
      yield* makePost(1).put()
      yield* makePost(1, { title: "Again" }).put({ IgnoreExistence: true })
      expect((yield* Post.get(makePost(1))).title).toBe("Again")
    }),
  )

  it.effect(
    "fails with ConditionalCheckFailedError for a caller's condition",
    () =>
      Effect.gen(function* () {
        const error = yield* Effect.flip(
          makePost(1).put({
            ConditionExpression: "attribute_exists(PK)",
          }),
        )
        expect(error._tag).toBe("ConditionalCheckFailedError")
      }),
  )

  it.effect("fails with ItemEncodeError for a value the schema rejects", () =>
    Effect.gen(function* () {
      const post = makePost(1, { publishedAt: "yesterday" as never })
      const error = yield* Effect.flip(post.put())
      expect(error._tag).toBe("ItemEncodeError")
    }),
  )
})

describe("update", () => {
  it.effect("writes the changed fields and bumps the version", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))
      const before = yield* sandbox.snapshot
      const post = yield* Post.load(makePost(1))

      const updated = yield* post.update({ title: "Renamed", views: 3 })

      expect(updated).toBeInstanceOf(Post)
      expect(post.title).toBe("Post 1")
      expect(docVersion(updated)).toBe(1)
      expect(yield* sandbox.diff(before)).toMatchInlineSnapshot(`
        "[POST#post_001 / POST]
            PK: "POST#post_001"
            SK: "POST"
            GSI2PK: "AUTHOR#ada"
            GSI2SK: "POST#post_001"
        -   _docVersion: 0
        +   _docVersion: 1
            _tag: "Post"
            authorId: "ada"
            id: "post_001"
            publishedAt: null
        -   title: "Post 1"
        +   title: "Renamed"
        -   views: 0
        +   views: 3"
      `)
    }),
  )

  it.effect("fails with RaceConditionError when the stored item changed", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))
      const first = yield* Post.load(makePost(1))
      const second = yield* Post.load(makePost(1))

      yield* first.update({ title: "First" })
      const error = yield* Effect.flip(second.update({ title: "Second" }))

      expect(error).toBeInstanceOf(RaceConditionError)
      expect((yield* Post.get(makePost(1))).title).toBe("First")
    }),
  )

  it.effect("moves the item when the update changes its key", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      const slug = new Slug({ slug: "hello", postId: PostId.make("post_001") })
      yield* sandbox.seed(slug)
      const before = yield* sandbox.snapshot

      const moved = yield* (yield* Slug.load(slug)).update({
        slug: "hello-world",
      })

      expect(moved.PK).toBe("SLUG#hello-world")
      expect(yield* sandbox.diff(before)).toMatchInlineSnapshot(`
        "- [SLUG#hello / SLUG]
        -   PK: "SLUG#hello"
        -   SK: "SLUG"
        -   _docVersion: 0
        -   _tag: "Slug"
        -   postId: "post_001"
        -   slug: "hello"

        + [SLUG#hello-world / SLUG]
        +   PK: "SLUG#hello-world"
        +   SK: "SLUG"
        +   _docVersion: 1
        +   _tag: "Slug"
        +   postId: "post_001"
        +   slug: "hello-world""
      `)
    }),
  )

  it.effect("applyUpdate returns the next instance and its operation", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))
      const post = yield* Post.load(makePost(1))

      const [next, operation] = post.applyUpdate({ title: "Renamed" })

      expect(next.title).toBe("Renamed")
      expect(operation).toMatchObject({
        _tag: "Put",
        ConditionExpression:
          "attribute_not_exists(#docVersion) OR #docVersion = :docVersion",
        ExpressionAttributeValues: { ":docVersion": 0 },
      })
    }),
  )
})

describe("updateRaw", () => {
  it.effect("sets encoded fields and removes null GSI attributes", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))
      const before = yield* sandbox.snapshot

      const post = yield* Post.updateRaw(makePost(1), {
        publishedAt: new Date(0),
        GSI2PK: null,
        GSI2SK: null,
      })

      expect(post.publishedAt).toEqual(new Date(0))
      expect(yield* sandbox.diff(before)).toMatchInlineSnapshot(`
        "[POST#post_001 / POST]
            PK: "POST#post_001"
            SK: "POST"
        -   GSI2PK: "AUTHOR#ada"
        -   GSI2SK: "POST#post_001"
            _docVersion: 0
            _tag: "Post"
            authorId: "ada"
            id: "post_001"
        -   publishedAt: null
        +   publishedAt: "1970-01-01T00:00:00.000Z"
            title: "Post 1"
            views: 0"
      `)
    }),
  )

  it.effect("fails with ItemNotFoundError when there is no item", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        Post.updateRaw(makePost(1), { title: "x" }),
      )
      expect(error._tag).toBe("ItemNotFoundError")
    }),
  )

  it.effect(
    "fails with ConditionalCheckFailedError for a caller's condition",
    () =>
      Effect.gen(function* () {
        const sandbox = yield* Sandbox
        yield* sandbox.seed(makePost(1))
        const error = yield* Effect.flip(
          Post.updateRaw(
            makePost(1),
            { title: "x" },
            {
              ConditionExpression: "#views > :views",
              ExpressionAttributeNames: { "#views": "views" },
              ExpressionAttributeValues: { ":views": 10 },
            },
          ),
        )
        expect(error._tag).toBe("ConditionalCheckFailedError")
      }),
  )
})

describe("delete", () => {
  it.effect("deletes a loaded instance", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))
      const post = yield* Post.load(makePost(1))

      yield* post.delete()

      expect(yield* sandbox.get(post)).toBeUndefined()
    }),
  )

  it.effect("fails with RaceConditionError when the stored item changed", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))
      const stale = yield* Post.load(makePost(1))
      yield* (yield* Post.load(makePost(1))).update({ title: "Changed" })

      const error = yield* Effect.flip(stale.delete())

      expect(error._tag).toBe("RaceConditionError")
      expect(yield* sandbox.get(stale)).toBeDefined()
    }),
  )

  it.effect("ignores the version with ignoreVersion", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))
      const stale = yield* Post.load(makePost(1))
      yield* (yield* Post.load(makePost(1))).update({ title: "Changed" })

      yield* stale.delete({ ignoreVersion: true })

      expect(yield* sandbox.get(stale)).toBeUndefined()
    }),
  )

  it.effect("deletes by key on the class", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))
      yield* Post.delete(makePost(1))
      yield* Post.delete(makePost(2))
      expect(yield* sandbox.snapshot).toEqual({})
    }),
  )
})

describe("softDelete", () => {
  it.effect("moves the item to its $$DELETED$$ keys", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))
      const before = yield* sandbox.snapshot

      yield* (yield* Post.load(makePost(1))).softDelete()

      expect(yield* sandbox.diff(before)).toMatchInlineSnapshot(`
        "+ [$$DELETED$$POST#post_001 / $$DELETED$$POST]
        +   PK: "$$DELETED$$POST#post_001"
        +   SK: "$$DELETED$$POST"
        +   GSI2PK: "$$DELETED$$AUTHOR#ada"
        +   GSI2SK: "$$DELETED$$POST#post_001"
        +   _deletedAt: "1970-01-01T00:00:00.000Z"
        +   _docVersion: 0
        +   _tag: "Post"
        +   authorId: "ada"
        +   id: "post_001"
        +   publishedAt: null
        +   title: "Post 1"
        +   views: 0

        - [POST#post_001 / POST]
        -   PK: "POST#post_001"
        -   SK: "POST"
        -   GSI2PK: "AUTHOR#ada"
        -   GSI2SK: "POST#post_001"
        -   _docVersion: 0
        -   _tag: "Post"
        -   authorId: "ada"
        -   id: "post_001"
        -   publishedAt: null
        -   title: "Post 1"
        -   views: 0"
      `)
    }),
  )

  it.effect("fails with RaceConditionError when the stored item changed", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))
      const stale = yield* Post.load(makePost(1))
      yield* (yield* Post.load(makePost(1))).update({ title: "Changed" })

      const error = yield* Effect.flip(stale.softDelete())

      expect(error._tag).toBe("RaceConditionError")
    }),
  )

  it.effect(
    "fails with TransactionCanceledError when the archive slot is taken",
    () =>
      Effect.gen(function* () {
        const sandbox = yield* Sandbox
        yield* sandbox.seed(makePost(1))
        yield* (yield* Post.load(makePost(1))).softDelete()
        yield* sandbox.seed(makePost(1))

        const error = yield* Effect.flip(
          (yield* Post.load(makePost(1))).softDelete(),
        )

        expect(error._tag).toBe("TransactionCanceledError")
        const codes =
          error._tag === "TransactionCanceledError"
            ? error.cancellationReasons.map((reason) => reason.Code)
            : []
        expect(codes).toEqual(["None", "ConditionalCheckFailed"])
      }),
  )
})

describe("attribute types", () => {
  it.effect("round-trips binary values and sets, also when nested", () =>
    Effect.gen(function* () {
      const db = yield* DynamoDB
      const Item = {
        PK: "TYPES",
        SK: "1",
        bytes: new Uint8Array([1, 2, 3]),
        byteSet: new Set([new Uint8Array([4]), new Uint8Array([5])]),
        strings: new Set(["a", "b"]),
        numbers: new Set([1, 2]),
        nested: { list: [new Uint8Array([6]), { bytes: new Uint8Array([7]) }] },
      }
      yield* db.send(
        "PutItem",
        new PutCommand({ TableName: db.tableName, Item }),
      )
      const output = yield* db.send(
        "GetItem",
        new GetCommand({
          TableName: db.tableName,
          Key: { PK: "TYPES", SK: "1" },
        }),
      )
      expect(output.Item).toEqual(Item)
    }),
  )
})
