import { describe, expect } from "@effect/vitest"
import { Cause, Effect, Exit } from "effect"
import { docVersion, DynamoDB, Operation } from "../src/index.ts"
import { Sandbox } from "../src/Sandbox.ts"
import { makePost, Post, Slug } from "./fixtures.ts"
import { it, recordSpans } from "./utils.ts"

describe("bulk", () => {
  it.effect("runs operations of different kinds in one transaction", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1), makePost(2), makePost(4))
      const before = yield* sandbox.snapshot
      const second = yield* Post.load(makePost(2))

      yield* DynamoDB.bulk([
        makePost(3).operation("put"),
        Post.operation("delete", makePost(1)),
        second.operation("update", { title: "Renamed" }),
        Operation.condition(makePost(4), {
          ConditionExpression: "attribute_exists(PK)",
        }),
      ])

      expect(yield* sandbox.diff(before)).toMatchInlineSnapshot(`
        "- [POST#post_001 / POST]
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
        -   views: 0

        [POST#post_002 / POST]
            PK: "POST#post_002"
            SK: "POST"
            GSI2PK: "AUTHOR#ada"
            GSI2SK: "POST#post_002"
        -   _docVersion: 0
        +   _docVersion: 1
            _tag: "Post"
            authorId: "ada"
            id: "post_002"
            publishedAt: null
        -   title: "Post 2"
        +   title: "Renamed"
            views: 0

        + [POST#post_003 / POST]
        +   PK: "POST#post_003"
        +   SK: "POST"
        +   GSI2PK: "AUTHOR#ada"
        +   GSI2SK: "POST#post_003"
        +   _docVersion: 0
        +   _tag: "Post"
        +   authorId: "ada"
        +   id: "post_003"
        +   publishedAt: null
        +   title: "Post 3"
        +   views: 0"
      `)
    }),
  )

  it.effect("fails with TransactionCanceledError and writes nothing", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))
      const before = yield* sandbox.snapshot

      const error = yield* Effect.flip(
        DynamoDB.bulk([
          makePost(2).operation("put"),
          makePost(1).operation("put"),
        ]),
      )

      expect(error._tag).toBe("TransactionCanceledError")
      expect(yield* sandbox.diff(before)).toBe("")
    }),
  )

  it.effect("rolls back later transactions first", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(0))
      const before = yield* sandbox.snapshot
      const posts = (from: number, count: number) =>
        Array.from({ length: count }, (_, i) =>
          makePost(from + i).operation("put"),
        )
      const slug = new Slug({ slug: "a", postId: makePost(1).id })

      // 1: creates the slug. 2: moves it. 3: conflicts. Undoing 1 before 2
      // would leave the slug behind at its first key.
      const error = yield* Effect.flip(
        DynamoDB.bulk([
          ...posts(1, 99),
          slug.operation("put"),
          slug.operation("update", { slug: "b" }),
          ...posts(100, 98),
          makePost(0).operation("put"),
        ]),
      )

      expect(error._tag).toBe("TransactionCanceledError")
      expect(yield* sandbox.diff(before)).toBe("")
    }),
  )

  it.effect("rolls back committed transactions when a later one dies", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      const before = yield* sandbox.snapshot
      class Exploding extends Post {
        override get GSI2PK(): string {
          throw new Error("boom")
        }
      }
      const exploding = new Exploding(makePost(101).values())

      const exit = yield* Effect.exit(
        DynamoDB.bulk([
          ...Array.from({ length: 100 }, (_, i) =>
            makePost(i + 1).operation("put"),
          ),
          exploding.operation("put"),
        ]),
      )

      expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true)
      expect(yield* sandbox.diff(before)).toBe("")
    }),
  )

  it.effect("reports a rollback that dies as BulkWriteRollbackError", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(0))
      class Exploding extends Post {
        override get GSI2PK(): string {
          throw new Error("rollback exploded")
        }
      }
      // The first transaction commits a write whose rollback cannot be built.
      const unrecoverable: Operation.TransactionOperation = {
        action: { _tag: "Put", item: makePost(1) },
        rollback: { _tag: "Put", item: new Exploding(makePost(1).values()) },
      }

      const error = yield* Effect.flip(
        DynamoDB.bulk([
          unrecoverable,
          ...Array.from({ length: 99 }, (_, i) =>
            makePost(i + 2).operation("put"),
          ),
          makePost(0).operation("put"),
        ]),
      )

      expect(error._tag).toBe("BulkWriteRollbackError")
      if (error._tag !== "BulkWriteRollbackError") return
      expect(error.requiresRollback).toHaveLength(100)
      expect(error.cause).toMatchObject({ _tag: "TransactionCanceledError" })
      expect(error.rollbackCause).toMatchObject({
        message: "rollback exploded",
      })
    }),
  )

  it.effect("rolls back committed transactions when a later one fails", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(0))
      const before = yield* sandbox.snapshot

      // 100 new posts commit in the first transaction; the second conflicts.
      const puts = Array.from({ length: 100 }, (_, i) =>
        makePost(i + 1).operation("put"),
      )
      const error = yield* Effect.flip(
        DynamoDB.bulk([...puts, makePost(0).operation("put")]),
      )

      expect(error._tag).toBe("TransactionCanceledError")
      expect(yield* sandbox.diff(before)).toBe("")
    }),
  )

  it.effect("keeps a group in one transaction", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      const puts = Array.from({ length: 99 }, (_, i) =>
        makePost(i + 1).operation("put"),
      )
      const group = [
        makePost(100).operation("put"),
        makePost(101).operation("put"),
      ]

      const { spans } = yield* recordSpans(DynamoDB.bulk([...puts, group]))

      const transactions = spans.filter(
        (span) => span.name === "DynamoDB.TransactWriteItems",
      )
      expect(
        transactions.map((span) =>
          span.attributes.get("aws.dynamodb.transact_items_count"),
        ),
      ).toEqual([99, 2])
      expect(Object.keys(yield* sandbox.snapshot)).toHaveLength(101)
    }),
  )

  it.effect("dies for a group of more than 100 operations", () =>
    Effect.gen(function* () {
      const group = Array.from({ length: 101 }, (_, i) =>
        makePost(i).operation("put"),
      )
      const exit = yield* Effect.exit(DynamoDB.bulk([group]))
      expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true)
    }),
  )

  it.effect("keeps an item that an overwriting put replaced", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(0), makePost(1))

      const error = yield* Effect.flip(
        DynamoDB.bulk([
          ...Array.from({ length: 99 }, (_, i) =>
            makePost(i + 2).operation("put"),
          ),
          makePost(1, { title: "Overwritten" }).operation("put", {
            ignoreExistence: true,
          }),
          makePost(0).operation("put"),
        ]),
      )

      expect(error._tag).toBe("TransactionCanceledError")
      // The overwrite stays; deleting it would lose the item it replaced.
      expect((yield* Post.load(makePost(1))).title).toBe("Overwritten")
      expect(yield* sandbox.get(makePost(2))).toBeUndefined()
    }),
  )

  it.effect("gives stored instances their version", () =>
    Effect.gen(function* () {
      const post = makePost(1)
      yield* DynamoDB.bulk([post.operation("put")])
      expect(docVersion(post)).toBe(0)

      yield* (yield* Post.load(post)).update({ title: "Elsewhere" })
      const error = yield* Effect.flip(post.update({ title: "Stale" }))
      expect(error._tag).toBe("RaceConditionError")
    }),
  )
})
