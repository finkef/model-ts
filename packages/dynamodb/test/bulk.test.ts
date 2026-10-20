import { describe, expect } from "@effect/vitest"
import { Cause, Effect, Exit } from "effect"
import { Dynamo, Operation } from "../src/index.ts"
import { Sandbox } from "../src/sandbox.ts"
import { makePost, Post } from "./fixtures.ts"
import { it } from "./utils.ts"

describe("bulk", () => {
  it.effect("runs operations of different kinds in one transaction", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1), makePost(2), makePost(4))
      const before = yield* sandbox.snapshot
      const second = yield* Post.load(makePost(2))

      yield* Dynamo.bulk([
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
        Dynamo.bulk([
          makePost(2).operation("put"),
          makePost(1).operation("put"),
        ]),
      )

      expect(error._tag).toBe("TransactionCanceledError")
      expect(yield* sandbox.diff(before)).toBe("")
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
        Dynamo.bulk([...puts, makePost(0).operation("put")]),
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

      const { spans } = yield* Effect.gen(function* () {
        const { recordSpans } = yield* Effect.promise(
          () => import("./utils.ts"),
        )
        return yield* recordSpans(Dynamo.bulk([...puts, group]))
      })

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
      const exit = yield* Effect.exit(Dynamo.bulk([group]))
      expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true)
    }),
  )
})
