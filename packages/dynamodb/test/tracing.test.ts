import { describe, expect } from "@effect/vitest"
import { Effect, Option } from "effect"
import { Sandbox } from "../src/Sandbox.ts"
import { makePost, Post } from "./fixtures.ts"
import { it, recordSpans } from "./utils.ts"

describe("tracing", () => {
  it.effect(
    "wraps each AWS call in a client span inside the operation span",
    () =>
      Effect.gen(function* () {
        const sandbox = yield* Sandbox
        yield* sandbox.seed(makePost(1))

        const { spans } = yield* recordSpans(
          Post.get(makePost(1)).pipe(
            Effect.flatMap((post) => post.update({ title: "Traced" })),
          ),
        )

        const byName = new Map(spans.map((span) => [span.name, span]))
        expect([...byName.keys()]).toEqual([
          "DynamoDB.get",
          "DynamoDB.GetItem",
          "DynamoDB.update",
          "DynamoDB.put",
          "DynamoDB.PutItem",
        ])

        const getItem = byName.get("DynamoDB.GetItem")!
        expect(getItem.kind).toBe("client")
        expect(Option.getOrUndefined(getItem.parent)?.spanId).toBe(
          byName.get("DynamoDB.get")!.spanId,
        )
        expect(Object.fromEntries(getItem.attributes)).toEqual({
          "db.system.name": "aws.dynamodb",
          "db.operation.name": "GetItem",
          "aws.dynamodb.table_names": [sandbox.tableName],
          "rpc.system": "aws-api",
          "rpc.service": "DynamoDB",
          "rpc.method": "GetItem",
        })
        expect(
          Object.fromEntries(byName.get("DynamoDB.update")!.attributes),
        ).toEqual({
          "model_ts.model": "Post",
          "model_ts.key": "POST#post_001 / POST",
        })
      }),
  )

  it.effect("records failures on the span", () =>
    Effect.gen(function* () {
      const { spans } = yield* recordSpans(Effect.exit(Post.get(makePost(1))))
      const get = spans.find((span) => span.name === "DynamoDB.get")!
      expect(get.status._tag).toBe("Ended")
      expect(get.status._tag === "Ended" && get.status.exit._tag).toBe(
        "Failure",
      )
    }),
  )
})
