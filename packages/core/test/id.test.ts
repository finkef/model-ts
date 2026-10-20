import { describe, expect, it } from "@effect/vitest"
import { Effect, Random, Schema } from "effect"
import { TestClock } from "effect/testing"
import { Id } from "../src/index.ts"

const PostId = Id("post_", "PostId")

describe("Id", () => {
  it("accepts strings with the prefix and rejects others", () => {
    expect(Schema.decodeUnknownSync(PostId)("post_1")).toBe("post_1")
    expect(() => Schema.decodeUnknownSync(PostId)("user_1")).toThrow()
  })

  it("generates prefixed ULIDs", () => {
    const id = PostId.generate()
    expect(id).toMatch(/^post_[0-9A-HJKMNP-TV-Z]{26}$/)
    expect(PostId.generate()).not.toBe(id)
  })

  it.effect("create is deterministic under TestClock and a seeded Random", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse("2020-11-09T23:00:00.000Z"))
      const a = yield* PostId.create.pipe(Random.withSeed("model-ts"))
      const b = yield* PostId.create.pipe(Random.withSeed("model-ts"))
      expect(a).toBe(b)
      // The time part sorts by creation time.
      expect(a.slice(5, 15)).toBe("01EPQKSMC0")
    }),
  )
})
