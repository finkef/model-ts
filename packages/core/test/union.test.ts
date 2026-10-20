import { describe, expect, it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import {
  makeProvider,
  model,
  type ProviderMembers,
  union,
} from "../src/index.ts"

class Circle extends model<Circle>()("Circle", { size: Schema.Number }) {
  get area() {
    return Math.PI * this.size ** 2
  }
}
class Square extends model<Square>()("Square", { size: Schema.Number }) {
  get area() {
    return this.size ** 2
  }
}

interface DescribedMembers extends ProviderMembers {
  readonly union: {
    describe(this: {
      readonly members: ReadonlyArray<{ readonly identifier: string }>
    }): string
  }
}
const described = makeProvider<DescribedMembers>()({
  unionProps: {
    describe() {
      return this.members.map((member) => member.identifier).join(" | ")
    },
  },
})

const Shape = union([Circle, Square])

describe("union()", () => {
  it("decodes by _tag when the members share their fields", () => {
    expect(Shape.from({ _tag: "Square", size: 2 })).toBeInstanceOf(Square)
    expect(Shape.from({ _tag: "Circle", size: 2 })).toBeInstanceOf(Circle)
  })

  it("falls back to the first member that matches when _tag is missing", () => {
    expect(Shape.from({ size: 2 })).toBeInstanceOf(Circle)
  })

  it("encodes each member with its own tag", () => {
    expect(Schema.encodeSync(Shape)(new Square({ size: 3 }))).toEqual({
      _tag: "Square",
      size: 3,
    })
  })

  it.effect("decode fails with a SchemaError", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(Shape.decode({ _tag: "Triangle" }))
      expect(error).toBeInstanceOf(Schema.SchemaError)
    }),
  )

  it("nests in models", () => {
    class Drawing extends model<Drawing>()("Drawing", {
      shapes: Schema.Array(Shape),
    }) {}
    const drawing = Drawing.from({ shapes: [{ _tag: "Square", size: 2 }] })
    expect(drawing.shapes[0]!.area).toBe(4)
  })

  it("adds provider union members", () => {
    const Described = union([Circle, Square], described)
    expect(Described.describe()).toBe("Circle | Square")
  })
})
