import { describe, expect, it } from "@effect/vitest"
import { Schema } from "effect"
import {
  makeProvider,
  mergeProviders,
  model,
  type ProviderMembers,
} from "../src/index.ts"

interface CounterInstance<Self> {
  readonly doubled: number
  increment(this: Self & { readonly count: number }): Self
}
interface CounterStatics<Self> {
  readonly kind: string
  zero(this: new (values: { count: number }) => Self): Self
}
interface CounterMembers extends ProviderMembers {
  readonly instance: CounterInstance<this["Self"]>
  readonly static: CounterStatics<this["Self"]>
}

const counter = makeProvider<CounterMembers>()({
  instanceProps: {
    get doubled() {
      return (this as unknown as { count: number }).count * 2
    },
    increment() {
      const Self = this.constructor as new (values: { count: number }) => any
      return new Self({ count: this.count + 1 })
    },
  },
  classProps: {
    kind: "counter",
    zero() {
      return new this({ count: 0 })
    },
  },
})

class Counter extends model<Counter>()("Counter", counter, {
  count: Schema.Number,
}) {}

describe("providers", () => {
  it("add instance members that return the model type", () => {
    const next = new Counter({ count: 1 }).increment()
    expect(next).toBeInstanceOf(Counter)
    expect(next.count).toBe(2)
  })

  it("keep getters as getters", () => {
    expect(new Counter({ count: 3 }).doubled).toBe(6)
    expect(Counter.from({ count: 4 }).doubled).toBe(8)
  })

  it("add class members", () => {
    expect(Counter.kind).toBe("counter")
    expect(Counter.zero().count).toBe(0)
  })

  it("do not leak into encoded output or values()", () => {
    const value = new Counter({ count: 1 })
    expect(value.encode()).toEqual({ _tag: "Counter", count: 1 })
    expect(value.values()).toEqual({ count: 1 })
  })
})

interface LabelMembers<Label extends string> extends ProviderMembers {
  readonly instance: { label(): Label }
  readonly static: { readonly source: Label }
}
const first = makeProvider<LabelMembers<"first">>()({
  instanceProps: { label: () => "first" },
  classProps: { source: "first" },
})
const second = makeProvider<LabelMembers<"second">>()({
  instanceProps: { label: () => "second" },
  classProps: { source: "second" },
})

describe("mergeProviders", () => {
  it("lets the later provider win, at runtime and in the types", () => {
    class Merged extends model<Merged>()("Merged", [first, second], {}) {}
    const label: "second" = new Merged().label()
    expect(label).toBe("second")
    expect(Merged.source).toBe("second")
  })

  it("merges an array the same way as mergeProviders()", () => {
    const merged = mergeProviders([second, first])
    class Merged extends model<Merged>()("Merged", merged, {}) {}
    expect(new Merged().label()).toBe("first")
  })

  it("combines members from different providers", () => {
    class Both extends model<Both>()("Both", [counter, first], {
      count: Schema.Number,
    }) {}
    const both = new Both({ count: 1 })
    expect(both.label()).toBe("first")
    expect(both.increment().count).toBe(2)
  })
})
