import * as Effect from "effect/Effect"
import * as t from "io-ts"
import { decode, RuntimeTypeValidationError } from "../effect"
import { model } from "../model"
import { union } from "../union"

type IsExact<T, U> = (<G>() => G extends T ? 1 : 2) extends (<G>() =>
  G extends U ? 1 : 2)
  ? true
  : false

describe("decode", () => {
  class User extends model("User", t.type({ name: t.string })) {
    get greeting() {
      return `Hello, ${this.name}`
    }
  }

  class Admin extends model("Admin", t.type({ level: t.number })) {}

  const userEffect = decode(User, { name: "Ada" })
  const userEffectType: IsExact<
    typeof userEffect,
    Effect.Effect<InstanceType<typeof User>, RuntimeTypeValidationError, never>
  > = true

  void userEffectType

  it("decodes models to subclass instances", async () => {
    const user = await Effect.runPromise(userEffect)

    expect(user).toBeInstanceOf(User)
    expect(user.greeting).toBe("Hello, Ada")
  })

  it("fails with a tagged runtime validation error", async () => {
    const error = await Effect.runPromise(
      Effect.flip(decode(User, { name: 42 }))
    )

    expect(error).toBeInstanceOf(RuntimeTypeValidationError)
    expect(error).toBeInstanceOf(Error)
    expect(error._tag).toBe("RuntimeTypeValidationError")
    expect(error.errors.length).toBeGreaterThan(0)
  })

  it("supports recovery with catchTag", () => {
    const error = Effect.runSync(
      decode(User, { name: 42 }).pipe(
        Effect.catchTag("RuntimeTypeValidationError", Effect.succeed)
      )
    )

    expect(error).toBeInstanceOf(RuntimeTypeValidationError)
  })

  it("decodes tagged union members and plain codecs", () => {
    const UserOrAdmin = union([User, Admin])
    const member = Effect.runSync(
      decode(UserOrAdmin, { _tag: "Admin", level: 1 })
    )

    expect(member).toBeInstanceOf(Admin)
    expect(Effect.runSync(decode(t.string, "model-ts"))).toBe("model-ts")
  })

  it("does not run decoders until the effect is executed", () => {
    let calls = 0
    const codec = new t.Type(
      "tracked",
      t.string.is,
      (value, context) => {
        calls += 1
        return t.string.validate(value, context)
      },
      t.identity
    )

    const effect = decode(codec, "model-ts")

    expect(calls).toBe(0)
    expect(Effect.runSync(effect)).toBe("model-ts")
    expect(calls).toBe(1)
  })
})
