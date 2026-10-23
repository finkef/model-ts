import { describe, expect, it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { Model } from "../src/index.ts"
import { Address, encodedUser, User, UserId } from "./fixtures.ts"

const { _tag: _, ...untagged } = encodedUser

describe("new Model(...)", () => {
  it("skips filters and brands", () => {
    // The pattern check on email would reject this when decoding.
    const user = new User({
      id: UserId.make("user_01"),
      email: "not an email" as never,
      firstName: "Ada",
      lastName: "Lovelace",
      address: null,
      createdAt: new Date(0),
    })
    expect(user.email).toBe("not an email")
  })

  it("still rejects a value of the wrong type", () => {
    expect(
      () =>
        new User({
          id: UserId.make("user_01"),
          email: 1 as never,
          firstName: "Ada",
          lastName: "Lovelace",
          address: null,
          createdAt: new Date(0),
        }),
    ).toThrow()
  })

  it("validates fully with disableChecks: false", () => {
    const input = {
      id: UserId.make("user_01"),
      email: "not an email" as never,
      firstName: "Ada",
      lastName: "Lovelace",
      address: null,
      createdAt: new Date(0),
    }
    expect(() => new User(input, { disableChecks: false })).toThrow()
  })

  it("applies constructor defaults", () => {
    const user = new User({
      id: UserId.make("user_01"),
      email: Schema.decodeSync(User.fields.email)("ada@example.com"),
      firstName: "Ada",
      lastName: "Lovelace",
      address: null,
      createdAt: new Date(0),
    })

    expect(user.role).toBe("member")
    expect(user.visits).toBe(0)
    expect(user._tag).toBe("User")
  })

  it("keeps explicit values over defaults", () => {
    const user = User.from(encodedUser)
    const copy = new User(user.values())
    expect(copy.id).toBe("user_01")
    expect(copy.role).toBe("admin")
  })
})

describe("Model.make", () => {
  it("validates, for callers that want it", () => {
    expect(() =>
      User.make({
        id: UserId.make("user_01"),
        email: "not an email" as never,
        firstName: "Ada",
        lastName: "Lovelace",
        address: null,
        createdAt: new Date(0),
      }),
    ).toThrow("Schema validation failed")
  })
})

describe("Model.from", () => {
  it("decodes into the subclass, with getters and methods", () => {
    const user = User.from(encodedUser)
    expect(user).toBeInstanceOf(User)
    expect(user.fullName).toBe("Ada Lovelace")
    expect(user.greet("Hi")).toBe("Hi, Ada!")
    expect(user.createdAt).toEqual(new Date("2026-01-01T00:00:00.000Z"))
  })

  it("does not need _tag, at any depth", () => {
    const user = User.from({ ...untagged, address: { city: "London" } })
    expect(user._tag).toBe("User")
    expect(user.address).toBeInstanceOf(Address)
  })

  it("applies decoding defaults for missing keys", () => {
    const { role: _role, ...rest } = encodedUser
    const user = User.from({ ...rest, visits: 1 })
    expect(user.role).toBe("member")
  })

  it("throws a SchemaError for invalid input", () => {
    expect(() => User.from({ ...encodedUser, email: "nope" })).toThrow(
      Schema.SchemaError,
    )
  })

  it("rejects a value tagged as a different model", () => {
    expect(() => Address.from({ _tag: "User", city: "London" })).toThrow(
      Schema.SchemaError,
    )
  })
})

describe("Model.decode", () => {
  it.effect("succeeds with the instance", () =>
    Effect.gen(function* () {
      const user = yield* User.decode(encodedUser)
      expect(user.fullName).toBe("Ada Lovelace")
    }),
  )

  it.effect("fails with a SchemaError", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(User.decode({ email: 1 }))
      expect(error).toBeInstanceOf(Schema.SchemaError)
    }),
  )
})

describe("instance.encode()", () => {
  it("writes _tag at every depth and encodes field transformations", () => {
    const user = User.from({ ...untagged, address: { city: "London" } })
    expect(user.encode()).toEqual(encodedUser)
  })

  it("round-trips through from()", () => {
    const user = User.from(encodedUser)
    expect(User.from(user.encode())).toEqual(user)
  })
})

describe("instance.values()", () => {
  it("returns the fields without _tag and leaves out missing optional keys", () => {
    const values = User.from(encodedUser).values()
    expect(Object.keys(values)).toEqual([
      "id",
      "email",
      "firstName",
      "lastName",
      "address",
      "createdAt",
      "role",
      "visits",
    ])
  })
})

describe("a model is a Schema", () => {
  it("nests in other schemas", () => {
    const Users = Schema.Array(User)
    const [user] = Schema.decodeUnknownSync(Users)([encodedUser])
    expect(user).toBeInstanceOf(User)
    expect(Schema.encodeSync(Users)([user!])).toEqual([encodedUser])
  })

  it("works with Schema.is", () => {
    expect(Schema.is(User)(User.from(encodedUser))).toBe(true)
    expect(Schema.is(User)(encodedUser)).toBe(false)
  })
})

describe("extend", () => {
  class Base extends Model.Class<Base>()("Base", { a: Schema.String }) {}
  class Child extends Base.extend<Child>("Child")({ b: Schema.Number }) {}

  it("encodes and lists the fields the extension adds", () => {
    const child = new Child({ a: "x", b: 1 })
    expect(child.encode()).toEqual({ _tag: "Base", a: "x", b: 1 })
    expect(child.values()).toEqual({ a: "x", b: 1 })
  })
})
