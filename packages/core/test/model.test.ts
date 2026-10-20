import { describe, expect, it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { Address, encodedUser, User, UserId } from "./fixtures.ts"

const { _tag: _, ...untagged } = encodedUser

describe("new Model(...)", () => {
  it("does not validate its input", () => {
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

  it("applies constructor defaults, however they were declared", () => {
    const user = User.make({
      email: Schema.decodeSync(User.fields.email)("ada@example.com"),
      firstName: "Ada",
      lastName: "Lovelace",
      address: null,
      createdAt: new Date(0),
    })
    const constructed = new User({ ...user.values(), id: undefined as never })

    expect(constructed.id).toMatch(/^user_[0-9A-HJKMNP-TV-Z]{26}$/)
    expect(constructed.role).toBe("member")
    expect(constructed.visits).toBe(0)
    expect(constructed._tag).toBe("User")
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
    const { id: _id, role: _role, ...rest } = encodedUser
    const user = User.from({ ...rest, visits: 1 })
    expect(user.id.startsWith("user_")).toBe(true)
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
