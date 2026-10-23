import { Effect, Schema } from "effect"
import { Model } from "../src/index.ts"

export const UserId = Schema.String.pipe(Schema.brand("UserId"))
export type UserId = typeof UserId.Type

export const Email = Schema.String.pipe(
  Schema.check(Schema.isPattern(/^[^@\s]+@[^@\s]+$/)),
  Schema.brand("Email"),
)

export class Address extends Model.Class<Address>()("Address", {
  city: Schema.String,
  zip: Schema.optional(Schema.String),
}) {}

export class User extends Model.Class<User>()("User", {
  id: UserId,
  email: Email,
  firstName: Schema.String,
  lastName: Schema.String,
  nickname: Schema.optional(Schema.NullOr(Schema.String)),
  address: Schema.NullOr(Address),
  createdAt: Schema.DateFromString,
  role: Schema.Literals(["admin", "member"]).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed("member" as const)),
    Schema.withConstructorDefault(Effect.succeed("member" as const)),
  ),
  visits: Schema.Number.pipe(Schema.withConstructorDefault(Effect.succeed(0))),
}) {
  get fullName() {
    return `${this.firstName} ${this.lastName}`
  }

  greet(greeting: string) {
    return `${greeting}, ${this.firstName}!`
  }
}

export const encodedUser = {
  _tag: "User",
  id: "user_01",
  email: "ada@example.com",
  firstName: "Ada",
  lastName: "Lovelace",
  address: { _tag: "Address", city: "London" },
  createdAt: "2026-01-01T00:00:00.000Z",
  role: "admin",
  visits: 3,
} as const
