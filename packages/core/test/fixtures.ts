import { Effect, Schema } from "effect"
import { Field, generated, Id, model } from "../src/index.ts"

export const UserId = Id("user_", "UserId")
export type UserId = typeof UserId.Type

export const Email = Schema.String.pipe(
  Schema.check(Schema.isPattern(/^[^@\s]+@[^@\s]+$/)),
  Schema.brand("Email"),
)

export class Address extends model<Address>()("Address", {
  city: Schema.String,
  zip: Field.optional(Schema.String),
}) {}

export class User extends model<User>()("User", {
  id: generated(UserId),
  email: Email,
  firstName: Schema.String,
  lastName: Schema.String,
  nickname: Field.nullish(Schema.String),
  address: Field.nullable(Address),
  createdAt: Schema.DateFromString,
  role: Schema.Literals(["admin", "member"]).pipe(
    Field.withDefault(() => "member" as const),
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
