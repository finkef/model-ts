/**
 * Type-level tests, checked by `bun run typecheck`. Every `@ts-expect-error`
 * must stay an error, or the build fails.
 */
import { Context, Effect, Schema } from "effect"
import {
  Field,
  makeProvider,
  model,
  type ProviderMembers,
  union,
} from "../src/index.ts"
import { Address, User, UserId } from "./fixtures.ts"

declare const user: User

// Fields, getters and methods are on the instance type.
export const fullName: string = user.fullName
export const id: UserId = user.id
export const address: Address | null = user.address
export const nickname: string | null | undefined = user.nickname

// Decoders return the subclass.
export const fromUnknown: User = User.from({})
export const decoded: Effect.Effect<User, Schema.SchemaError> = User.decode({})
export const viaSchema: User = Schema.decodeUnknownSync(User)({})

// The constructor takes the decoded types, with defaults optional.
new User({
  email: user.email,
  firstName: "Ada",
  lastName: "Lovelace",
  address: null,
  createdAt: new Date(),
})
new User({
  email: user.email,
  firstName: "Ada",
  lastName: "Lovelace",
  address: null,
  // @ts-expect-error a string is not a Date
  createdAt: "2026-01-01",
})
new User({
  // @ts-expect-error a plain string is not a branded Email
  email: "ada@example.com",
  firstName: "Ada",
  lastName: "Lovelace",
  address: null,
  createdAt: new Date(),
})

// Ids are nominal.
// @ts-expect-error a raw string is not a UserId
export const rawId: UserId = "user_01"
// @ts-expect-error make() checks the prefix at the type level
UserId.make("post_01")

// values() has no _tag; encode() does.
// @ts-expect-error values() has no _tag
user.values()._tag
export const tag: string = user.encode()._tag

// Unions decode to the union of their members.
const Place = union([User, Address])
export const place: User | Address = Place.from({})

// A provider's this-constraints are enforced at the call site.
interface NeedsName extends ProviderMembers {
  readonly instance: { hello(this: { readonly name: string }): string }
}
const needsName = makeProvider<NeedsName>()({
  instanceProps: {
    hello() {
      return this.name
    },
  },
})
class Named extends model<Named>()("Named", needsName, {
  name: Schema.String,
}) {}
class Unnamed extends model<Unnamed>()("Unnamed", needsName, {
  id: Schema.Number,
}) {}
new Named({ name: "x" }).hello()
// @ts-expect-error Unnamed has no name field
new Unnamed({ id: 1 }).hello()

// An implementation must match its declared members.
makeProvider<NeedsName>()({
  // @ts-expect-error hello returns a number, declared string
  instanceProps: { hello: () => 1 },
})

// An implementation cannot leave out a group with required members.
// @ts-expect-error instanceProps is missing
makeProvider<NeedsName>()({})

// Field defaults are decoded values, also for transformed fields.
export const createdAt = Schema.DateFromString.pipe(
  Field.withDefault(() => new Date()),
)
// @ts-expect-error the default is the decoded Date, not its encoded string
Schema.DateFromString.pipe(Field.withDefault(() => "2026-01-01"))

// Fields that need Effect services cannot be model fields: construction,
// from() and encode() run synchronously.
class Locale extends Context.Service<Locale, string>()("test/Locale") {}
model()("Localized", {
  // @ts-expect-error decoding this field requires the Locale service
  locale: Schema.String.pipe(
    Schema.withDecodingDefaultKey(
      Effect.gen(function* () {
        return yield* Locale
      }),
    ),
  ),
})
