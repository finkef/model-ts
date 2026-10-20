/**
 * Helpers for declaring fields. Each one is an ordinary Schema combinator, so
 * they compose with everything else in `effect/Schema`.
 *
 * ```ts
 * class Profile extends model<Profile>()("Profile", {
 *   bio: Field.optional(Schema.String), //       bio?: string
 *   avatar: Field.nullable(Schema.String), //    avatar: string | null
 *   website: Field.nullish(Schema.String), //    website?: string | null
 *   theme: Schema.String.pipe(Field.withDefault(() => "light")),
 * }) {}
 * ```
 *
 * @since 7.0.0
 */
import { Effect, Schema } from "effect"

/**
 * `key?: T`. The key may be missing. DynamoDB drops undefined attributes on
 * write, so a missing key round-trips as missing.
 *
 * @since 7.0.0
 */
export const optional: <S extends Schema.Top>(schema: S) => Schema.optional<S> =
  Schema.optional

/**
 * `key: T | null`. The key is always present and may be null.
 *
 * @since 7.0.0
 */
export const nullable: <S extends Schema.Top>(schema: S) => Schema.NullOr<S> =
  Schema.NullOr

/**
 * `key?: T | null`. The key may be missing or null.
 *
 * @since 7.0.0
 */
export const nullish = <S extends Schema.Top>(
  schema: S,
): Schema.optional<Schema.NullOr<S>> => Schema.optional(Schema.NullOr(schema))

/**
 * A default for both `new Model({...})` and decoding, computed on each use.
 * Use it for ids, timestamps and other values a new instance should get when
 * the caller leaves them out, and for stored rows that predate the field.
 * The default is a decoded value, so it works for transformed fields too.
 *
 * ```ts
 * createdAt: Schema.DateFromString.pipe(Field.withDefault(() => new Date()))
 * ```
 *
 * @since 7.0.0
 */
export const withDefault =
  <S extends Schema.Top & Schema.WithoutConstructorDefault>(
    makeDefault: () => S["Type"] & S["~type.make.in"],
  ) =>
  (schema: S) =>
    schema.pipe(
      Schema.withDecodingDefaultTypeKey(Effect.sync(makeDefault)),
      Schema.withConstructorDefault(Effect.sync(makeDefault)),
    )
