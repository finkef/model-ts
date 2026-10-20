/**
 * One nominal type per id. An `Id` is a string that starts with a prefix and
 * carries a brand, so a `UserId` cannot be passed where a `PostId` is expected
 * even though both are strings at runtime and in storage.
 *
 * ```ts
 * const UserId = Id("user_", "UserId")
 * type UserId = typeof UserId.Type
 *
 * UserId.make("user_01J...") // UserId
 * UserId.make("post_01J...") // type error: wrong prefix
 * const id = yield* UserId.create // new ULID-based id, from Effect's Clock and Random
 * ```
 *
 * @since 7.0.0
 */
import { Clock, Effect, Random, Schema } from "effect"
import { withDefault } from "./Field.ts"
import { ulid } from "./internal/ulid.ts"

/**
 * The schema `Id(prefix, brand)` returns.
 *
 * @since 7.0.0
 */
export type Id<Prefix extends string, Brand extends string> = Schema.brand<
  Schema.TemplateLiteral<readonly [Prefix, Schema.String]>,
  Brand
> & {
  readonly prefix: Prefix
  /** A new ULID-based id. Time and randomness come from Effect's `Clock` and `Random`. */
  readonly create: Effect.Effect<
    Schema.brand<
      Schema.TemplateLiteral<readonly [Prefix, Schema.String]>,
      Brand
    >["Type"]
  >
  /** A new ULID-based id from the system clock and `crypto.getRandomValues`. */
  readonly generate: () => Schema.brand<
    Schema.TemplateLiteral<readonly [Prefix, Schema.String]>,
    Brand
  >["Type"]
}

/**
 * An id schema: `${prefix}${string}`, branded as `brand`.
 *
 * @since 7.0.0
 */
export const Id = <const Prefix extends string, const Brand extends string>(
  prefix: Prefix,
  brand: Brand,
): Id<Prefix, Brand> => {
  const schema = Schema.TemplateLiteral([prefix, Schema.String]).pipe(
    // Schema.brand expects a literal key; a generic one is still a single
    // brand, which is what the cast restores.
    Schema.brand<Brand>(brand as never),
    Schema.annotate({
      identifier: brand,
      message: `Expected ${brand}, a string starting with "${prefix}"`,
    }),
  )
  type Type = typeof schema.Type
  const brandId = (value: string) => value as Type
  return Object.assign(schema, {
    prefix,
    create: Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis
      const draws: Array<number> = []
      for (let i = 0; i < 16; i++) draws.push(yield* Random.next)
      return brandId(prefix + ulid(now, draws))
    }),
    generate: () => brandId(prefix + ulid(Date.now())),
  })
}

/**
 * An id field that fills itself in when a new instance leaves it out, and
 * when a stored row has none.
 *
 * ```ts
 * class Post extends model<Post>()("Post", { id: generated(PostId), ... }) {}
 * new Post({ title: "Hi" }).id // "post_01J..."
 * ```
 *
 * For deterministic ids in tests, pass `yield* PostId.create` explicitly and
 * pin `TestClock` and `Random.withSeed`.
 *
 * @since 7.0.0
 */
export const generated = <Prefix extends string, Brand extends string>(
  id: Id<Prefix, Brand>,
) => id.pipe(withDefault(id.generate))
