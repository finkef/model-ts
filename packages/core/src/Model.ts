/**
 * `model()` turns a set of Schema fields into a class. The class is a real
 * `Schema.Class`: it can be nested in other schemas, used in unions and
 * passed to every Schema API. On top of that it has `from`, `decode`,
 * `encode` and `values`, and whatever members its providers add.
 *
 * ```ts
 * class User extends model<User>()("User", {
 *   firstName: Schema.String,
 *   lastName: Schema.String,
 * }) {
 *   get fullName() {
 *     return `${this.firstName} ${this.lastName}`
 *   }
 * }
 * ```
 *
 * Two choices differ from a plain `Schema.TaggedClass`:
 *
 * - `new User({...})` does not validate. The input is already typed (brands
 *   included), so construction only fills in constructor defaults. Data from
 *   outside is validated where it enters: `User.from`, `User.decode` or any
 *   Schema decoder. `User.make` is still there when you want a validating
 *   constructor.
 * - `_tag` is optional when decoding, at any depth, and always written when
 *   encoding.
 *
 * @since 7.0.0
 */
import { Effect, Schema, SchemaAST } from "effect"
import {
  type AsProvider,
  defineMembers,
  type InstanceMembers,
  type NoMembers,
  noProvider,
  type Provider,
  type ProviderInput,
  type StaticMembers,
  toProvider,
} from "./Provider.ts"

type Fields = Schema.Struct.Fields

const modelTag = <Tag extends string>(tag: Tag) =>
  Schema.tag(tag).pipe(Schema.withDecodingDefaultKey(Effect.succeed(tag)))

type ModelTag<Tag extends string> = ReturnType<typeof modelTag<Tag>>

/** The Struct behind a model: its fields plus `_tag`. */
export type ModelStruct<Tag extends string, F extends Fields> = Schema.Struct<
  { readonly _tag: ModelTag<Tag> } & F
>

/**
 * The field values of a model, without `_tag`.
 *
 * @since 7.0.0
 */
export type Values<F> = F extends Fields
  ? Omit<Schema.Struct.Type<F>, "_tag">
  : never

/**
 * Members every model instance has.
 *
 * @since 7.0.0
 */
export interface ModelMethods<F extends Fields> {
  /**
   * The encoded form, e.g. for JSON APIs or event payloads.
   *
   * @throws {Schema.SchemaError} when a field holds a value its schema cannot encode
   */
  encode(): Schema.Struct.Encoded<F> & { readonly _tag: string }
  /** The field values, without `_tag`, getters or provider members. */
  values(): Values<F>
}

/**
 * Members every model class has, in addition to the `Schema.Class` API.
 *
 * @since 7.0.0
 */
export interface ModelStatics<Self> {
  /**
   * Decodes untrusted input into an instance.
   *
   * @throws {Schema.SchemaError} when the input does not match the schema
   */
  from(input: unknown): Self
  /** Decodes untrusted input into an instance, failing with a `SchemaError`. */
  decode(input: unknown): Effect.Effect<Self, Schema.SchemaError>
}

/**
 * The class `model()` returns. Model and provider instance members sit next to
 * the fields; class members are added to the constructor.
 *
 * @since 7.0.0
 */
export type Model<
  Self,
  Tag extends string,
  F extends Fields,
  P extends Provider,
> = Schema.Class<
  Self,
  ModelStruct<Tag, F>,
  ModelMethods<ModelStruct<Tag, F>["fields"]> & InstanceMembers<P, Self, F>
> &
  ModelStatics<Self> &
  StaticMembers<P, Self, F>

/**
 * @since 7.0.0
 */
export interface ModelFactory<Self> {
  /** A model without providers. */
  <const Tag extends string, const F extends Fields>(
    tag: Tag,
    fields: F,
  ): Model<Self, Tag, F, Provider<NoMembers>>
  /** A model with the members of one provider, or of several merged left to right. */
  <
    const Tag extends string,
    const P extends ProviderInput,
    const F extends Fields,
  >(
    tag: Tag,
    provider: P,
    fields: F,
  ): Model<Self, Tag, F, AsProvider<P>>
}

/**
 * Defines a model class. Pass the class itself as `Self` so that decoders and
 * provider members return your subclass, getters included.
 *
 * ```ts
 * class Post extends model<Post>()("Post", dynamodb, {
 *   id: PostId,
 *   title: Schema.String,
 * }) {
 *   get PK() {
 *     return `POST#${this.id}`
 *   }
 *   get SK() {
 *     return "POST"
 *   }
 * }
 * ```
 *
 * @since 7.0.0
 */
export const model = <Self = never>(): ModelFactory<Self> =>
  ((tag: string, ...rest: [Fields] | [ProviderInput, Fields]) => {
    const [provider, fields] =
      rest.length === 1 ? [noProvider, rest[0]] : [toProvider(rest[0]), rest[1]]

    // TypeScript cannot extend Schema's conditional class type inside a
    // generic function, so the class is built on `any` and the factory's
    // return type restores the precise type.
    const Base: any = Schema.Class<any>(tag)(uncheckedStruct(tag, fields))
    const fieldNames = Object.keys(fields)

    class Model extends Base {
      encode() {
        return Schema.encodeSync(
          this.constructor as Schema.Codec<unknown, unknown>,
        )(this)
      }
      values() {
        const values: Record<string, unknown> = {}
        for (const name of fieldNames) {
          if (name in this) values[name] = this[name as keyof this]
        }
        return values
      }
      static from(input: unknown) {
        return Schema.decodeUnknownSync(
          this as unknown as Schema.Codec<unknown, unknown>,
        )(input)
      }
      static decode(input: unknown) {
        return Schema.decodeUnknownEffect(
          this as unknown as Schema.Codec<unknown, unknown>,
        )(input)
      }
    }

    defineMembers(Model.prototype, provider.instanceProps)
    defineMembers(Model, provider.classProps)
    return Model
  }) as ModelFactory<Self>

/**
 * A Struct whose `make` trusts its input.
 *
 * `Schema.Class` builds each instance through `struct.make(input)`, which
 * validates. Decoding takes a different path (the class AST), so replacing
 * `make` on this private Struct changes construction only: constructor
 * defaults are applied and nothing is checked. The core test suite pins this
 * behaviour, so an Effect upgrade that changes it fails loudly.
 */
const uncheckedStruct = (tag: string, fields: Fields) => {
  const struct = Schema.Struct({ _tag: modelTag(tag), ...fields })
  const defaults = constructorDefaults(fields)
  return Object.defineProperty(struct, "make", {
    value: (input: Record<string, unknown> = {}) => {
      const value: Record<string, unknown> = { ...input, _tag: tag }
      for (const [name, makeDefault] of defaults) {
        if (value[name] === undefined) value[name] = Effect.runSync(makeDefault)
      }
      return value
    },
  })
}

/** Every field with a constructor default, however the default was attached. */
const constructorDefaults = (fields: Fields) =>
  Object.entries(fields).flatMap(([name, field]) => {
    const makeDefault = SchemaAST.toType(field.ast).context?.constructorDefault
    return makeDefault ? [[name, makeDefault] as const] : []
  })
