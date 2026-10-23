/**
 * `Model.Class` turns a set of Schema fields into a class. The class is a real
 * `Schema.Class`: it can be nested in other schemas, used in unions and
 * passed to every Schema API. On top of that it has `from`, `decode`,
 * `encode` and `values`, and whatever members its providers add.
 *
 * ```ts
 * class User extends Model.Class<User>()("User", {
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
 * - `new User({...})` skips filters and brands (`disableChecks`). The input
 *   is already typed, brands included, so construction fills in constructor
 *   defaults and checks the shape only. Data from outside is validated where
 *   it enters: `User.from`, `User.decode` or any Schema decoder. `User.make`
 *   and `new User(input, { disableChecks: false })` validate fully.
 * - `_tag` is optional when decoding, at any depth, and always written when
 *   encoding.
 *
 * @since 7.0.0
 */
import { Effect, Schema } from "effect"
import { defineMembers, noProvider, toProvider } from "./internal/provider.ts"
import type {
  AsProvider,
  Input,
  InstanceMembers,
  NoMembers,
  Provider,
  StaticMembers,
} from "./Provider.ts"

export { type Any, Union, type UnionStatics } from "./Union.ts"

type StructFields = Schema.Struct.Fields

/**
 * The fields a model accepts: Schemas that decode and encode without Effect
 * services. Construction, `from` and `encode` run synchronously, so a field
 * whose transformation or default needs a service could not run there.
 *
 * @since 7.0.0
 */
export type Fields = {
  readonly [name: string]: Schema.Top & {
    readonly DecodingServices: never
    readonly EncodingServices: never
  }
}

const modelTag = <Tag extends string>(tag: Tag) =>
  Schema.tag(tag).pipe(Schema.withDecodingDefaultKey(Effect.succeed(tag)))

type ModelTag<Tag extends string> = ReturnType<typeof modelTag<Tag>>

/** The Struct behind a model: its fields plus `_tag`. */
export type Struct<Tag extends string, F extends StructFields> = Schema.Struct<
  { readonly _tag: ModelTag<Tag> } & F
>

/**
 * The field values of a model, without `_tag`.
 *
 * @since 7.0.0
 */
export type Values<F> = F extends StructFields
  ? Omit<Schema.Struct.Type<F>, "_tag">
  : never

/**
 * Members every model instance has.
 *
 * @since 7.0.0
 */
export interface Methods<F extends StructFields> {
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
export interface Statics<Self> {
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
 * The class `Model.Class` returns. Model and provider instance members sit next to
 * the fields; class members are added to the constructor.
 *
 * @since 7.0.0
 */
export type Class<
  Self,
  Tag extends string,
  F extends StructFields,
  P extends Provider,
> = Schema.Class<
  Self,
  Struct<Tag, F>,
  Methods<Struct<Tag, F>["fields"]> & InstanceMembers<P, Self, F>
> &
  Statics<Self> &
  StaticMembers<P, Self, F>

/**
 * @since 7.0.0
 */
export interface Factory<Self> {
  /** A model without providers. */
  <const Tag extends string, const F extends Fields>(
    tag: Tag,
    fields: F,
  ): Class<Self, Tag, F, Provider<NoMembers>>
  /** A model with the members of one provider, or of several merged left to right. */
  <const Tag extends string, const P extends Input, const F extends Fields>(
    tag: Tag,
    provider: P,
    fields: F,
  ): Class<Self, Tag, F, AsProvider<P>>
}

/**
 * Defines a model class. Pass the class itself as `Self` so that decoders and
 * provider members return your subclass, getters included.
 *
 * ```ts
 * class Post extends Model.Class<Post>()("Post", DynamoDB.provider, {
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
export const Class = <Self = never>(): Factory<Self> =>
  // The one cast in this module. `makeModel` builds the class from fields it
  // only knows as `Fields`; the factory's signature states the precise
  // class for the fields and providers of each call, which no implementation
  // can be checked against.
  ((tag: string, ...rest: [Fields] | [Input, Fields]) =>
    rest.length === 1
      ? makeModel(tag, noProvider, rest[0])
      : makeModel(tag, toProvider(rest[0]), rest[1])) as Factory<Self>

const makeModel = (tag: string, provider: Provider, fields: Fields) => {
  const struct = Schema.Struct({ _tag: modelTag(tag), ...fields })
  const Base = Schema.Class<unknown>(tag)(struct)

  /**
   * The class an instance was built with: this model, or a class made from
   * it with `.extend`, which has more fields.
   */
  const schemaOf = (instance: object): ModelSchema =>
    isModelSchema(instance.constructor) ? instance.constructor : ModelClass

  class ModelClass extends Base {
    // Fields are read by name below; the factory's type lists the real ones.
    readonly [field: string]: unknown

    constructor(
      input?: ConstructorParameters<typeof Base>[0],
      options?: Schema.MakeOptions,
    ) {
      super(input, { disableChecks: true, ...options })
    }
    encode() {
      return Schema.encodeSync(schemaOf(this))(this)
    }
    values() {
      const values: Record<string, unknown> = {}
      for (const name of Object.keys(schemaOf(this).fields)) {
        if (name !== "_tag" && name in this) values[name] = this[name]
      }
      return values
    }
    static from(input: unknown) {
      return Schema.decodeUnknownSync(this)(input)
    }
    static decode(input: unknown) {
      return Schema.decodeUnknownEffect(this)(input)
    }
  }

  defineMembers(ModelClass.prototype, provider.instanceProps)
  defineMembers(ModelClass, provider.classProps)
  return ModelClass
}

/** A Schema class: a codec without service requirements, with its fields. */
type ModelSchema = Schema.Codec<unknown, unknown> & {
  readonly fields: Schema.Struct.Fields
}

const isModelSchema = (value: unknown): value is ModelSchema =>
  Schema.isSchema(value) && "fields" in value
