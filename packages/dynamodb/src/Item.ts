/**
 * What can be stored and read: model classes with `PK`/`SK` on their
 * instances, and unions of them.
 *
 * @since 7.0.0
 */
import { Effect, Schema } from "effect"
import { ItemDecodeError, ItemEncodeError } from "./Errors.ts"
import { GSI_NAMES, type ItemKeys, type Key, keysOf } from "./Keys.ts"

/**
 * An item as DynamoDB stores it.
 *
 * @since 7.0.0
 */
export type RawItem = Record<string, unknown>

/**
 * A model class whose instances have a primary key.
 *
 * @since 7.0.0
 */
export interface TableModel<A extends Key = any> extends Schema.Codec<A, any> {
  readonly identifier: string
  readonly fields: Schema.Struct.Fields
  new (values: any): A
}

/**
 * A union of table models, as built by `union([A, B], dynamodb)`.
 *
 * @since 7.0.0
 */
export interface TableUnion<A extends Key = any> extends Schema.Codec<A, any> {
  readonly members: ReadonlyArray<TableModel | TableUnion>
}

/**
 * Anything an item can be decoded with.
 *
 * @since 7.0.0
 */
export type Decodable<A extends Key = any> = TableModel<A> | TableUnion<A>

/**
 * Added to the result of `load(key, { recover: true })`.
 *
 * @since 7.0.0
 */
export interface Recovered {
  /** `true` when the item was read from its soft-deleted location. */
  readonly isDeleted?: true
}

// -----------------------------------------------------------------------------
// Versions
// -----------------------------------------------------------------------------

/**
 * `_docVersion` is persistence metadata, so it lives beside the instance
 * rather than on it: it never shows up in `values()`, `encode()` or equality.
 */
const versions = new WeakMap<object, number>()

/**
 * The `_docVersion` an instance was read or last written with, or
 * `undefined` for an instance that was never stored.
 *
 * @since 7.0.0
 */
export const docVersion = (item: object): number | undefined =>
  versions.get(item)

/** @internal */
export const setDocVersion = (item: object, version: number) => {
  versions.set(item, version)
}

// -----------------------------------------------------------------------------
// Codecs
// -----------------------------------------------------------------------------

/** @internal */
export const modelOf = <A extends Key>(item: A) =>
  item.constructor as TableModel<A>

const isUnion = (decodable: Decodable): decodable is TableUnion =>
  "members" in decodable

/** @internal Every `_tag` an item decoded by `decodable` can have. */
export const tagsOf = (decodable: Decodable): ReadonlyArray<string> =>
  isUnion(decodable)
    ? decodable.members.flatMap(tagsOf)
    : [decodable.identifier]

/** @internal The name used in errors and spans. */
export const nameOf = (decodable: Decodable) => tagsOf(decodable).join(" | ")

type Decode = (input: unknown) => Effect.Effect<any, Schema.SchemaError>
type Encode = (input: unknown) => Effect.Effect<any, Schema.SchemaError>
const decoders = new WeakMap<object, Decode>()
const encoders = new WeakMap<object, Encode>()

const decoderOf = (schema: Schema.Codec<any, any>): Decode => {
  let decode = decoders.get(schema)
  if (!decode) {
    decode = Schema.decodeUnknownEffect(schema)
    decoders.set(schema, decode)
  }
  return decode
}

/** @internal */
export const encoderOf = (schema: Schema.Codec<any, any>): Encode => {
  let encode = encoders.get(schema)
  if (!encode) {
    encode = Schema.encodeUnknownEffect(schema)
    encoders.set(schema, encode)
  }
  return encode
}

/**
 * @internal
 * The stored form of an instance: encoded fields, then `_tag`, the keys and
 * `_docVersion`, in the same order as earlier releases.
 */
export const encodeItem = <A extends Key>(
  item: A,
): Effect.Effect<RawItem, ItemEncodeError> => {
  const model = modelOf(item)
  return encoderOf(model)(item).pipe(
    Effect.map((encoded: RawItem) => {
      const { _tag, ...fields } = encoded
      return Object.assign(fields, { _tag }, keysOf(item as ItemKeys), {
        _docVersion: docVersion(item) ?? 0,
      })
    }),
    Effect.mapError(
      (cause) => new ItemEncodeError({ model: model.identifier, cause }),
    ),
  )
}

/** @internal */
export const decodeItem = <A extends Key>(
  decodable: Decodable<A>,
  raw: RawItem,
): Effect.Effect<A, ItemDecodeError> =>
  decoderOf(decodable)(raw).pipe(
    Effect.tap((item: A) =>
      Effect.sync(() => setDocVersion(item, Number(raw._docVersion ?? 0))),
    ),
    Effect.mapError(
      (cause) => new ItemDecodeError({ model: nameOf(decodable), cause }),
    ),
  )

// -----------------------------------------------------------------------------
// Soft deletion
// -----------------------------------------------------------------------------

/** @internal */
export const DELETED_PREFIX = "$$DELETED$$"

/**
 * Where `softDelete` moves an item.
 *
 * @since 7.0.0
 */
export const deletedKey = (key: Key): Key => ({
  PK: `${DELETED_PREFIX}${key.PK}`,
  SK: `${DELETED_PREFIX}${key.SK}`,
})

/** @internal Prefixes every key attribute and records when the item was deleted. */
export const applySoftDeletionFields = (
  item: RawItem,
  deletedAt: string,
): RawItem => {
  const prefixed = (value: unknown) =>
    typeof value === "string" ? `${DELETED_PREFIX}${value}` : undefined
  const result: RawItem = { _deletedAt: deletedAt, ...item }
  result.PK = prefixed(item.PK)
  result.SK = prefixed(item.SK)
  for (const gsi of GSI_NAMES) {
    result[`${gsi}PK`] = prefixed(item[`${gsi}PK`])
    result[`${gsi}SK`] = prefixed(item[`${gsi}SK`])
  }
  return result
}

/** @internal */
export const markRecovered = <A extends object>(item: A): A & Recovered =>
  Object.defineProperty(item, "isDeleted", {
    value: true,
    enumerable: true,
    writable: true,
    configurable: true,
  }) as A & Recovered
