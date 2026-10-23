/** Item codecs and the metadata kept beside decoded instances. */
import { Effect, Schema, SchemaAST } from "effect"
import { ItemDecodeError, ItemEncodeError } from "../Errors.ts"
import type {
  Decodable,
  RawItem,
  Recovered,
  TableModel,
  TableUnion,
} from "../Item.ts"
import type { ItemKeys, Key } from "../Keys.ts"
import { GSI_NAMES, keysOf } from "./keys.ts"

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

export const setDocVersion = (item: object, version: number) => {
  versions.set(item, version)
}

/**
 * The key attributes a decoded instance was stored with. They usually match
 * its getters, but a raw update can change a stored index key, and a cursor
 * must point at the row as it is stored.
 */
const storedKeys = new WeakMap<object, ItemKeys>()

/** The keys `item` was read with, or its own keys if it was not read. */
export const storedKeysOf = (item: ItemKeys): ItemKeys =>
  storedKeys.get(item) ?? item

const keysOfRaw = (raw: RawItem): ItemKeys | undefined => {
  if (typeof raw.PK !== "string" || typeof raw.SK !== "string") return undefined
  const keys: Key & Record<string, string> = { PK: raw.PK, SK: raw.SK }
  for (const gsi of GSI_NAMES) {
    for (const name of [`${gsi}PK`, `${gsi}SK`]) {
      const value = raw[name]
      if (typeof value === "string") keys[name] = value
    }
  }
  return keys
}

// -----------------------------------------------------------------------------
// Codecs
// -----------------------------------------------------------------------------

/**
 * The class an instance was built with. TypeScript types every
 * `constructor` as `Function`, so this is the one place the class is
 * asserted rather than inferred.
 */
export const modelOf = <A extends Key>(item: A) =>
  item.constructor as TableModel<A>

const isUnion = (decodable: Decodable): decodable is TableUnion =>
  "members" in decodable

/**
 * The `_tag` a model writes. It is the model's name, but a class made with
 * `.extend` keeps the `_tag` of the model it extends under a new name.
 */
const tagOf = (model: TableModel): string => {
  const ast = model.fields._tag?.ast
  return ast !== undefined &&
    SchemaAST.isLiteral(ast) &&
    typeof ast.literal === "string"
    ? ast.literal
    : model.identifier
}

/** Every `_tag` an item decoded by `decodable` can have. */
export const tagsOf = (decodable: Decodable): ReadonlyArray<string> =>
  isUnion(decodable) ? decodable.members.flatMap(tagsOf) : [tagOf(decodable)]

const namesOf = (decodable: Decodable): ReadonlyArray<string> =>
  isUnion(decodable)
    ? decodable.members.flatMap(namesOf)
    : [decodable.identifier]

/** The name used in errors and spans. */
export const nameOf = (decodable: Decodable) => namesOf(decodable).join(" | ")

/**
 * The stored form of an instance: encoded fields, then `_tag`, the keys and
 * `_docVersion`, in the same order as earlier releases.
 */
export const encodeItem = <A extends Key>(
  item: A,
): Effect.Effect<RawItem, ItemEncodeError> => {
  const model = modelOf(item)
  return Schema.encodeUnknownEffect(model)(item).pipe(
    Effect.map((encoded) => {
      const { _tag, ...fields } = encoded
      return Object.assign(fields, { _tag }, keysOf(item), {
        _docVersion: docVersion(item) ?? 0,
      })
    }),
    Effect.mapError(
      (cause) => new ItemEncodeError({ model: model.identifier, cause }),
    ),
  )
}

export const decodeItem = <A extends Key>(
  decodable: Decodable<A>,
  raw: RawItem,
): Effect.Effect<A, ItemDecodeError> =>
  Schema.decodeUnknownEffect(decodable)(raw).pipe(
    Effect.tap((item) =>
      Effect.sync(() => {
        setDocVersion(item, Number(raw._docVersion ?? 0))
        const keys = keysOfRaw(raw)
        if (keys) storedKeys.set(item, keys)
      }),
    ),
    Effect.mapError(
      (cause) =>
        new ItemDecodeError({
          model: nameOf(decodable),
          ...(typeof raw.PK === "string" &&
            typeof raw.SK === "string" && { key: { PK: raw.PK, SK: raw.SK } }),
          cause,
        }),
    ),
  )

// -----------------------------------------------------------------------------
// Soft deletion
// -----------------------------------------------------------------------------

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

/** Prefixes every key attribute and records when the item was deleted. */
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

export const markRecovered = <A extends object>(item: A): A & Recovered =>
  Object.assign(item, { isDeleted: true } as const)
