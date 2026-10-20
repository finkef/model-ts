/**
 * Table keys. Every model stored with `@model-ts/dynamodb` exposes `PK` and
 * `SK` (usually as getters) and may expose `GSI2PK`/`GSI2SK` through
 * `GSI19PK`/`GSI19SK` for the secondary indexes. `GSI1` is the inverted
 * primary index (`SK`, `PK`) and needs no attributes of its own.
 *
 * @since 7.0.0
 */
import { Schema } from "effect"

/**
 * The secondary indexes that read their own `GSI<n>PK`/`GSI<n>SK` attributes.
 *
 * @since 7.0.0
 */
export const GSI_NAMES = [
  "GSI2",
  "GSI3",
  "GSI4",
  "GSI5",
  "GSI6",
  "GSI7",
  "GSI8",
  "GSI9",
  "GSI10",
  "GSI11",
  "GSI12",
  "GSI13",
  "GSI14",
  "GSI15",
  "GSI16",
  "GSI17",
  "GSI18",
  "GSI19",
] as const

/** @since 7.0.0 */
export type GSI = (typeof GSI_NAMES)[number]

/**
 * A primary key.
 *
 * @since 7.0.0
 */
export const Key = Schema.Struct({ PK: Schema.String, SK: Schema.String })

/** @since 7.0.0 */
export interface Key {
  readonly PK: string
  readonly SK: string
}

/**
 * The optional secondary index attributes of an item.
 *
 * @since 7.0.0
 */
export type GSIKeys = {
  readonly [K in `${GSI}PK` | `${GSI}SK`]?: string
}

/**
 * The primary key plus any secondary index attributes.
 *
 * @since 7.0.0
 */
export type ItemKeys = Key & GSIKeys

/** Just `PK` and `SK`, without the rest of the item. */
export const keyOf = (item: Key): Key => ({ PK: item.PK, SK: item.SK })

/** `PK`, `SK` and every GSI attribute, missing ones as `undefined`. */
export const keysOf = (item: ItemKeys): ItemKeys => {
  const keys: Key & Record<string, string | undefined> = {
    PK: item.PK,
    SK: item.SK,
  }
  for (const gsi of GSI_NAMES) {
    keys[`${gsi}PK`] = item[`${gsi}PK`]
    keys[`${gsi}SK`] = item[`${gsi}SK`]
  }
  return keys
}

/** `"PK / SK"`, the way keys appear in messages and sandbox diffs. */
export const formatKey = (key: Key) => `${key.PK} / ${key.SK}`
