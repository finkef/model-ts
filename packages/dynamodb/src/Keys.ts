/**
 * Table keys. Every model stored with `@model-ts/dynamodb` exposes `PK` and
 * `SK` (usually as getters) and may expose `GSI1PK`/`GSI1SK` through
 * `GSI19PK`/`GSI19SK` for the secondary indexes.
 *
 * @since 7.0.0
 */
import { Schema } from "effect"
import type { GSI_NAMES } from "./internal/keys.ts"

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
