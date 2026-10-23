/**
 * What can be stored and read: model classes with `PK`/`SK` on their
 * instances, and unions of them.
 *
 * @since 7.0.0
 */
import type { Model } from "@model-ts/core"
import type { Schema } from "effect"
import type { Key } from "./Keys.ts"

/**
 * An item as DynamoDB stores it.
 *
 * @since 7.0.0
 */
export type RawItem = Record<string, unknown>

/**
 * A model instance with a primary key, as `put`, `update` and the other
 * writes take it.
 *
 * @since 7.0.0
 */
export interface TableItem extends Key {
  values(): Readonly<Record<string, unknown>>
}

/**
 * A model class whose instances have a primary key. Updates rebuild an
 * instance from its `values()` and a patch through `make`, so its input is
 * stated as a record of field values, or `void` for a model whose fields
 * all have defaults.
 *
 * @since 7.0.0
 */
export interface TableModel<A extends Key = Key> extends Schema.Top {
  readonly Type: A
  readonly Encoded: RawItem
  readonly DecodingServices: never
  readonly EncodingServices: never
  readonly "~type.make.in": Readonly<Record<string, unknown>> | void
  readonly identifier: string
  readonly fields: Model.Fields
}

/**
 * A union of table models, as built by `Model.Union([A, B], DynamoDB.provider)`.
 *
 * @since 7.0.0
 */
export interface TableUnion<A extends Key = Key> extends Schema.Top {
  readonly Type: A
  readonly DecodingServices: never
  readonly EncodingServices: never
  readonly members: ReadonlyArray<TableModel | TableUnion>
}

/**
 * Anything an item can be decoded with.
 *
 * @since 7.0.0
 */
export type Decodable<A extends Key = Key> = TableModel<A> | TableUnion<A>

/**
 * Added to the result of `load(key, { recover: true })`.
 *
 * @since 7.0.0
 */
export interface Recovered {
  /** `true` when the item was read from its soft-deleted location. */
  readonly isDeleted?: true
}

export { deletedKey, docVersion } from "./internal/item.ts"
