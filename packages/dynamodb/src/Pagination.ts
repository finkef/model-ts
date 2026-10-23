/**
 * Relay-style cursor pagination types. A cursor is the base64-encoded key of
 * an item. With an `encryptionKey` (see `CursorOptions`), the key is encrypted
 * and authenticated with AES-256-GCM, so clients can neither read nor forge
 * cursors.
 *
 * @since 7.0.0
 */
import type { Redacted } from "effect"

/** @since 7.0.0 */
export interface PageInfo {
  readonly hasPreviousPage: boolean
  readonly hasNextPage: boolean
  readonly startCursor: string | null
  readonly endCursor: string | null
}

/** @since 7.0.0 */
export interface Edge<T> {
  readonly node: T
  readonly cursor: string
}

/** @since 7.0.0 */
export interface PaginationResult<T> {
  readonly pageInfo: PageInfo
  readonly edges: ReadonlyArray<Edge<T>>
}

/**
 * GraphQL connection arguments. `first`/`after` page forward,
 * `last`/`before` page backward.
 *
 * @since 7.0.0
 */
export interface PaginationInput {
  readonly first?: number | null | undefined
  readonly last?: number | null | undefined
  readonly before?: string | null | undefined
  readonly after?: string | null | undefined
}

/** @since 7.0.0 */
export interface PaginationOptions {
  /** The most items one page returns. Defaults to 50. */
  readonly limit?: number | undefined
  /** The page size when the input has neither `first` nor `last`. Defaults to 20. */
  readonly default?: number | undefined
}

/**
 * A cursor encryption key: 32 bytes, as a `Uint8Array` or a base64 string,
 * optionally `Redacted` (as `Config.Redacted` returns it). Generate one with
 * `openssl rand -base64 32`.
 *
 * @since 7.0.0
 */
export type CursorKey =
  Uint8Array | string | Redacted.Redacted<Uint8Array | string>

/**
 * How pagination cursors are written. A cursor holds the item's `PK`, `SK`
 * and every GSI attribute, so the same cursor works in every connection the
 * item appears in.
 *
 * - `{ encryptionKey }`: cursors are encrypted and authenticated with
 *   AES-256-GCM. Clients can't read the keys, and an altered or forged cursor
 *   fails with `PaginationError`. `previousKeys` still decrypt cursors after
 *   the key is rotated.
 * - `"plain"`: cursors are base64-encoded JSON, so clients can read every key
 *   attribute of the item, GSI keys included. Choose it only when no key
 *   holds anything clients shouldn't see.
 *
 * @since 7.0.0
 */
export type CursorOptions =
  | "plain"
  | {
      readonly encryptionKey: CursorKey
      readonly previousKeys?: ReadonlyArray<CursorKey> | undefined
    }
