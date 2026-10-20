/**
 * Relay-style cursor pagination. A cursor is the base64-encoded key of an
 * item, encrypted with AES-256-CTR when the table has a
 * `cursorEncryptionKey`. Cursors are opaque to clients.
 *
 * @since 7.0.0
 */
import { createCipheriv, createDecipheriv } from "node:crypto"
import { Effect } from "effect"
import { PaginationError } from "./Errors.ts"
import { GSI_NAMES, type GSI, type ItemKeys } from "./Keys.ts"

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

/** @internal */
export interface Page {
  readonly cursor: string | undefined
  readonly limit: number
  readonly direction: "forward" | "backward"
}

/** @internal Validates connection arguments and resolves the page size. */
export const decodePagination = (
  input: PaginationInput,
  options: Required<PaginationOptions>,
): Effect.Effect<Page, PaginationError> => {
  const { after, before, first, last } = input
  const conflict = (a: string, b: string) =>
    Effect.fail(
      new PaginationError({
        message: `Only one of "${a}" and "${b}" can be specified`,
      }),
    )
  if (before && after) return conflict("before", "after")
  if (first && last) return conflict("first", "last")
  if (before && first) return conflict("before", "first")
  if (last && after) return conflict("last", "after")
  if (first && first < 0) {
    return Effect.fail(
      new PaginationError({ message: '"first" must be positive' }),
    )
  }
  if (last && last < 0) {
    return Effect.fail(
      new PaginationError({ message: '"last" must be positive' }),
    )
  }
  return Effect.succeed({
    cursor: before ?? after ?? undefined,
    limit: Math.min(first ?? last ?? options.default, options.limit),
    direction: before || last ? "backward" : "forward",
  })
}

// A fixed IV keeps cursors stable: the same item always has the same cursor.
const IV = Buffer.from("Q05yyCR+0tyWl6glrZhlNw==", "base64")
const ALGORITHM = "aes-256-ctr"

const encrypt = (key: Uint8Array, cursor: string) => {
  const cipher = createCipheriv(ALGORITHM, key, IV)
  return Buffer.concat([cipher.update(cursor), cipher.final()]).toString(
    "base64",
  )
}

const decrypt = (key: Uint8Array, encrypted: string) => {
  const decipher = createDecipheriv(ALGORITHM, key, IV)
  return Buffer.concat([
    decipher.update(Buffer.from(encrypted, "base64")),
    decipher.final(),
  ]).toString()
}

/**
 * The cursor of an item: its primary key and GSI attributes.
 *
 * @since 7.0.0
 */
export const encodeCursor = (
  item: ItemKeys,
  encryptionKey?: Uint8Array,
): string => {
  const keys: Record<string, string | undefined> = { PK: item.PK, SK: item.SK }
  for (const gsi of GSI_NAMES) {
    keys[`${gsi}PK`] = item[`${gsi}PK`]
    keys[`${gsi}SK`] = item[`${gsi}SK`]
  }
  const cursor = Buffer.from(JSON.stringify(keys)).toString("base64")
  return encryptionKey ? encrypt(encryptionKey, cursor) : cursor
}

/**
 * The `ExclusiveStartKey` a cursor stands for on the given index.
 *
 * @since 7.0.0
 */
export const decodeCursor = (
  cursor: string,
  index?: GSI,
  encryptionKey?: Uint8Array,
): Effect.Effect<Record<string, string>, PaginationError> =>
  Effect.try({
    try: () => {
      const json = encryptionKey ? decrypt(encryptionKey, cursor) : cursor
      const { PK, SK, ...rest } = JSON.parse(
        Buffer.from(json, "base64").toString(),
      )
      if (typeof PK !== "string" || typeof SK !== "string") {
        throw new Error("Cursor has no PK and SK")
      }
      return index
        ? {
            PK,
            SK,
            [`${index}PK`]: rest[`${index}PK`],
            [`${index}SK`]: rest[`${index}SK`],
          }
        : { PK, SK }
    },
    catch: () => new PaginationError({ message: "Couldn't decode cursor" }),
  })
