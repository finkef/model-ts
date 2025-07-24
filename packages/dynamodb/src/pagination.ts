import crypto from "crypto"
import { PaginationError } from "./errors"
import { GSI, GSI_NAMES, GSIPK, GSISK } from "./gsi"

const SIV = "Q05yyCR+0tyWl6glrZhlNw=="
const ENCRYPTION_ALG = "aes-256-ctr"

export interface PageInfo {
  hasPreviousPage: boolean
  hasNextPage: boolean
  startCursor: string | null
  endCursor: string | null
}

export interface Edge<T> {
  node: T
  cursor: string
}

export interface PaginationResult<T> {
  pageInfo: PageInfo
  edges: Edge<T>[]
}

export enum PaginationDirection {
  FORWARD,
  BACKWARD,
}

export interface PaginationInput {
  first?: number | null
  last?: number | null
  before?: string | null
  after?: string | null
}

export interface PaginationOptions {
  /**
   * Maximum number of items to return.
   */
  limit?: number

  /**
   * Default number of items to return if no limit is provided.
   */
  default?: number
}

const DEFAULT_OPTIONS = {
  limit: 50,
  default: 20,
}

export function decodePagination(
  pagination: PaginationInput,
  paginationOptions: PaginationOptions = DEFAULT_OPTIONS
): {
  cursor?: string
  limit: number
  direction: PaginationDirection
} {
  const { after, before, first, last } = pagination

  if (before && after)
    throw new PaginationError(
      `Only one of "before" and "after" can be specified`
    )
  if (first && last)
    throw new PaginationError(`Only one of "first" and "last" can be specified`)
  if (before && first)
    throw new PaginationError(
      `Only one of "before" and "first" can be specified`
    )
  if (last && after)
    throw new PaginationError(`Only one of "last" and "after" can be specified`)

  if (first && first < 0) throw new PaginationError(`"first" must be positive`)
  if (last && last < 0) throw new PaginationError(`"last" must be positive`)

  return {
    cursor: before ?? after ?? undefined,
    limit: Math.min(
      first ?? last ?? paginationOptions.default ?? DEFAULT_OPTIONS.default,
      paginationOptions.limit ?? DEFAULT_OPTIONS.limit
    ),
    direction:
      before || last
        ? PaginationDirection.BACKWARD
        : PaginationDirection.FORWARD,
  }
}

/**
 * Utility function to encrypt a cursor with AES-256-CTR, but uses a
 * synthetic initialization vector (SIV) to ensure that the same cursor
 * produces the same encrypted value.
 */
const encryptCursor = (key: Buffer, cursor: string) => {
  const cipher = crypto.createCipheriv(
    ENCRYPTION_ALG,
    key,
    Buffer.from(SIV, "base64")
  )

  const encrypted = Buffer.concat([cipher.update(cursor), cipher.final()])

  return encrypted.toString("base64")
}

/**
 * Utility function to decrypt a cursor with AES-256-CTR, but uses a
 * synthetic initialization vector (SIV) to ensure that the same cursor
 * produces the same encrypted value.
 */
const decryptCursor = (key: Buffer, encryptedCursor: string) => {
  try {
    const decipher = crypto.createDecipheriv(
      ENCRYPTION_ALG,
      key,
      Buffer.from(SIV, "base64")
    )

    const decrypted = Buffer.concat([
      decipher.update(Buffer.from(encryptedCursor, "base64")),
      decipher.final(),
    ]).toString()

    return decrypted
  } catch (error) {
    return null
  }
}

export const encodeDDBCursor = (
  {
    PK,
    SK,
    ...values
  }: {
    PK: string
    SK: string
  } & { [key in GSIPK]?: string } &
    { [key in GSISK]?: string },
  encryptionKey?: Buffer
) => {
  const cursor = Buffer.from(
    JSON.stringify({
      PK,
      SK,
      ...GSI_NAMES.map((GSI) => ({
        [`${GSI}PK`]: values[`${GSI}PK` as const],
        [`${GSI}SK`]: values[`${GSI}SK` as const],
      })).reduce((acc, cur) => Object.assign(acc, cur), {}),
    })
  ).toString("base64")

  if (encryptionKey) return encryptCursor(encryptionKey, cursor)

  return cursor
}

export const decodeDDBCursor = (
  encoded: string,
  index?: GSI,
  encryptionKey?: Buffer
) => {
  try {
    const json = encryptionKey ? decryptCursor(encryptionKey, encoded) : encoded
    // const json = encoded

    if (!json) throw new Error("Couldn't decrypt cursor")

    const { PK, SK, ...values } = JSON.parse(
      Buffer.from(json, "base64").toString()
    )

    if (typeof PK !== "string" || typeof SK !== "string") throw new Error()

    if (!index) return { PK, SK }

    return {
      PK,
      SK,
      [`${index}PK`]: values[`${index}PK`],
      [`${index}SK`]: values[`${index}SK`],
    }
  } catch (error) {
    throw new PaginationError("Couldn't decode cursor")
  }
}
