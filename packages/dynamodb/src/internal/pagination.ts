import {
  createCipheriv,
  createDecipheriv,
  createHmac,
  hkdfSync,
} from "node:crypto"
import { Effect, Redacted, Result } from "effect"
import { PaginationError } from "../Errors.ts"
import type { GSI, ItemKeys } from "../Keys.ts"
import type {
  CursorKey,
  CursorOptions,
  PaginationInput,
  PaginationOptions,
} from "../Pagination.ts"
import { GSI_NAMES } from "./keys.ts"

export interface Page {
  readonly cursor: string | undefined
  readonly limit: number
  readonly direction: "forward" | "backward"
}

/** Validates connection arguments and resolves the page size. */
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
  const given = (value: unknown) => value !== undefined && value !== null
  if (given(before) && given(after)) return conflict("before", "after")
  if (given(first) && given(last)) return conflict("first", "last")
  if (given(before) && given(first)) return conflict("before", "first")
  if (given(last) && given(after)) return conflict("last", "after")
  for (const [name, count] of [
    ["first", first],
    ["last", last],
  ] as const) {
    if (given(count) && !(Number.isSafeInteger(count) && Number(count) >= 0)) {
      return Effect.fail(
        new PaginationError({
          message: `"${name}" must be a non-negative integer`,
        }),
      )
    }
  }
  return Effect.succeed({
    cursor: before ?? after ?? undefined,
    limit: Math.min(first ?? last ?? options.default, options.limit),
    direction: given(before) || given(last) ? "backward" : "forward",
  })
}

const ALGORITHM = "aes-256-gcm"
const IV_LENGTH = 12
const TAG_LENGTH = 16

/** The length of a cursor encryption key, in bytes. */
export const CURSOR_KEY_LENGTH = 32

/** 32 bytes in base64, with or without padding, standard or URL-safe. */
const BASE64_KEY = /^[A-Za-z0-9+/_-]{43}=?$/

/**
 * The keys cursors are encrypted with (the first) and decrypted with (any),
 * copied so that changing the caller's arrays cannot change them. Empty for
 * plain cursors. Fails with a message when a key is not 32 bytes.
 */
export const parseCursorKeys = (
  cursors: CursorOptions,
): Result.Result<ReadonlyArray<Uint8Array>, string> => {
  if (cursors === "plain") return Result.succeed([])
  const keys: Array<Uint8Array> = []
  for (const [index, input] of [
    cursors.encryptionKey,
    ...(cursors.previousKeys ?? []),
  ].entries()) {
    const name = index === 0 ? "encryptionKey" : `previousKeys[${index - 1}]`
    const key = toBytes(input)
    if (key === undefined) {
      return Result.fail(
        `cursors.${name} must be ${CURSOR_KEY_LENGTH} bytes, as a Uint8Array or a base64 string`,
      )
    }
    keys.push(key)
  }
  return Result.succeed(keys)
}

const toBytes = (input: CursorKey): Uint8Array | undefined => {
  const key: Uint8Array | string = Redacted.isRedacted(input)
    ? Redacted.value(input)
    : input
  const bytes =
    typeof key === "string"
      ? BASE64_KEY.test(key)
        ? Uint8Array.from(Buffer.from(key, "base64"))
        : undefined
      : Uint8Array.from(key)
  return bytes?.length === CURSOR_KEY_LENGTH ? bytes : undefined
}

interface Subkeys {
  readonly encryption: Buffer
  readonly iv: Buffer
}

/** Separate keys for encryption and for deriving IVs, from the table's key. */
const subkeys = new WeakMap<Uint8Array, Subkeys>()
const subkeysOf = (key: Uint8Array): Subkeys => {
  let derived = subkeys.get(key)
  if (!derived) {
    if (key.length !== CURSOR_KEY_LENGTH) {
      throw new RangeError(
        `A cursor encryption key must be ${CURSOR_KEY_LENGTH} bytes, not ${key.length}`,
      )
    }
    const derive = (info: string) =>
      Buffer.from(hkdfSync("sha256", key, new Uint8Array(0), info, 32))
    derived = {
      encryption: derive("model-ts cursor encryption"),
      iv: derive("model-ts cursor iv"),
    }
    subkeys.set(key, derived)
  }
  return derived
}

/**
 * `iv | ciphertext | tag`. The IV is an HMAC of the plaintext, so the same
 * item always gets the same cursor, and an IV repeats only for the same
 * plaintext, which then encrypts to the same cursor and reveals nothing else.
 */
const encrypt = (key: Uint8Array, plaintext: string) => {
  const { encryption, iv: ivKey } = subkeysOf(key)
  const iv = createHmac("sha256", ivKey)
    .update(plaintext)
    .digest()
    .subarray(0, IV_LENGTH)
  const cipher = createCipheriv(ALGORITHM, encryption, iv)
  const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()])
  return Buffer.concat([iv, ciphertext, cipher.getAuthTag()]).toString("base64")
}

/** Throws when the cursor was not made with `key` or was altered. */
const decrypt = (key: Uint8Array, cursor: string) => {
  const data = Buffer.from(cursor, "base64")
  if (data.length < IV_LENGTH + TAG_LENGTH) throw new Error("Cursor too short")
  const decipher = createDecipheriv(
    ALGORITHM,
    subkeysOf(key).encryption,
    data.subarray(0, IV_LENGTH),
  )
  decipher.setAuthTag(data.subarray(data.length - TAG_LENGTH))
  return Buffer.concat([
    decipher.update(data.subarray(IV_LENGTH, data.length - TAG_LENGTH)),
    decipher.final(),
  ]).toString()
}

/**
 * The cursor of an item: its primary key and GSI attributes.
 */
export const encodeCursor = (
  item: ItemKeys,
  keys: ReadonlyArray<Uint8Array>,
): string => {
  const attributes: Record<string, string | undefined> = {
    PK: item.PK,
    SK: item.SK,
  }
  for (const gsi of GSI_NAMES) {
    attributes[`${gsi}PK`] = item[`${gsi}PK`]
    attributes[`${gsi}SK`] = item[`${gsi}SK`]
  }
  const json = JSON.stringify(attributes)
  const [current] = keys
  return current ? encrypt(current, json) : Buffer.from(json).toString("base64")
}

/** The plaintext of a cursor, decrypted with whichever key made it. */
const open = (cursor: string, keys: ReadonlyArray<Uint8Array>): string => {
  if (keys.length === 0) return Buffer.from(cursor, "base64").toString()
  for (const key of keys) {
    try {
      return decrypt(key, cursor)
    } catch {
      // Made with another key, or altered: try the next one.
    }
  }
  throw new Error("No key decrypts the cursor")
}

/**
 * The `ExclusiveStartKey` a cursor stands for on the given index.
 */
export const decodeCursor = (
  cursor: string,
  index: GSI | undefined,
  keys: ReadonlyArray<Uint8Array>,
): Effect.Effect<Record<string, string>, PaginationError> =>
  Effect.try({
    try: () => {
      const { PK, SK, ...rest } = JSON.parse(open(cursor, keys))
      if (typeof PK !== "string" || typeof SK !== "string") {
        throw new Error("Cursor has no PK and SK")
      }
      if (!index) return { PK, SK }
      const indexPK: unknown = rest[`${index}PK`]
      const indexSK: unknown = rest[`${index}SK`]
      if (typeof indexPK !== "string" || typeof indexSK !== "string") {
        throw new Error(`Cursor has no ${index} keys`)
      }
      return { PK, SK, [`${index}PK`]: indexPK, [`${index}SK`]: indexSK }
    },
    catch: () => new PaginationError({ message: "Couldn't decode cursor" }),
  })
