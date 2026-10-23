import { createHash } from "node:crypto"

export type InMemoryItem = { [key: string]: any }

export const ITEM_KEY_SEPARATOR = "\u0000"

export const encodeCompositeKey = (...parts: string[]): string =>
  parts.map((part) => `${part.length}:${part}`).join(ITEM_KEY_SEPARATOR)

export const encodeItemKey = (pk: string, sk: string): string =>
  encodeCompositeKey(pk, sk)

export const encodeIndexEntryKey = (
  rangeKey: string,
  itemKey: string,
): string => `${rangeKey}${ITEM_KEY_SEPARATOR}${itemKey}`

/**
 * A deep copy of an item. Binary values and sets are copied; attributes set
 * to `undefined`, and `undefined` list and set members, are left out, as
 * the AWS SDK leaves them out.
 */
export const cloneItem = <T>(item: T): T => cloneValue(item) as T

const isDefined = (value: unknown) => value !== undefined

const cloneValue = (value: unknown): unknown => {
  if (value instanceof Uint8Array) return new Uint8Array(value)
  if (value instanceof Set) {
    return new Set([...value].filter(isDefined).map(cloneValue))
  }
  if (Array.isArray(value)) return value.filter(isDefined).map(cloneValue)
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, entry]) => isDefined(entry))
        .map(([key, entry]) => [key, cloneValue(entry)]),
    )
  }
  return value
}

/**
 * Whether two stored items hold the same attributes. Sets compare by their
 * members, as DynamoDB sets are unordered.
 */
export const sameItem = (
  left: InMemoryItem | undefined,
  right: InMemoryItem | undefined,
) => canonical(left) === canonical(right)

const canonical = (value: unknown): string => {
  if (value === undefined) return "undefined"
  if (value instanceof Uint8Array) {
    return `bytes(${Buffer.from(value).toString("base64")})`
  }
  if (value instanceof Set) {
    return `set(${[...value].map(canonical).toSorted().join(",")})`
  }
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`
  if (value !== null && typeof value === "object") {
    const entries = Object.entries(value)
      .filter(([, entry]) => isDefined(entry))
      .toSorted(([left], [right]) => compareStrings(left, right))
      .map(([key, entry]) => `${JSON.stringify(key)}:${canonical(entry)}`)
    return `{${entries.join(",")}}`
  }
  // DynamoDB numbers compare by value, whichever type they decoded to.
  if (typeof value === "number" || typeof value === "bigint") {
    return `number(${value})`
  }
  return JSON.stringify(value)
}

export const stablePriority = (
  indexName: string,
  hashKey: string,
  rangeKey: string,
  itemKey: string,
): number => {
  const hash = createHash("sha256")
    .update(`${indexName}::${hashKey}::${rangeKey}::${itemKey}`)
    .digest()

  return hash.readUInt32BE(0)
}

export const sortItemsByPKSK = (items: InMemoryItem[]): InMemoryItem[] =>
  [...items].toSorted(
    (a, b) =>
      compareStrings(String(a.PK ?? ""), String(b.PK ?? "")) ||
      compareStrings(String(a.SK ?? ""), String(b.SK ?? "")),
  )

const isSurrogate = (unit: number) => unit >= 0xd800 && unit <= 0xdfff

/**
 * Orders strings like DynamoDB, by their UTF-8 bytes, which is code point
 * order. JavaScript compares UTF-16 code units, which puts characters above
 * U+FFFF (stored as surrogate pairs, U+D800 to U+DFFF) before U+E000 to
 * U+FFFF. Where the first difference is a surrogate against a character
 * below U+10000, the surrogate's character is the larger one.
 */
export const compareStrings = (left: string, right: string): number => {
  const length = Math.min(left.length, right.length)
  for (let index = 0; index < length; index += 1) {
    const a = left.charCodeAt(index)
    const b = right.charCodeAt(index)
    if (a === b) continue
    if (isSurrogate(a) !== isSurrogate(b)) return isSurrogate(a) ? 1 : -1
    return a < b ? -1 : 1
  }
  return left.length === right.length ? 0 : left.length < right.length ? -1 : 1
}
