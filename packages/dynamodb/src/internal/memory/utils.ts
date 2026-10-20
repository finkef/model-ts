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
  [...items].sort((a, b) => {
    const pkA = String(a.PK ?? "")
    const pkB = String(b.PK ?? "")
    if (pkA !== pkB) return pkA < pkB ? -1 : 1

    const skA = String(a.SK ?? "")
    const skB = String(b.SK ?? "")
    if (skA !== skB) return skA < skB ? -1 : 1

    return 0
  })
