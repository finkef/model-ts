import crypto from "crypto"

export type InMemoryItem = { [key: string]: any }

export const ITEM_KEY_SEPARATOR = "\u0000"

export const encodeCompositeKey = (...parts: string[]): string =>
  parts
    .map((part) => `${part.length}:${part}`)
    .join(ITEM_KEY_SEPARATOR)

export const encodeItemKey = (pk: string, sk: string): string =>
  encodeCompositeKey(pk, sk)

export const encodeIndexEntryKey = (rangeKey: string, itemKey: string): string =>
  `${rangeKey}${ITEM_KEY_SEPARATOR}${itemKey}`

export const cloneItem = <T>(item: T): T => JSON.parse(JSON.stringify(item))

export const stablePriority = (
  indexName: string,
  hashKey: string,
  rangeKey: string,
  itemKey: string
): number => {
  const hash = crypto
    .createHash("sha256")
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
