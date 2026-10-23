import type { GSI, ItemKeys, Key } from "../Keys.ts"

/**
 * The secondary indexes. Each one is keyed by its own `GSI<n>PK` and
 * `GSI<n>SK` attributes.
 */
export const GSI_NAMES = [
  "GSI1",
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

/** Whether `name` is one of the table's secondary indexes. */
export const isGSI = (name: string): name is GSI =>
  GSI_NAMES.some((gsi) => gsi === name)

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
