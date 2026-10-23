import type { Key } from "../Keys.ts"

/**
 * The key of an item in a sandbox snapshot. JSON keeps any two primary keys
 * apart, whatever characters they contain.
 */
export const snapshotKey = (key: Key): string =>
  JSON.stringify([key.PK, key.SK])

/** The primary key a snapshot key stands for, if it is one. */
export const parseSnapshotKey = (
  key: string,
): { readonly PK: string; readonly SK: string } | undefined => {
  try {
    const parsed: unknown = JSON.parse(key)
    return Array.isArray(parsed) &&
      parsed.length === 2 &&
      typeof parsed[0] === "string" &&
      typeof parsed[1] === "string"
      ? { PK: parsed[0], SK: parsed[1] }
      : undefined
  } catch {
    return undefined
  }
}
