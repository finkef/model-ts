/** The store behind `RequestCache`: what one request knows about each key. */
import type { RawItem } from "../Item.ts"

/**
 * The PK and SK of a stored item, as DynamoDB returns them in items and
 * transaction keys. Both are present on anything read from or written to the
 * table; the type only says so loosely because those records are untyped.
 */
export interface StoredKey {
  readonly PK?: unknown
  readonly SK?: unknown
}

/** The stored item at a key, or `undefined` when the key holds no item. */
export interface Entry {
  readonly item: RawItem | undefined
}

/**
 * Items by table and key. Writes are recorded as they succeed. A read is
 * recorded only if no write or `clear` happened to its key while it was in
 * flight, so a slow read never replaces what a newer write stored.
 */
export class ItemCache {
  readonly #entries = new Map<string, Entry>()
  /** When each key was last written or evicted, on `#clock`. */
  readonly #changedAt = new Map<string, number>()
  #clock = 0
  #clearedAt = 0

  /** What the request knows about `key`, if anything. */
  lookup(tableName: string, key: StoredKey): Entry | undefined {
    return this.#entries.get(identify(tableName, key))
  }

  /** Taken before a read and passed to `fill` with its result. */
  token(): number {
    return this.#clock
  }

  /** Records a read that started at `token`, unless the key changed since. */
  fill(
    tableName: string,
    key: StoredKey,
    token: number,
    item: RawItem | undefined,
  ): void {
    const id = identify(tableName, key)
    if (this.#clearedAt > token || (this.#changedAt.get(id) ?? 0) > token) {
      return
    }
    this.#entries.set(id, { item })
  }

  /** Records a successful write: the item now stored, or none. */
  set(tableName: string, key: StoredKey, item: RawItem | undefined): void {
    const id = identify(tableName, key)
    this.#changedAt.set(id, ++this.#clock)
    this.#entries.set(id, { item: item && stored(item) })
  }

  /** Forgets `key`; the next load reads it from the table. */
  evict(tableName: string, key: StoredKey): void {
    const id = identify(tableName, key)
    this.#changedAt.set(id, ++this.#clock)
    this.#entries.delete(id)
  }

  /** Forgets every key, including reads still in flight. */
  clear(): void {
    this.#clearedAt = ++this.#clock
    this.#entries.clear()
  }
}

/** A string that identifies a key; unambiguous whatever PK and SK contain. */
const identify = (tableName: string, key: StoredKey) =>
  JSON.stringify([tableName, key.PK, key.SK])

/**
 * An item as DynamoDB stores it: the client drops `undefined` attributes,
 * list entries and map values, so a cached item decodes like a read one. The
 * copy also keeps the entry independent of the object that was written.
 */
const stored = (item: RawItem): RawItem => {
  const result: RawItem = {}
  for (const [name, value] of Object.entries(item)) {
    if (value !== undefined) result[name] = copy(value)
  }
  return result
}

const copy = (value: unknown): unknown => {
  if (Array.isArray(value)) {
    return value.filter((entry) => entry !== undefined).map(copy)
  }
  if (isPlainObject(value)) return stored(value)
  return value
}

const isPlainObject = (value: unknown): value is RawItem => {
  if (typeof value !== "object" || value === null) return false
  const prototype: unknown = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}
