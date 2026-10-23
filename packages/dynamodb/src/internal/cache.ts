/**
 * The store behind `RequestCache`: what one request knows about each key, and
 * the reads and writes of it still in flight.
 */
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb"
import { Context, type Deferred, Effect, Option } from "effect"
import type { DynamoDBError } from "../Errors.ts"
import type { RawItem } from "../Item.ts"
import type { Key } from "../Keys.ts"
import type { RequestCache, RequestCacheService } from "../RequestCache.ts"
import { marshallOptions } from "./table.ts"

/** Brands the values `RequestCache.make` returns. */
export const TypeId = "~@model-ts/dynamodb/RequestCache"

/** The key of the `RequestCache` service. */
export const KEY = "@model-ts/dynamodb/RequestCache"

/**
 * The `RequestCache` service, for the operations. Like `TableService`, it
 * shares its key with the public class so that this module needs no runtime
 * import of `RequestCache.ts`.
 */
export const CacheService = Context.Service<RequestCache, RequestCacheService>(
  KEY,
)

/** The stores behind the service values, which keep them opaque. */
const stores = new WeakMap<RequestCacheService, ItemCache>()

export const makeService = (): RequestCacheService => {
  const service: RequestCacheService = { [TypeId]: TypeId }
  stores.set(service, new ItemCache())
  return service
}

/** The store of the current request, or `undefined` without a cache. */
export const currentCache: Effect.Effect<ItemCache | undefined> =
  Effect.flatMap(Effect.serviceOption(CacheService), (service) => {
    if (Option.isNone(service)) return Effect.succeed(undefined)
    const store = stores.get(service.value)
    return store
      ? Effect.succeed(store)
      : Effect.die(
          new TypeError("Provide a RequestCache made by RequestCache.make"),
        )
  })

/** The stored item at a key, or `undefined` when the key holds no item. */
export interface Entry {
  readonly item: RawItem | undefined
}

/**
 * An eventually consistent read in flight, shared by the loads of its key.
 * It completes with `None` when the load that sent it was interrupted, and
 * the loads waiting for it read again.
 */
export type PendingRead = Deferred.Deferred<SharedRead, DynamoDBError>

/** The item a shared read found, or `None` when its load was interrupted. */
export type SharedRead = Option.Option<RawItem | undefined>

/**
 * How a read relates to the others of its key: a consistent read fences off
 * every read that started before it, and an eventually consistent one is
 * shared with the loads that need the key while it is in flight.
 */
export type ReadKind =
  | { readonly _tag: "Consistent" }
  | { readonly _tag: "Shared"; readonly pending: PendingRead }

/** What a successful write did to one of its keys. */
export type Change =
  | { readonly _tag: "Stored"; readonly key: Key; readonly item: RawItem }
  | { readonly _tag: "Deleted"; readonly key: Key }
  /** Changed in a way the write's response does not show. */
  | { readonly _tag: "Changed"; readonly key: Key }

/** A write in flight, from `beginWrite` to `endWrite`. */
export interface WriteTicket {
  readonly namespace: object
  readonly keys: ReadonlyArray<Key>
}

interface KeyState {
  entry: Entry | undefined
  pending: PendingRead | undefined
  /** When the key was last written, evicted or read consistently. */
  changedAt: number
  /** Writes of the key in flight. */
  writes: number
  /** Whether writes of the key overlapped since it last had none in flight. */
  overlapped: boolean
}

/**
 * Items by table and key. A namespace is the `DynamoDB` service the items
 * were read through, so tables of the same name on different endpoints or
 * accounts never share entries.
 *
 * Every value goes in and comes out as a copy, so neither the caller nor a
 * decoded instance can change what the cache holds.
 *
 * The ordering rules:
 *
 * - A write is recorded when it succeeds. When writes of a key overlap, their
 *   responses don't tell which one DynamoDB applied last, so the key is
 *   forgotten instead. A failed write forgets every key it touched.
 * - A read is recorded only if no write, eviction, consistent read or
 *   `clear` of its key started after it, so a slow read never replaces newer
 *   knowledge, and an eventually consistent read never replaces an entry.
 */
export class ItemCache {
  readonly #namespaces = new Map<object, Map<string, KeyState>>()
  #clock = 0
  #clearedAt = 0

  /** What the request knows about `key`, if anything. */
  lookup(namespace: object, key: Key): Entry | undefined {
    const entry = this.#find(namespace, key)?.entry
    return entry && { item: copy(entry.item) }
  }

  /** The eventually consistent read of `key` in flight, if any. */
  pending(namespace: object, key: Key): PendingRead | undefined {
    return this.#find(namespace, key)?.pending
  }

  /** Starts a read and returns the token to pass to `endRead`. */
  beginRead(namespace: object, key: Key, kind: ReadKind): number {
    const state = this.#state(namespace, key)
    if (kind._tag === "Consistent") state.changedAt = ++this.#clock
    else state.pending = kind.pending
    return this.#clock
  }

  /**
   * Ends a read that started at `token`, recording what it found, or
   * nothing when it failed (`found` is `undefined`).
   */
  endRead(
    namespace: object,
    key: Key,
    token: number,
    kind: ReadKind,
    found: Entry | undefined,
  ): void {
    const state = this.#state(namespace, key)
    if (kind._tag === "Shared" && state.pending === kind.pending) {
      state.pending = undefined
    }
    if (!found || this.#clearedAt > token || state.changedAt > token) return
    if (kind._tag === "Shared" && state.entry) return
    state.entry = { item: copy(found.item) }
  }

  beginWrite(namespace: object, keys: ReadonlyArray<Key>): WriteTicket {
    for (const key of keys) {
      const state = this.#state(namespace, key)
      if (state.writes > 0) state.overlapped = true
      state.writes++
    }
    return { namespace, keys }
  }

  /**
   * Ends a write with what it did, or `undefined` when it failed or was
   * interrupted. Keys without a change, such as condition checks, keep their
   * entry.
   */
  endWrite(
    ticket: WriteTicket,
    changes: ReadonlyArray<Change> | undefined,
  ): void {
    const byId = new Map(changes?.map((change) => [id(change.key), change]))
    for (const key of ticket.keys) {
      const state = this.#state(ticket.namespace, key)
      const change = byId.get(id(key))
      if (changes === undefined || state.overlapped) {
        this.#change(state, undefined)
      } else if (change !== undefined) {
        this.#change(
          state,
          change._tag === "Stored"
            ? { item: change.item }
            : change._tag === "Deleted"
              ? { item: undefined }
              : undefined,
        )
      }
      if (--state.writes === 0) state.overlapped = false
    }
  }

  /** Forgets `key`; the next load reads it from the table. */
  evict(namespace: object, key: Key): void {
    const state = this.#find(namespace, key)
    if (state) this.#change(state, undefined)
  }

  /** Forgets every key, including reads still in flight. */
  clear(): void {
    this.#clearedAt = ++this.#clock
    for (const states of this.#namespaces.values()) {
      for (const state of states.values()) {
        state.entry = undefined
        state.pending = undefined
      }
    }
  }

  #change(state: KeyState, entry: Entry | undefined) {
    state.changedAt = ++this.#clock
    state.entry = entry
    state.pending = undefined
  }

  #find(namespace: object, key: Key): KeyState | undefined {
    return this.#namespaces.get(namespace)?.get(id(key))
  }

  #state(namespace: object, key: Key): KeyState {
    let states = this.#namespaces.get(namespace)
    if (!states) {
      states = new Map()
      this.#namespaces.set(namespace, states)
    }
    let state = states.get(id(key))
    if (!state) {
      state = {
        entry: undefined,
        pending: undefined,
        changedAt: 0,
        writes: 0,
        overlapped: false,
      }
      states.set(id(key), state)
    }
    return state
  }
}

/** A string that identifies a key; unambiguous whatever PK and SK contain. */
const id = (key: Key) => JSON.stringify([key.PK, key.SK])

/** A copy that shares nothing with `item`, binary values included. */
export const copy = <A extends RawItem | undefined>(item: A): A =>
  structuredClone(item)

/**
 * `item` as reads return it once the client has written it: marshalled with
 * the client's options, which drop `undefined`, turn a `Map` into a map and a
 * `bigint` into a number, and unmarshalled again. `undefined` for an item the
 * client cannot marshall, whose write fails anyway.
 */
export const stored = (item: RawItem): RawItem | undefined => {
  try {
    return copy(unmarshall(marshall(item, marshallOptions())))
  } catch {
    return undefined
  }
}

/** The PK and SK of an item or key that DynamoDB returned or was sent. */
export const keyOfRecord = (
  record: Readonly<Record<string, unknown>> | undefined,
): Key | undefined =>
  typeof record?.PK === "string" && typeof record.SK === "string"
    ? { PK: record.PK, SK: record.SK }
    : undefined
