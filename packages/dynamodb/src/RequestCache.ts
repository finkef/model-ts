/**
 * A cache of the items one request reads and writes. Within a request,
 * loading a key that was already loaded or written returns the known item
 * without reading the table again, and loads of a key that is being read wait
 * for that read.
 *
 * Create one per request, never per container: the cache only knows about
 * writes made through model-ts in the same request.
 *
 * ```ts
 * export const handler = (event: APIGatewayProxyEvent) =>
 *   runtime.runPromise(handle(event).pipe(DynamoDB.withRequestCache))
 * ```
 *
 * - Only `load` and `loadMany` read from the cache. `get`, queries,
 *   streams and pagination always read the table, and `consistentRead: true`
 *   reads the table and refreshes the entry.
 * - A load stores what it read, including that a key holds no item.
 * - A successful `put`, `update`, `updateRaw`, `delete`, `softDelete` or
 *   bulk transaction stores what it wrote, as a read would return it. A key
 *   changed by a transaction's UpdateItem, whose result DynamoDB does not
 *   return, is forgotten instead, and so is a key that two writes changed at
 *   the same time.
 * - A write that fails, for example with `RaceConditionError`, forgets every
 *   key it touched, so a retry loads fresh data.
 *
 * @since 7.0.0
 */
import { Context, Effect } from "effect"
import type { DynamoDB } from "./DynamoDB.ts"
import {
  currentCache,
  KEY,
  makeService,
  type TypeId,
} from "./internal/cache.ts"
import { TableService } from "./internal/service.ts"
import type { Key } from "./Keys.ts"

/**
 * A request's cache. Its contents are only reachable through the operations,
 * so nothing can put an item in it that the table doesn't hold.
 *
 * @since 7.0.0
 */
export interface RequestCacheService {
  readonly [TypeId]: typeof TypeId
}

/**
 * @since 7.0.0
 */
export class RequestCache extends Context.Service<
  RequestCache,
  RequestCacheService
>()(KEY) {
  /**
   * An empty cache. `DynamoDB.withRequestCache` provides one to an effect.
   * When one request runs several effects, as GraphQL resolvers that each
   * call `runPromise` do, make one per request and provide that one to each
   * of them with `Effect.provideService(RequestCache, cache)`.
   *
   * @since 7.0.0
   */
  static readonly make: Effect.Effect<RequestCacheService> =
    Effect.sync(makeService)

  /**
   * Forgets `keys` in the current request's cache after writing them some
   * other way than through model-ts. A soft-deleted item lives at
   * `deletedKey(key)`, so pass that too when the write moved one. Does
   * nothing without a cache.
   *
   * @since 7.0.0
   */
  static readonly invalidate = (
    ...keys: ReadonlyArray<Key>
  ): Effect.Effect<void, never, DynamoDB> =>
    Effect.gen(function* () {
      const cache = yield* currentCache
      if (!cache) return
      const db = yield* TableService
      for (const key of keys) cache.evict(db, key)
    })

  /**
   * Forgets everything in the current request's cache. Does nothing without
   * a cache.
   *
   * @since 7.0.0
   */
  static readonly clear: Effect.Effect<void> = Effect.flatMap(
    currentCache,
    (cache) => Effect.sync(() => cache?.clear()),
  )
}
