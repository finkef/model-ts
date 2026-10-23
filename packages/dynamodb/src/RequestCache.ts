/**
 * A cache of the items one request reads and writes. Within a request,
 * loading a key that was already loaded or written returns the known item
 * without reading the table again.
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
 *   bulk transaction stores what it wrote. A key changed by a transaction's
 *   UpdateItem, whose result DynamoDB does not return, is forgotten instead.
 * - A write that fails, for example with `RaceConditionError`, forgets every
 *   key it touched, so a retry loads fresh data.
 *
 * @since 7.0.0
 */
import { Context, Effect, Option } from "effect"
import type { DynamoDB } from "./DynamoDB.ts"
import { ItemCache } from "./internal/cache.ts"
import { TableService } from "./internal/service.ts"
import type { Key } from "./Keys.ts"

/**
 * @since 7.0.0
 */
export class RequestCache extends Context.Service<RequestCache, ItemCache>()(
  "@model-ts/dynamodb/RequestCache",
) {
  /**
   * An empty cache. `DynamoDB.withRequestCache` provides one to an effect;
   * when one request runs several effects, as GraphQL resolvers that each
   * call `runPromise` do, make one per request and provide it to each with
   * `Effect.provideService(RequestCache, cache)`.
   *
   * @since 7.0.0
   */
  static readonly make: Effect.Effect<ItemCache> = Effect.sync(
    () => new ItemCache(),
  )

  /**
   * Forgets `key` in the current request's cache, after writing it some
   * other way than through model-ts. Does nothing without a cache.
   *
   * @since 7.0.0
   */
  static readonly invalidate = (
    key: Key,
  ): Effect.Effect<void, never, DynamoDB> =>
    Effect.gen(function* () {
      const cache = yield* Effect.serviceOption(RequestCache)
      if (Option.isNone(cache)) return
      const { tableName } = yield* TableService
      cache.value.evict(tableName, key)
    })

  /**
   * Forgets everything in the current request's cache. Does nothing without
   * a cache.
   *
   * @since 7.0.0
   */
  static readonly clear: Effect.Effect<void> = Effect.flatMap(
    Effect.serviceOption(RequestCache),
    (cache) =>
      Effect.sync(() => {
        if (Option.isSome(cache)) cache.value.clear()
      }),
  )
}

export type { ItemCache } from "./internal/cache.ts"
