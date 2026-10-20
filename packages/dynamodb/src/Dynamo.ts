/**
 * Table operations as Effects. The `dynamodb` provider exposes these as
 * model methods (`User.load(key)`, `user.update({...})`); this module is the
 * same API as functions, for code that works with several models at once.
 *
 * Every operation requires the `DynamoDB` service, fails with tagged errors
 * from `Errors`, and runs in a `Dynamo.<name>` span around the
 * `DynamoDB.<Operation>` client spans it sends.
 *
 * @since 7.0.0
 */
import {
  DeleteCommand,
  GetCommand,
  PutCommand,
  QueryCommand,
  type QueryCommandInput,
  TransactWriteCommand,
  type TransactWriteCommandInput,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb"
import { Cause, Clock, Effect, Option, Schedule, Stream } from "effect"
import { DynamoDB } from "./DynamoDB.ts"
import {
  BulkWriteRollbackError,
  ConditionalCheckFailedError,
  DynamoDBError,
  type ItemDecodeError,
  ItemEncodeError,
  ItemNotFoundError,
  KeyExistsError,
  type PaginationError,
  RaceConditionError,
  TransactionCanceledError,
} from "./Errors.ts"
import {
  applySoftDeletionFields,
  type Decodable,
  decodeItem,
  deletedKey,
  encodeItem,
  encoderOf,
  markRecovered,
  modelOf,
  nameOf,
  type RawItem,
  type Recovered,
  setDocVersion,
  type TableModel,
  tagsOf,
} from "./Item.ts"
import {
  formatKey,
  type GSI,
  type ItemKeys,
  type Key,
  keyOf,
  keysOf,
} from "./Keys.ts"
import * as Operation from "./Operation.ts"
import {
  decodeCursor,
  decodePagination,
  encodeCursor,
  type PaginationInput,
  type PaginationResult,
} from "./Pagination.ts"

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

/** @since 7.0.0 */
export type GetError = ItemNotFoundError | ItemDecodeError | DynamoDBError
/** @since 7.0.0 */
export type QueryError = DynamoDBError
/** @since 7.0.0 */
export type PaginateError = PaginationError | DynamoDBError
/** @since 7.0.0 */
export type PutError =
  KeyExistsError | ConditionalCheckFailedError | ItemEncodeError | DynamoDBError
/** @since 7.0.0 */
export type BulkError =
  | TransactionCanceledError
  | BulkWriteRollbackError
  | ItemEncodeError
  | DynamoDBError
/** @since 7.0.0 */
export type UpdateError = RaceConditionError | PutError | BulkError
/** @since 7.0.0 */
export type UpdateRawError =
  | ItemNotFoundError
  | ConditionalCheckFailedError
  | ItemDecodeError
  | ItemEncodeError
  | DynamoDBError
/** @since 7.0.0 */
export type DeleteError = ConditionalCheckFailedError | DynamoDBError
/** @since 7.0.0 */
export type DeleteItemError = RaceConditionError | DynamoDBError
/** @since 7.0.0 */
export type SoftDeleteError = RaceConditionError | BulkError

// -----------------------------------------------------------------------------
// Reads
// -----------------------------------------------------------------------------

/** @since 7.0.0 */
export interface ReadOptions {
  /** Read with strong consistency. */
  readonly consistentRead?: boolean | undefined
}

/** @since 7.0.0 */
export interface LoadOptions extends ReadOptions {
  /**
   * When the key is missing, read the soft-deleted copy instead. A recovered
   * item has `isDeleted: true`.
   */
  readonly recover?: boolean | undefined
}

/**
 * One GetItem. Prefer `load`, which batches concurrent reads.
 *
 * @since 7.0.0
 */
export const get: <A extends Key>(
  model: Decodable<A>,
  key: Key,
  options?: ReadOptions,
) => Effect.Effect<A, GetError, DynamoDB> = Effect.fn("Dynamo.get")(function* (
  model: Decodable,
  key: Key,
  options?: ReadOptions,
) {
  const db = yield* DynamoDB
  yield* Effect.annotateCurrentSpan(spanAttributes(model, key))
  const { Item } = yield* db.send(
    "GetItem",
    new GetCommand({
      TableName: db.tableName,
      Key: keyOf(key),
      ConsistentRead: options?.consistentRead,
    }),
  )
  if (!Item) return yield* new ItemNotFoundError({ key: keyOf(key) })
  return yield* decodeItem(model, Item)
})

/** @since 7.0.0 */
export interface Load {
  <A extends Key>(
    model: Decodable<A>,
    key: Key,
    options: LoadOptions & { readonly recover: true },
  ): Effect.Effect<A & Recovered, GetError, DynamoDB>
  <A extends Key>(
    model: Decodable<A>,
    key: Key,
    options?: LoadOptions,
  ): Effect.Effect<A, GetError, DynamoDB>
}

/**
 * Reads one item. Loads that run concurrently, e.g. inside
 * `Effect.forEach(..., { concurrency: "unbounded" })` or `Effect.all`, share
 * BatchGetItem calls of up to 100 keys.
 *
 * @since 7.0.0
 */
export const load: Load = Effect.fn("Dynamo.load")(function* (
  model: Decodable,
  key: Key,
  options?: LoadOptions,
) {
  const db = yield* DynamoDB
  yield* Effect.annotateCurrentSpan(spanAttributes(model, key))
  const consistentRead = options?.consistentRead ?? false
  const raw = yield* db.getBatched(keyOf(key), consistentRead)
  if (raw) return yield* decodeItem(model, raw)
  if (options?.recover) {
    const deleted = yield* db.getBatched(deletedKey(key), consistentRead)
    if (deleted) return markRecovered(yield* decodeItem(model, deleted))
  }
  return yield* new ItemNotFoundError({ key: keyOf(key) })
})

/**
 * `load`, with `Option.none()` for a missing key.
 *
 * @since 7.0.0
 */
export const loadOption: {
  <A extends Key>(
    model: Decodable<A>,
    key: Key,
    options: LoadOptions & { readonly recover: true },
  ): Effect.Effect<
    Option.Option<A & Recovered>,
    ItemDecodeError | DynamoDBError,
    DynamoDB
  >
  <A extends Key>(
    model: Decodable<A>,
    key: Key,
    options?: LoadOptions,
  ): Effect.Effect<Option.Option<A>, ItemDecodeError | DynamoDBError, DynamoDB>
} = (model: Decodable, key: Key, options?: LoadOptions) =>
  load(model, key, options).pipe(
    Effect.map(Option.some),
    Effect.catchTag("ItemNotFoundError", () => Effect.succeedNone),
  )

/**
 * `load` for several keys, batched. Fails with the first error; wrap the
 * loads in `Effect.option` or `Effect.exit` yourself to keep partial results.
 *
 * @since 7.0.0
 */
export const loadMany = <A extends Key>(
  model: Decodable<A>,
  keys: Iterable<Key>,
  options?: ReadOptions,
): Effect.Effect<Array<A>, GetError, DynamoDB> =>
  Effect.forEach(keys, (key) => load(model, key, options), {
    concurrency: "unbounded",
  })

/**
 * Query parameters. `TableName` comes from the `DynamoDB` service.
 *
 * @since 7.0.0
 */
export type QueryParams = Omit<
  QueryCommandInput,
  "TableName" | "KeyConditionExpression"
> &
  Required<Pick<QueryCommandInput, "KeyConditionExpression">> & {
    /** Follow `LastEvaluatedKey` until every page is read. */
    readonly FetchAllPages?: boolean | undefined
  }

/** @since 7.0.0 */
export interface QueryMeta {
  readonly lastEvaluatedKey?: Record<string, unknown> | undefined
}

/**
 * `query(params, models)` groups the items by the name each decodable was
 * given. Items that none of them decode end up in `_unknown`.
 *
 * @since 7.0.0
 */
export type QueryResult<R extends Record<string, Decodable>> = {
  readonly [K in keyof R]: Array<R[K]["Type"]>
} & {
  readonly _unknown: Array<RawItem>
  readonly meta: QueryMeta
}

/**
 * Runs a Query and decodes each item with the model its `_tag` names. Items
 * without a matching tag are tried against every model in order.
 *
 * ```ts
 * const { posts, comments } = yield* Dynamo.query(
 *   { KeyConditionExpression: "PK = :pk", ExpressionAttributeValues: { ":pk": key } },
 *   { posts: Post, comments: Comment },
 * )
 * ```
 *
 * @since 7.0.0
 */
export const query: <const R extends Record<string, Decodable>>(
  params: QueryParams,
  models: R,
) => Effect.Effect<QueryResult<R>, QueryError, DynamoDB> = Effect.fn(
  "Dynamo.query",
)(function* (params: QueryParams, models: Record<string, Decodable>) {
  const db = yield* DynamoDB
  const { FetchAllPages, ...input } = params
  yield* Effect.annotateCurrentSpan({
    "model_ts.models": Object.values(models).map(nameOf),
    ...(input.IndexName && { "aws.dynamodb.index_name": input.IndexName }),
  })

  const items: Array<RawItem> = []
  let lastEvaluatedKey: Record<string, unknown> | undefined
  do {
    const page = yield* db.send(
      "Query",
      new QueryCommand({
        TableName: db.tableName,
        // Rows written by the predecessor library carry this flag.
        FilterExpression: "attribute_not_exists(dynamotorLegacy)",
        ...input,
        ExclusiveStartKey: lastEvaluatedKey ?? input.ExclusiveStartKey,
      }),
      {
        "aws.dynamodb.index_name": input.IndexName,
        "aws.dynamodb.limit": input.Limit,
        "aws.dynamodb.scan_forward": input.ScanIndexForward,
      },
    )
    items.push(...((page.Items ?? []) as Array<RawItem>))
    lastEvaluatedKey = page.LastEvaluatedKey
  } while (FetchAllPages && lastEvaluatedKey)

  const entries = Object.entries(models)
  const byTag = new Map<string, Array<[string, Decodable]>>()
  const grouped: Record<string, Array<unknown>> = {}
  for (const entry of entries) {
    grouped[entry[0]] = []
    for (const tag of tagsOf(entry[1])) {
      byTag.set(tag, [...(byTag.get(tag) ?? []), entry])
    }
  }
  const unknown: Array<RawItem> = []
  for (const item of items) {
    const candidates = byTag.get(String(item._tag)) ?? entries
    let matched = false
    for (const [name, decodable] of candidates) {
      const decoded = yield* Effect.option(decodeItem(decodable, item))
      if (Option.isSome(decoded)) {
        grouped[name]!.push(decoded.value)
        matched = true
        break
      }
    }
    if (!matched) unknown.push(item)
  }

  yield* Effect.annotateCurrentSpan({
    "aws.dynamodb.count": items.length,
    "model_ts.unknown_count": unknown.length,
  })
  return { ...grouped, _unknown: unknown, meta: { lastEvaluatedKey } } as never
})

/**
 * The items of one decodable, as returned by `Model.query(params)`. The
 * array carries the query's `meta`.
 *
 * @since 7.0.0
 */
export type QueryItems<A> = Array<A> & { readonly meta: QueryMeta }

/** @internal `query` for a single decodable. */
export const queryItems = <A extends Key>(
  model: Decodable<A>,
  params: QueryParams,
): Effect.Effect<QueryItems<A>, QueryError, DynamoDB> =>
  Effect.map(query(params, { items: model }), ({ items, meta }) =>
    Object.assign(items, { meta }),
  )

/** @since 7.0.0 */
export type StreamParams = Omit<QueryParams, "FetchAllPages">

/**
 * Every item a query matches, read page by page as the stream is consumed.
 * `Limit` is the page size.
 *
 * ```ts
 * yield* Post.stream({ KeyConditionExpression: "PK = :pk", ... }).pipe(
 *   Stream.grouped(25),
 *   Stream.runForEach((posts) => archive(posts)),
 * )
 * ```
 *
 * @since 7.0.0
 */
export const stream = <A extends Key>(
  model: Decodable<A>,
  params: StreamParams,
): Stream.Stream<A, QueryError, DynamoDB> =>
  Stream.paginate(params.ExclusiveStartKey, (exclusiveStartKey) =>
    Effect.map(
      queryItems(model, { ...params, ExclusiveStartKey: exclusiveStartKey }),
      (items) =>
        [items, Option.fromNullishOr(items.meta.lastEvaluatedKey)] as const,
    ),
  )

/**
 * Query parameters for `paginate`. Page size, start key and direction come
 * from the pagination input.
 *
 * @since 7.0.0
 */
export type PaginationParams = Omit<
  QueryParams,
  "FetchAllPages" | "Limit" | "ExclusiveStartKey" | "ScanIndexForward"
>

/**
 * One page of a Relay-style connection. Cursors work across requests and
 * are encrypted when the table has a `cursorEncryptionKey`.
 *
 * @since 7.0.0
 */
export const paginate: <A extends Key>(
  model: Decodable<A>,
  input: PaginationInput,
  params: PaginationParams,
) => Effect.Effect<PaginationResult<A>, PaginateError, DynamoDB> = Effect.fn(
  "Dynamo.paginate",
)(function* <A extends Key>(
  model: Decodable<A>,
  input: PaginationInput,
  params: PaginationParams,
) {
  const db = yield* DynamoDB
  const page = yield* decodePagination(input, db.pagination)
  // GSI1 inverts the primary key, so its cursors are plain PK/SK.
  const index =
    params.IndexName && params.IndexName !== "GSI1"
      ? (params.IndexName as GSI)
      : undefined
  const exclusiveStartKey = page.cursor
    ? yield* decodeCursor(page.cursor, index, db.cursorEncryptionKey)
    : undefined

  // One extra item tells whether there is another page. A filter or items of
  // other models can leave a Query short, so keep reading until the page is
  // full or the query is exhausted.
  const wanted = page.limit + 1
  const items: Array<A> = []
  let startKey: Record<string, unknown> | undefined = exclusiveStartKey
  do {
    const batch = yield* queryItems(model, {
      ...params,
      Limit: wanted - items.length,
      ExclusiveStartKey: startKey,
      ScanIndexForward: page.direction === "forward",
    })
    items.push(...batch)
    startKey = batch.meta.lastEvaluatedKey
  } while (items.length < wanted && startKey !== undefined)
  const hasMore = items.length === wanted
  const slice = items.slice(0, page.limit)
  if (page.direction === "backward") slice.reverse()
  const edges = slice.map((node) => ({
    node,
    cursor: encodeCursor(node as ItemKeys, db.cursorEncryptionKey),
  }))

  return {
    pageInfo: {
      hasPreviousPage: page.direction === "backward" && hasMore,
      hasNextPage: page.direction === "forward" && hasMore,
      startCursor: edges[0]?.cursor ?? null,
      endCursor: edges[edges.length - 1]?.cursor ?? null,
    },
    edges,
  }
})

/**
 * The cursor `paginate` would give `item`.
 *
 * @since 7.0.0
 */
export const cursor = (
  item: ItemKeys,
): Effect.Effect<string, never, DynamoDB> =>
  DynamoDB.use((db) =>
    Effect.succeed(encodeCursor(item, db.cursorEncryptionKey)),
  )

// -----------------------------------------------------------------------------
// Writes
// -----------------------------------------------------------------------------

/**
 * Stores `item`. Fails with `KeyExistsError` when the key exists, unless
 * `IgnoreExistence` is set or `options` bring their own condition (which
 * fails with `ConditionalCheckFailedError`).
 *
 * @since 7.0.0
 */
export const put: <A extends Key>(
  item: A,
  options?: Operation.PutOptions,
) => Effect.Effect<A, PutError, DynamoDB> = Effect.fn("Dynamo.put")(function* <
  A extends Key,
>(item: A, options?: Operation.PutOptions) {
  const db = yield* DynamoDB
  yield* Effect.annotateCurrentSpan(spanAttributes(modelOf(item), item))
  const { IgnoreExistence, ...params } = options ?? {}
  const encoded = yield* encodeItem(item)
  yield* db
    .send(
      "PutItem",
      new PutCommand({
        TableName: db.tableName,
        Item: encoded,
        ConditionExpression: IgnoreExistence
          ? undefined
          : "attribute_not_exists(PK)",
        ...params,
      }),
    )
    .pipe(
      catchConditionalCheck(() =>
        "ConditionExpression" in params
          ? new ConditionalCheckFailedError({ key: keyOf(item) })
          : new KeyExistsError({ key: keyOf(item) }),
      ),
    )
  setDocVersion(item, Number(encoded._docVersion))
  return item
})

/**
 * Applies `patch` and stores the result, if the stored item still has the
 * version `item` was read with (`RaceConditionError` otherwise). When the
 * patch changes the key, the item is moved in one transaction.
 *
 * `item` is left as it was; use the returned instance.
 *
 * @since 7.0.0
 */
export const update: <A extends Key>(
  item: A,
  patch: Partial<Omit<A, "_tag">>,
) => Effect.Effect<A, UpdateError, DynamoDB> = Effect.fn("Dynamo.update")(
  function* <A extends Key>(item: A, patch: Partial<Omit<A, "_tag">>) {
    yield* Effect.annotateCurrentSpan(spanAttributes(modelOf(item), item))
    const [next, operation] = Operation.update(item, patch)
    if (isPair(operation)) {
      yield* bulk([operation])
      return next
    }
    const { _tag, item: _, ...options } = operation
    return yield* put(next, options).pipe(
      Effect.catchTag("ConditionalCheckFailedError", () =>
        Effect.fail(new RaceConditionError({ key: keyOf(item) })),
      ),
    )
  },
)

/**
 * Builds an UpdateExpression from `attributes`, encoding each model field
 * with its schema, and returns the updated item. `null` GSI attributes are
 * removed. Fails with `ItemNotFoundError` when there is no item at `key`, or
 * `ConditionalCheckFailedError` when `params` bring a condition that fails.
 *
 * @since 7.0.0
 */
export const updateRaw: <A extends Key>(
  model: TableModel<A>,
  key: Key,
  attributes: Operation.UpdateRawAttributes<Omit<A, "_tag">>,
  params?: Operation.UpdateRawParams,
) => Effect.Effect<A, UpdateRawError, DynamoDB> = Effect.fn("Dynamo.updateRaw")(
  function* (
    model: TableModel,
    key: Key,
    attributes: Record<string, unknown>,
    params?: Operation.UpdateRawParams,
  ) {
    const db = yield* DynamoDB
    yield* Effect.annotateCurrentSpan(spanAttributes(model, key))
    const update = yield* toUpdate(
      Operation.updateRaw(model, key, attributes, params),
    )
    const { Attributes } = yield* db
      .send(
        "UpdateItem",
        new UpdateCommand({
          TableName: db.tableName,
          ...update,
          ReturnValues: "ALL_NEW",
        }),
      )
      .pipe(
        catchConditionalCheck(() =>
          params?.ConditionExpression
            ? new ConditionalCheckFailedError({ key: keyOf(key) })
            : new ItemNotFoundError({ key: keyOf(key) }),
        ),
      )
    return yield* decodeItem(model, Attributes as RawItem)
  },
)

const remove: (
  key: Key,
  params?: Operation.ConditionParams,
) => Effect.Effect<void, DeleteError, DynamoDB> = Effect.fn("Dynamo.delete")(
  function* (key: Key, params?: Operation.ConditionParams) {
    const db = yield* DynamoDB
    yield* Effect.annotateCurrentSpan({ "model_ts.key": formatKey(key) })
    yield* db
      .send(
        "DeleteItem",
        new DeleteCommand({
          TableName: db.tableName,
          Key: keyOf(key),
          ...params,
        }),
      )
      .pipe(
        catchConditionalCheck(
          () => new ConditionalCheckFailedError({ key: keyOf(key) }),
        ),
      )
  },
)

export {
  /**
   * Deletes the item at `key`, unconditionally unless `params` add a
   * condition (`ConditionalCheckFailedError` when it fails).
   *
   * @since 7.0.0
   */
  remove as delete,
}

/**
 * Deletes `item` if the stored item still has the version `item` was read
 * with (`RaceConditionError` otherwise, including when it is gone).
 *
 * @since 7.0.0
 */
export const deleteItem: (
  item: Key,
  options?: Operation.DeleteOptions,
) => Effect.Effect<void, DeleteItemError, DynamoDB> = Effect.fn(
  "Dynamo.deleteItem",
)(function* (item: Key, options?: Operation.DeleteOptions) {
  yield* Effect.annotateCurrentSpan(spanAttributes(modelOf(item), item))
  const { key, ...params } = Operation.deleteItem(item, options)
  yield* remove(key, params).pipe(
    Effect.catchTag("ConditionalCheckFailedError", () =>
      Effect.fail(new RaceConditionError({ key: keyOf(item) })),
    ),
  )
})

/**
 * Moves `item` to its `$$DELETED$$` keys in one transaction, guarded by its
 * version like `deleteItem`. `load(key, { recover: true })` reads it back.
 *
 * @since 7.0.0
 */
export const softDelete: <A extends Key>(
  item: A,
  options?: Operation.DeleteOptions,
) => Effect.Effect<A, SoftDeleteError, DynamoDB> = Effect.fn(
  "Dynamo.softDelete",
)(function* <A extends Key>(item: A, options?: Operation.DeleteOptions) {
  yield* Effect.annotateCurrentSpan(spanAttributes(modelOf(item), item))
  const pair = Operation.softDelete(item, options)
  const guarded = pair[0].action.ConditionExpression !== undefined
  yield* bulk([pair]).pipe(
    Effect.catchTag(
      "TransactionCanceledError",
      (
        error,
      ): Effect.Effect<
        never,
        RaceConditionError | TransactionCanceledError
      > => {
        const [live, archived] = error.cancellationReasons
        // Only the live-item guard failed: someone changed or removed it.
        return guarded &&
          error.cancellationReasons.length === 2 &&
          live?.Code === "ConditionalCheckFailed" &&
          archived?.Code === "None"
          ? Effect.fail(new RaceConditionError({ key: keyOf(item) }))
          : Effect.fail(error)
      },
    ),
  )
  return item
})

// -----------------------------------------------------------------------------
// Transactions
// -----------------------------------------------------------------------------

type TransactItem = NonNullable<
  TransactWriteCommandInput["TransactItems"]
>[number]

const MAX_TRANSACTION_ITEMS = 100

/**
 * Runs operations in transactions of up to 100 items. A nested array stays
 * in one transaction. When a transaction fails, the ones already committed
 * are rolled back (last to first) and the original error is returned; if a
 * rollback fails too, the error is `BulkWriteRollbackError`.
 *
 * A defect rolls back the same way. Interruption waits until the bulk write
 * has finished or been rolled back, so it never leaves the table half written.
 *
 * A group of more than 100 operations is a defect: it can never succeed.
 *
 * @since 7.0.0
 */
export const bulk: (
  operations: Operation.BulkInput,
) => Effect.Effect<void, BulkError, DynamoDB> = Effect.fn("Dynamo.bulk")(
  function* (operations: Operation.BulkInput) {
    const batches = yield* toBatches(operations)
    yield* Effect.annotateCurrentSpan({
      "model_ts.operations": batches.reduce((n, batch) => n + batch.length, 0),
      "model_ts.transactions": batches.length,
    })
    yield* Effect.uninterruptible(writeAll(batches))
  },
)

const writeAll = Effect.fnUntraced(function* (
  batches: ReadonlyArray<ReadonlyArray<Operation.BulkOperation>>,
) {
  for (const [index, batch] of batches.entries()) {
    yield* writeBatch(batch, "action").pipe(
      Effect.catchCause((cause) =>
        rollback(batches.slice(0, index), Cause.squash(cause)).pipe(
          Effect.andThen(Effect.failCause(cause)),
        ),
      ),
    )
  }
})

const toBatches = (operations: Operation.BulkInput) =>
  Effect.suspend(() => {
    const batches: Array<Array<Operation.BulkOperation>> = []
    let batch: Array<Operation.BulkOperation> = []
    for (const entry of operations) {
      const group = isGroup(entry) ? entry : [entry]
      if (group.length > MAX_TRANSACTION_ITEMS) {
        return Effect.die(
          new RangeError(
            `A bulk operation group cannot exceed ${MAX_TRANSACTION_ITEMS} actions.`,
          ),
        )
      }
      if (batch.length + group.length > MAX_TRANSACTION_ITEMS) {
        batches.push(batch)
        batch = []
      }
      batch.push(...group)
    }
    if (batch.length > 0) batches.push(batch)
    return Effect.succeed(batches)
  })

/**
 * Undoes committed batches, last to first, so that a later batch that built
 * on an earlier one is undone before it.
 */
const rollback = Effect.fnUntraced(function* (
  committed: ReadonlyArray<ReadonlyArray<Operation.BulkOperation>>,
  cause: unknown,
) {
  for (let index = committed.length - 1; index >= 0; index--) {
    yield* writeBatch(committed[index]!, "rollback").pipe(
      Effect.mapError(
        (rollbackCause) =>
          new BulkWriteRollbackError({
            requiresRollback: committed.slice(0, index + 1).flat(),
            cause,
            rollbackCause,
          }),
      ),
    )
  }
})

const writeBatch = (
  batch: ReadonlyArray<Operation.BulkOperation>,
  phase: "action" | "rollback",
) =>
  Effect.gen(function* () {
    const items: Array<TransactItem> = []
    for (const operation of batch) {
      const write = Operation.isTransactionOperation(operation)
        ? operation[phase]
        : phase === "action"
          ? operation
          : undefined
      if (write) items.push(yield* toTransactItem(write))
    }
    if (items.length > 0) yield* transactWrite(items)
  })

/**
 * TransactWriteItems. Server faults and network errors are retried up to
 * three times, 50ms apart; a cancelled transaction or a client error
 * (validation, permissions) fails right away. Every attempt sends the same
 * `ClientRequestToken`, so a retry of a transaction that committed without
 * a response is not applied twice.
 *
 * @since 7.0.0
 */
export const transactWrite: (
  items: ReadonlyArray<TransactItem>,
) => Effect.Effect<void, TransactionCanceledError | DynamoDBError, DynamoDB> =
  Effect.fn("Dynamo.transactWrite")(function* (
    items: ReadonlyArray<TransactItem>,
  ) {
    const db = yield* DynamoDB
    const ClientRequestToken = crypto.randomUUID()
    yield* db
      .send(
        "TransactWriteItems",
        new TransactWriteCommand({
          TransactItems: [...items],
          ClientRequestToken,
        }),
        { "aws.dynamodb.transact_items_count": items.length },
      )
      .pipe(
        Effect.mapError((error) =>
          error.code === "TransactionCanceledException"
            ? new TransactionCanceledError({
                cancellationReasons: cancellationReasons(error.cause),
                cause: error.cause,
              })
            : error,
        ),
        Effect.retry({
          schedule: Schedule.spaced("50 millis"),
          times: 3,
          while: isTransient,
        }),
      )
  })

const toTransactItem = (
  operation: Operation.WriteOperation,
): Effect.Effect<TransactItem, ItemEncodeError, DynamoDB> =>
  Effect.gen(function* () {
    const db = yield* DynamoDB
    switch (operation._tag) {
      case "Put": {
        const { _tag, item, softDeleted, IgnoreExistence, ...params } =
          operation
        const encoded = yield* encodeItem(item)
        return {
          Put: {
            TableName: db.tableName,
            Item: softDeleted
              ? applySoftDeletionFields(
                  encoded,
                  new Date(yield* Clock.currentTimeMillis).toISOString(),
                )
              : encoded,
            ConditionExpression: IgnoreExistence
              ? undefined
              : "attribute_not_exists(PK)",
            ...params,
          },
        }
      }
      case "UpdateRaw":
        return {
          Update: { TableName: db.tableName, ...(yield* toUpdate(operation)) },
        }
      case "Delete": {
        const { _tag, key, ...params } = operation
        return { Delete: { Key: key, TableName: db.tableName, ...params } }
      }
      case "ConditionCheck": {
        const { _tag, key, ...params } = operation
        return {
          ConditionCheck: { TableName: db.tableName, Key: key, ...params },
        }
      }
    }
  })

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

/** The Key, expressions and condition of an UpdateItem, shared by updateRaw and bulk. */
const toUpdate = Effect.fnUntraced(function* (
  operation: Operation.UpdateRawOperation,
) {
  const { _tag, model, key, attributes, ...params } = operation
  const encoded: Array<readonly [string, unknown]> = []
  for (const [name, value] of Object.entries(attributes)) {
    const field = model.fields[name]
    encoded.push([
      name,
      field
        ? yield* encoderOf(field as never)(value).pipe(
            Effect.mapError(
              (cause) =>
                new ItemEncodeError({ model: model.identifier, cause }),
            ),
          )
        : value,
    ])
  }
  const expression = buildUpdateExpression(encoded, params)
  return {
    Key: key,
    UpdateExpression: expression.UpdateExpression,
    ConditionExpression:
      "attribute_exists(PK) and attribute_not_exists(dynamotorLegacy)",
    ...params,
    ExpressionAttributeNames: nonEmpty(expression.ExpressionAttributeNames),
    ExpressionAttributeValues: nonEmpty(expression.ExpressionAttributeValues),
  }
})

/**
 * `SET` for every attribute and `REMOVE` for GSI attributes set to `null`
 * (an index key cannot be null). Placeholder names are the attribute names
 * without non-alphanumerics, suffixed on collision. The text matches what
 * earlier releases wrote, double spaces included, so stored expressions and
 * request logs stay comparable.
 */
const buildUpdateExpression = (
  attributes: ReadonlyArray<readonly [string, unknown]>,
  params: Operation.ConditionParams,
) => {
  const isRemoval = ([name, value]: readonly [string, unknown]) =>
    name.startsWith("GSI") && value === null
  const sets = attributes.filter((entry) => !isRemoval(entry))
  const removals = attributes.filter(isRemoval)

  const used = new Set<string>()
  const placeholder = (name: string) => {
    const alphanumeric = name.replace(/[^a-zA-Z\d]/g, "")
    const unique = used.has(alphanumeric)
      ? `${alphanumeric}x${used.size}`
      : alphanumeric
    used.add(unique)
    return unique
  }

  const names: Record<string, string> = { ...params.ExpressionAttributeNames }
  const values: Record<string, unknown> = {
    ...params.ExpressionAttributeValues,
  }
  const set = sets.map(([name, value]) => {
    const id = placeholder(name)
    names[`#${id}`] = name
    values[`:${id}`] = value
    return `#${id} = :${id}`
  })
  const remove = removals.map(([name]) => {
    const id = name.replace(/[^a-zA-Z\d]/g, "")
    names[`#${id}`] = name
    return `#${id}`
  })

  return {
    UpdateExpression: clause("SET", set) + " " + clause("REMOVE", remove),
    ExpressionAttributeNames: names,
    ExpressionAttributeValues: values,
  }
}

const clause = (keyword: string, parts: ReadonlyArray<string>) =>
  parts.length === 0
    ? ""
    : `${keyword} ` +
      parts.map((part, index) => `${index === 0 ? "" : ","} ${part}`).join("")

const nonEmpty = <T extends object>(value: T): T | undefined =>
  Object.keys(value).length > 0 ? value : undefined

/** A failure that may pass on retry: a server fault or no response at all. */
const isTransient = (error: TransactionCanceledError | DynamoDBError) =>
  error._tag === "DynamoDBError" &&
  (error.cause as { $fault?: string } | undefined)?.$fault !== "client"

const catchConditionalCheck =
  <E2>(onFailure: () => E2) =>
  <A, R>(self: Effect.Effect<A, DynamoDBError, R>) =>
    self.pipe(
      Effect.catchIf(
        (error) => error.code === "ConditionalCheckFailedException",
        () => Effect.fail(onFailure()),
      ),
    )

const cancellationReasons = (cause: unknown) => {
  const reasons = (cause as { CancellationReasons?: unknown })
    ?.CancellationReasons
  return Array.isArray(reasons)
    ? reasons.map((reason: { Code?: string; Message?: string }) => ({
        ...(reason.Code !== undefined && { Code: reason.Code }),
        ...(reason.Message !== undefined && { Message: reason.Message }),
      }))
    : []
}

const isGroup = (
  entry: Operation.BulkOperation | ReadonlyArray<Operation.BulkOperation>,
): entry is ReadonlyArray<Operation.BulkOperation> => Array.isArray(entry)

const isPair = (
  operation: Operation.UpdateOperation,
): operation is readonly [
  Operation.TransactionOperation,
  Operation.TransactionOperation,
] => Array.isArray(operation)

const spanAttributes = (model: Decodable, key: Key) => ({
  "model_ts.model": nameOf(model),
  "model_ts.key": formatKey(key),
})

export { deletedKey, keysOf }
