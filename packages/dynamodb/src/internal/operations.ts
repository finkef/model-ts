/**
 * The table operations behind the `DynamoDB` statics and `DynamoDB.provider`.
 * They are documented on the statics. They read the service through
 * `TableService`, so this module never imports `DynamoDB.ts` at runtime.
 */
import {
  DeleteCommand,
  GetCommand,
  PutCommand,
  QueryCommand,
  TransactWriteCommand,
  type TransactWriteCommandInput,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb"
import {
  Cause,
  Clock,
  Effect,
  Option,
  Result,
  Schedule,
  Schema,
  Stream,
} from "effect"
import type { DynamoDB } from "../DynamoDB.ts"
import { TableService } from "./service.ts"
import {
  BulkWriteRollbackError,
  ConditionalCheckFailedError,
  DynamoDBError,
  type ItemDecodeError,
  ItemEncodeError,
  ItemNotFoundError,
  KeyExistsError,
  PaginationError,
  RaceConditionError,
  TransactionCanceledError,
} from "../Errors.ts"
import type {
  Decodable,
  RawItem,
  Recovered,
  TableItem,
  TableModel,
} from "../Item.ts"
import {
  applySoftDeletionFields,
  decodeItem,
  deletedKey,
  docVersion,
  encodeItem,
  markRecovered,
  modelOf,
  nameOf,
  setDocVersion,
  storedKeysOf,
  tagsOf,
} from "./item.ts"
import type { ItemKeys, Key } from "../Keys.ts"
import { formatKey, isGSI, keyOf } from "./keys.ts"
import * as Operation from "../Operation.ts"
import type { PaginationInput, PaginationResult } from "../Pagination.ts"
import { decodeCursor, encodeCursor } from "./pagination.ts"
import { decodePagination } from "./pagination.ts"

// -----------------------------------------------------------------------------
// Reads
// -----------------------------------------------------------------------------

export const get: <A extends Key>(
  model: Decodable<A>,
  key: Key,
  options?: DynamoDB.ReadOptions,
) => Effect.Effect<A, DynamoDB.GetError, DynamoDB> = Effect.fn("DynamoDB.get")(
  function* <A extends Key>(
    model: Decodable<A>,
    key: Key,
    options?: DynamoDB.ReadOptions,
  ) {
    const db = yield* TableService
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
  },
)

const loadItem = Effect.fn("DynamoDB.load")(function* <A extends Key>(
  model: Decodable<A>,
  key: Key,
  options?: DynamoDB.LoadOptions,
) {
  const db = yield* TableService
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

export function load<A extends Key>(
  model: Decodable<A>,
  key: Key,
  options: DynamoDB.LoadOptions & { readonly recover: true },
): Effect.Effect<A & Recovered, DynamoDB.GetError, DynamoDB>
export function load<A extends Key>(
  model: Decodable<A>,
  key: Key,
  options?: DynamoDB.LoadOptions,
): Effect.Effect<A, DynamoDB.GetError, DynamoDB>
export function load<A extends Key>(
  model: Decodable<A>,
  key: Key,
  options?: DynamoDB.LoadOptions,
): Effect.Effect<A | (A & Recovered), DynamoDB.GetError, DynamoDB> {
  return loadItem(model, key, options)
}

export const loadMany = <A extends Key>(
  model: Decodable<A>,
  keys: Iterable<Key>,
  options?: DynamoDB.ReadOptions,
): Effect.Effect<Array<A>, DynamoDB.GetError, DynamoDB> =>
  Effect.forEach(keys, (key) => load(model, key, options), {
    concurrency: "unbounded",
  })

export const query: <const R extends DynamoDB.QueryModels>(
  params: DynamoDB.QueryParams,
  models: R,
) => Effect.Effect<DynamoDB.QueryResult<R>, DynamoDB.QueryError, DynamoDB> =
  Effect.fn("DynamoDB.query")(function* <const R extends DynamoDB.QueryModels>(
    params: DynamoDB.QueryParams,
    models: R,
  ) {
    for (const reserved of ["_unknown", "meta"]) {
      if (Object.hasOwn(models, reserved)) {
        return yield* Effect.die(
          new TypeError(`"${reserved}" is taken by the query result`),
        )
      }
    }
    const db = yield* TableService
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
          ...input,
          ExclusiveStartKey: lastEvaluatedKey ?? input.ExclusiveStartKey,
        }),
        {
          "aws.dynamodb.index_name": input.IndexName,
          "aws.dynamodb.limit": input.Limit,
          "aws.dynamodb.scan_forward": input.ScanIndexForward,
        },
      )
      items.push(...(page.Items ?? []))
      lastEvaluatedKey = page.LastEvaluatedKey
    } while (FetchAllPages && lastEvaluatedKey)

    const entries: Array<[string, Decodable]> = Object.entries(models)
    const byTag = new Map<string, Array<[string, Decodable]>>()
    // Without a prototype, a group can have any other name.
    const grouped: Record<string, Array<unknown>> = Object.create(null)
    for (const entry of entries) {
      grouped[entry[0]] = []
      for (const tag of tagsOf(entry[1])) {
        byTag.set(tag, [...(byTag.get(tag) ?? []), entry])
      }
    }
    const unknown: Array<RawItem> = []
    for (const item of items) {
      const tag = item._tag
      const tagged = typeof tag === "string"
      const candidates = tagged ? byTag.get(tag) : entries
      if (!candidates) {
        unknown.push(item)
        continue
      }
      let error: ItemDecodeError | undefined
      let matched = false
      for (const [name, decodable] of candidates) {
        const decoded = yield* Effect.result(decodeItem(decodable, item))
        if (Result.isSuccess(decoded)) {
          grouped[name]!.push(decoded.success)
          matched = true
          break
        }
        error ??= decoded.failure
      }
      if (matched) continue
      if (tagged && error) return yield* error
      unknown.push(item)
    }

    yield* Effect.annotateCurrentSpan({
      "aws.dynamodb.count": items.length,
      "model_ts.unknown_count": unknown.length,
    })
    // The groups are keyed by the names in `models`, which only exist at
    // runtime here; `DynamoDB.QueryResult<R>` states them for the caller.
    return {
      ...grouped,
      _unknown: unknown,
      meta: { lastEvaluatedKey },
    } as DynamoDB.QueryResult<R>
  })

export const queryItems = <A extends Key>(
  model: Decodable<A>,
  params: DynamoDB.QueryParams,
): Effect.Effect<DynamoDB.QueryItems<A>, DynamoDB.QueryError, DynamoDB> =>
  Effect.map(query(params, { items: model }), ({ items, meta }) =>
    Object.assign(items, { meta }),
  )

export const stream = <A extends Key>(
  model: Decodable<A>,
  params: DynamoDB.StreamParams,
): Stream.Stream<A, DynamoDB.QueryError, DynamoDB> =>
  Stream.paginate(params.ExclusiveStartKey, (exclusiveStartKey) =>
    Effect.map(
      queryItems(model, { ...params, ExclusiveStartKey: exclusiveStartKey }),
      (items) =>
        [items, Option.fromNullishOr(items.meta.lastEvaluatedKey)] as const,
    ),
  )

export const paginate: <A extends Key>(
  model: Decodable<A>,
  input: PaginationInput,
  params: DynamoDB.PaginationParams,
) => Effect.Effect<PaginationResult<A>, DynamoDB.PaginateError, DynamoDB> =
  Effect.fn("DynamoDB.paginate")(function* <A extends Key>(
    model: Decodable<A>,
    input: PaginationInput,
    params: DynamoDB.PaginationParams,
  ) {
    const db = yield* TableService
    const page = yield* decodePagination(input, db.pagination)
    const index =
      params.IndexName !== undefined && isGSI(params.IndexName)
        ? params.IndexName
        : undefined
    const exclusiveStartKey = page.cursor
      ? yield* decodeCursor(page.cursor, index, db.cursorKeys)
      : undefined

    // One extra item tells whether there is another page. A filter or items of
    // other models can leave a Query short, so keep reading until the page is
    // full or the query is exhausted.
    const wanted = page.limit + 1
    const items: Array<A> = []
    let startKey: Record<string, unknown> | undefined = exclusiveStartKey
    do {
      const query = queryItems(model, {
        ...params,
        Limit: wanted - items.length,
        ExclusiveStartKey: startKey,
        ScanIndexForward: page.direction === "forward",
      })
      // Only the cursor's key can be rejected; later pages start from keys
      // DynamoDB returned itself.
      const batch = yield* startKey === exclusiveStartKey
        ? Effect.mapError(query, cursorRejection)
        : query
      items.push(...batch)
      startKey = batch.meta.lastEvaluatedKey
    } while (items.length < wanted && startKey !== undefined)
    const hasMore = items.length === wanted
    const slice = items.slice(0, page.limit)
    if (page.direction === "backward") slice.reverse()
    const edges = slice.map((node) => ({
      node,
      cursor: encodeCursor(storedKeysOf(node), db.cursorKeys),
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

export const cursor = (
  item: ItemKeys,
): Effect.Effect<string, never, DynamoDB> =>
  TableService.use((db) =>
    Effect.succeed(encodeCursor(storedKeysOf(item), db.cursorKeys)),
  )

/**
 * DynamoDB rejects a start key that the query could not have returned, such
 * as the cursor of an item in another partition or another connection. That
 * is a bad cursor, not a failing table.
 */
const cursorRejection = <E>(error: E): E | PaginationError =>
  error instanceof DynamoDBError &&
  error.code === "ValidationException" &&
  /start(ing)? key/i.test(describeCause(error.cause))
    ? new PaginationError({
        message: "The cursor does not belong to this query",
      })
    : error

const describeCause = (cause: unknown) =>
  cause instanceof Error ? cause.message : String(cause)

// -----------------------------------------------------------------------------
// Writes
// -----------------------------------------------------------------------------

export const put: <A extends TableItem>(
  item: A,
  options?: Operation.PutOptions,
) => Effect.Effect<A, DynamoDB.PutError, DynamoDB> = Effect.fn("DynamoDB.put")(
  function* <A extends TableItem>(item: A, options?: Operation.PutOptions) {
    const db = yield* TableService
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
  },
)

export const update: <A extends TableItem>(
  item: A,
  patch: Partial<ReturnType<A["values"]>>,
) => Effect.Effect<A, DynamoDB.UpdateError, DynamoDB> = Effect.fn(
  "DynamoDB.update",
)(function* <A extends TableItem>(
  item: A,
  patch: Partial<ReturnType<A["values"]>>,
) {
  yield* Effect.annotateCurrentSpan(spanAttributes(modelOf(item), item))
  const [next, operation] = Operation.update(item, patch)
  if (isPair(operation)) {
    yield* transaction(operation).pipe(
      Effect.catchTag(
        "TransactionCanceledError",
        (
          error,
        ): Effect.Effect<
          never,
          RaceConditionError | KeyExistsError | TransactionCanceledError
        > => {
          const [created, removed] = error.cancellationReasons.map(
            (reason) => reason.Code ?? "None",
          )
          if (error.cancellationReasons.length !== 2) return Effect.fail(error)
          if (created === "ConditionalCheckFailed" && removed === "None") {
            return Effect.fail(new KeyExistsError({ key: keyOf(next) }))
          }
          if (created === "None" && removed === "ConditionalCheckFailed") {
            return Effect.fail(new RaceConditionError({ key: keyOf(item) }))
          }
          return Effect.fail(error)
        },
      ),
    )
    return next
  }
  const { _tag, item: _, ...options } = operation
  return yield* put(next, options).pipe(
    Effect.catchTag("ConditionalCheckFailedError", () =>
      Effect.fail(new RaceConditionError({ key: keyOf(item) })),
    ),
  )
})

export const updateRaw: <A extends TableItem>(
  model: TableModel<A>,
  key: Key,
  attributes: Operation.UpdateRawAttributes<ReturnType<A["values"]>>,
  params?: Operation.UpdateRawParams,
) => Effect.Effect<A, DynamoDB.UpdateRawError, DynamoDB> = Effect.fn(
  "DynamoDB.updateRaw",
)(function* <A extends TableItem>(
  model: TableModel<A>,
  key: Key,
  attributes: Operation.UpdateRawAttributes<ReturnType<A["values"]>>,
  params?: Operation.UpdateRawParams,
) {
  const db = yield* TableService
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
  // ReturnValues ALL_NEW returns the item; a missing one means it is gone.
  if (!Attributes) return yield* new ItemNotFoundError({ key: keyOf(key) })
  return yield* decodeItem(model, Attributes)
})

export const remove: (
  key: Key,
  params?: Operation.ConditionParams,
) => Effect.Effect<void, DynamoDB.DeleteError, DynamoDB> = Effect.fn(
  "DynamoDB.delete",
)(function* (key: Key, params?: Operation.ConditionParams) {
  const db = yield* TableService
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
})

export const deleteItem: (
  item: TableItem,
  options?: Operation.DeleteOptions,
) => Effect.Effect<void, DynamoDB.DeleteItemError, DynamoDB> = Effect.fn(
  "DynamoDB.deleteItem",
)(function* (item: TableItem, options?: Operation.DeleteOptions) {
  yield* Effect.annotateCurrentSpan(spanAttributes(modelOf(item), item))
  const { key, ...params } = Operation.deleteItem(item, options)
  yield* remove(key, params).pipe(
    Effect.catchTag("ConditionalCheckFailedError", () =>
      Effect.fail(new RaceConditionError({ key: keyOf(item) })),
    ),
  )
})

export const softDelete: <A extends TableItem>(
  item: A,
  options?: Operation.DeleteOptions,
) => Effect.Effect<A, DynamoDB.SoftDeleteError, DynamoDB> = Effect.fn(
  "DynamoDB.softDelete",
)(function* <A extends TableItem>(item: A, options?: Operation.DeleteOptions) {
  yield* Effect.annotateCurrentSpan(spanAttributes(modelOf(item), item))
  const pair = Operation.softDelete(item, options)
  const guarded = pair[0].action.ConditionExpression !== undefined
  yield* transaction(pair).pipe(
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

const MAX_TRANSACTION_ITEMS = 100

export const bulk: (
  operations: Operation.BulkInput,
) => Effect.Effect<void, DynamoDB.BulkError, DynamoDB> = Effect.fn(
  "DynamoDB.bulk",
)(function* (operations: Operation.BulkInput) {
  const batches = yield* toBatches(operations)
  yield* Effect.annotateCurrentSpan({
    "model_ts.operations": batches.reduce((n, batch) => n + batch.length, 0),
    "model_ts.transactions": batches.length,
  })
  yield* Effect.uninterruptible(writeAll(batches))
  // Like `put`, a stored instance carries the version it was written with,
  // so its next update or delete is checked against it.
  for (const operation of batches.flat()) {
    const action = Operation.isTransactionOperation(operation)
      ? operation.action
      : operation
    if (action._tag === "Put" && !action.softDeleted) {
      setDocVersion(action.item, docVersion(action.item) ?? 0)
    }
  }
})

/** Operations that always share one transaction, written with `bulk`. */
const transaction = (
  group: ReadonlyArray<Operation.BulkOperation>,
): Effect.Effect<void, DynamoDB.TransactionError, DynamoDB> =>
  bulk([group]).pipe(
    // One transaction leaves nothing committed to roll back.
    Effect.catchTag("BulkWriteRollbackError", (error) => Effect.die(error)),
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
      Effect.catchCause((rollbackCause) =>
        Effect.fail(
          new BulkWriteRollbackError({
            requiresRollback: committed.slice(0, index + 1).flat(),
            cause,
            rollbackCause: Cause.squash(rollbackCause),
          }),
        ),
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

type TransactItem = NonNullable<
  TransactWriteCommandInput["TransactItems"]
>[number]

const transactWrite: (
  items: ReadonlyArray<TransactItem>,
) => Effect.Effect<void, TransactionCanceledError | DynamoDBError, DynamoDB> =
  Effect.fn("DynamoDB.transactWrite")(function* (
    items: ReadonlyArray<TransactItem>,
  ) {
    const db = yield* TableService
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
    const db = yield* TableService
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

/**
 * The UpdateItem input for an `updateRaw` operation, shared by `updateRaw`
 * and `bulk`. The update always bumps `_docVersion`, so instances read
 * before it fail their version check, and always requires the item to exist;
 * a caller's condition is added to that check.
 */
const toUpdate = Effect.fnUntraced(function* (
  operation: Operation.UpdateRawOperation,
) {
  const { model, key, attributes, UpdateExpression, ConditionExpression } =
    operation
  const names: Record<string, string> = {
    ...operation.ExpressionAttributeNames,
  }
  const values: Record<string, unknown> = {
    ...operation.ExpressionAttributeValues,
  }
  const placeholders = makePlaceholders(names, values)

  const sets: Array<string> = []
  const removals: Array<string> = []
  if (UpdateExpression !== undefined && Object.keys(attributes).length > 0) {
    return yield* Effect.die(
      new TypeError(
        "updateRaw takes either attributes or an UpdateExpression, not both.",
      ),
    )
  }
  for (const [name, value] of Object.entries(attributes)) {
    const field = model.fields[name]
    // An index key cannot be null, so a null GSI attribute is removed.
    if (field === undefined && name.startsWith("GSI") && value === null) {
      removals.push(placeholders.name(name))
      continue
    }
    const encoded = field
      ? yield* Schema.encodeUnknownEffect(field)(value).pipe(
          Effect.mapError(
            (cause) => new ItemEncodeError({ model: model.identifier, cause }),
          ),
        )
      : value
    sets.push(`${placeholders.name(name)} = ${placeholders.value(encoded)}`)
  }
  const version = placeholders.name("_docVersion")
  const bump = `${version} = if_not_exists(${version}, ${placeholders.value(0)}) + ${placeholders.value(1)}`

  return {
    Key: key,
    UpdateExpression:
      UpdateExpression === undefined
        ? [clause("SET", [...sets, bump]), clause("REMOVE", removals)]
            .filter((part) => part !== "")
            .join(" ")
        : withSetAction(UpdateExpression, bump),
    ConditionExpression:
      ConditionExpression === undefined
        ? "attribute_exists(PK)"
        : `attribute_exists(PK) AND (${ConditionExpression})`,
    ExpressionAttributeNames: names,
    ExpressionAttributeValues: values,
  }
})

/**
 * Hands out placeholders (`#u0`, `:u1`, ...) that the caller's expression
 * attribute names and values do not use, and records what they stand for.
 */
const makePlaceholders = (
  names: Record<string, string>,
  values: Record<string, unknown>,
) => {
  let next = 0
  const fresh = (prefix: "#" | ":", taken: object) => {
    let id = `${prefix}u${next++}`
    while (id in taken) id = `${prefix}u${next++}`
    return id
  }
  return {
    name: (attribute: string) => {
      const id = fresh("#", names)
      names[id] = attribute
      return id
    },
    value: (value: unknown) => {
      const id = fresh(":", values)
      values[id] = value
      return id
    },
  }
}

/**
 * Adds `action` to the SET clause of a caller's UpdateExpression, or adds a
 * SET clause. Each clause keyword appears at most once in an expression, and
 * a reserved word such as SET can only be an attribute name through a
 * placeholder, so the first standalone SET starts the clause.
 */
const withSetAction = (expression: string, action: string) => {
  const set = /\bSET\b/i.exec(expression)
  if (set === null) return `${expression} SET ${action}`
  const at = set.index + set[0].length
  return `${expression.slice(0, at)} ${action},${expression.slice(at)}`
}

const clause = (keyword: string, parts: ReadonlyArray<string>) =>
  parts.length === 0 ? "" : `${keyword} ${parts.join(", ")}`

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
