/**
 * The table connection as an Effect service, with the table operations as
 * its statics. Every operation names `DynamoDB` in its requirements, so the
 * table is chosen where the program is run: `DynamoDB.layer(...)` in
 * production, a sandbox layer in tests.
 *
 * ```ts
 * const TableLive = DynamoDB.layer({ tableName: "app", cursors: "plain" })
 * const program = DynamoDB.load(User, key).pipe(Effect.provide(TableLive))
 * ```
 *
 * `DynamoDB.provider` exposes the same operations as model members
 * (`User.load(key)`, `user.update({...})`); the statics are for code that
 * works with several models at once. Each operation fails with tagged errors
 * from `Errors` and runs in a `DynamoDB.<name>` span around the
 * `DynamoDB.<Operation>` client spans it sends.
 *
 * @since 7.0.0
 */
import {
  DynamoDBClient,
  type DynamoDBClientConfig,
} from "@aws-sdk/client-dynamodb"
import {
  BatchGetCommand,
  type BatchGetCommandOutput,
  type DeleteCommand,
  type DeleteCommandOutput,
  DynamoDBDocumentClient,
  type GetCommand,
  type GetCommandOutput,
  type PutCommand,
  type PutCommandOutput,
  type QueryCommand,
  type QueryCommandInput,
  type QueryCommandOutput,
  type TransactWriteCommand,
  type TransactWriteCommandOutput,
  type UpdateCommand,
  type UpdateCommandOutput,
} from "@aws-sdk/lib-dynamodb"
import {
  Context,
  Duration,
  Effect,
  Exit,
  Layer,
  Request,
  RequestResolver,
  Result,
} from "effect"
import {
  type BulkWriteRollbackError,
  type ConditionalCheckFailedError,
  DynamoDBError,
  type ItemDecodeError,
  type ItemEncodeError,
  type ItemNotFoundError,
  type KeyExistsError,
  type PaginationError,
  type RaceConditionError,
  type TransactionCanceledError,
} from "./Errors.ts"
import { keyOf } from "./internal/keys.ts"
import * as Table from "./internal/operations.ts"
import { parseCursorKeys } from "./internal/pagination.ts"
import { KEY } from "./internal/service.ts"
import { span } from "./internal/tracing.ts"
import type { Decodable, RawItem } from "./Item.ts"
import type { Key } from "./Keys.ts"
import type { CursorOptions, PaginationOptions } from "./Pagination.ts"
import { type DynamoDBProvider, provider } from "./Provider.ts"
import { RequestCache } from "./RequestCache.ts"

/**
 * The DynamoDB operations this library sends, by name.
 *
 * @since 7.0.0
 */
export interface Commands {
  readonly BatchGetItem: BatchGetCommand
  readonly DeleteItem: DeleteCommand
  readonly GetItem: GetCommand
  readonly PutItem: PutCommand
  readonly Query: QueryCommand
  readonly TransactWriteItems: TransactWriteCommand
  readonly UpdateItem: UpdateCommand
}

/**
 * The output of each command.
 *
 * @since 7.0.0
 */
export interface CommandOutputs {
  readonly BatchGetItem: BatchGetCommandOutput
  readonly DeleteItem: DeleteCommandOutput
  readonly GetItem: GetCommandOutput
  readonly PutItem: PutCommandOutput
  readonly Query: QueryCommandOutput
  readonly TransactWriteItems: TransactWriteCommandOutput
  readonly UpdateItem: UpdateCommandOutput
}

/**
 * The output of a command.
 *
 * @since 7.0.0
 */
export type CommandOutput<Op extends keyof Commands> = CommandOutputs[Op]

/**
 * @since 7.0.0
 */
export interface DynamoDBOptions {
  /** The single table every model is stored in. */
  readonly tableName: string
  /** Passed to `new DynamoDBClient(...)`: region, credentials, endpoint, request handler. */
  readonly client?: DynamoDBClientConfig | undefined
  /**
   * How pagination cursors are written: `{ encryptionKey }` to encrypt them,
   * or `"plain"` for readable base64. There is no default, because a plain
   * cursor shows clients every key attribute of the item.
   */
  readonly cursors: CursorOptions
  /** Defaults for `paginate`. */
  readonly pagination?: PaginationOptions | undefined
  /** The most keys one BatchGetItem reads. DynamoDB's limit is 100. */
  readonly maxBatchSize?: number | undefined
}

/**
 * @since 7.0.0
 */
export interface DynamoDBService {
  readonly tableName: string
  /**
   * Cursors are encrypted with the first key and decrypted with any of them.
   * Empty for plain cursors.
   */
  readonly cursorKeys: ReadonlyArray<Uint8Array>
  readonly pagination: Required<PaginationOptions>
  /**
   * Sends a command in a `DynamoDB.<operation>` client span. Failures become
   * `DynamoDBError`; operations map the ones they understand to more specific
   * errors.
   */
  readonly send: <Op extends keyof Commands>(
    operation: Op,
    command: Commands[Op],
    attributes?: Record<string, unknown>,
  ) => Effect.Effect<CommandOutput<Op>, DynamoDBError>
  /**
   * Reads one item. Reads issued concurrently are combined into BatchGetItem
   * calls of up to `maxBatchSize` keys.
   */
  readonly getBatched: (
    key: Key,
    consistentRead?: boolean,
  ) => Effect.Effect<RawItem | undefined, DynamoDBError>
}

/**
 * @since 7.0.0
 */
export class DynamoDB extends Context.Service<DynamoDB, DynamoDBService>()(
  KEY,
) {
  /**
   * The model provider: table operations as model members.
   *
   * ```ts
   * class Post extends Model.Class<Post>()("Post", DynamoDB.provider, {
   *   id: PostId,
   *   title: Schema.String,
   * }) {
   *   get PK() { return `POST#${this.id}` }
   *   get SK() { return "POST" }
   * }
   * ```
   */
  static readonly provider: DynamoDBProvider = provider

  /**
   * Connects to a table. The client is created when the layer is built and
   * destroyed when its scope closes.
   */
  static readonly layer = (options: DynamoDBOptions): Layer.Layer<DynamoDB> =>
    Layer.effect(DynamoDB, make(options))

  /**
   * Runs `self` with a new, empty `RequestCache`: loads of a key that was
   * already loaded or written in `self` return the known item without
   * reading the table. Wrap one request with it, never the whole container.
   *
   * ```ts
   * export const handler = (event: APIGatewayProxyEvent) =>
   *   runtime.runPromise(handle(event).pipe(DynamoDB.withRequestCache))
   * ```
   *
   * @since 7.0.0
   */
  static readonly withRequestCache = <A, E, R>(
    self: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E, Exclude<R, RequestCache>> =>
    Effect.provideServiceEffect(self, RequestCache, RequestCache.make)

  // Reads

  /**
   * One GetItem. Prefer `load`, which batches concurrent reads.
   *
   * @since 7.0.0
   */
  static readonly get = Table.get

  /**
   * Reads one item. Loads that run concurrently, e.g. inside
   * `Effect.forEach(..., { concurrency: "unbounded" })` or `Effect.all`, share
   * BatchGetItem calls of up to 100 keys. With a `RequestCache`, a key the
   * request already loaded or wrote is not read again.
   *
   * @since 7.0.0
   */
  static readonly load = Table.load

  /**
   * `load` for several keys, batched. Fails with the first error; wrap the
   * loads in `Effect.option` or `Effect.exit` yourself to keep partial results.
   *
   * @since 7.0.0
   */
  static readonly loadMany = Table.loadMany

  /**
   * Runs a Query and decodes each item with the model its `_tag` names.
   *
   * - An item tagged with one of the models must decode with it; otherwise the
   *   query fails with `ItemDecodeError`, so a corrupt item is never skipped.
   * - An item tagged with another model goes to `_unknown`.
   * - An item without `_tag` is tried against every model in order, and goes
   *   to `_unknown` when none of them decodes it.
   *
   * ```ts
   * const { posts, comments } = yield* DynamoDB.query(
   *   { KeyConditionExpression: "PK = :pk", ExpressionAttributeValues: { ":pk": key } },
   *   { posts: Post, comments: Comment },
   * )
   * ```
   *
   * @since 7.0.0
   */
  static readonly query = Table.query

  /**
   * The items of one model or union that a query matches, as
   * `Post.query(params)` returns them.
   *
   * @since 7.0.0
   */
  static readonly queryItems = Table.queryItems

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
  static readonly stream = Table.stream

  /**
   * One page of a Relay-style connection. Cursors work across requests, are
   * written as the layer's `cursors` option says, and fail with
   * `PaginationError` when they don't belong to the query.
   *
   * @since 7.0.0
   */
  static readonly paginate = Table.paginate

  /**
   * The cursor `paginate` would give `item`.
   *
   * @since 7.0.0
   */
  static readonly cursor = Table.cursor

  // Writes

  /**
   * Stores `item`. Fails with `KeyExistsError` when the key exists, unless
   * `ignoreExistence` is set or `options` bring their own condition (which
   * fails with `ConditionalCheckFailedError`).
   *
   * @since 7.0.0
   */
  static readonly put = Table.put

  /**
   * Applies `patch` and stores the result, if the stored item still has the
   * version `item` was read with (`RaceConditionError` otherwise, including
   * when it was deleted). When the patch changes the key, the item is moved in
   * one transaction, which fails with `KeyExistsError` when the new key is
   * taken.
   *
   * `item` is left as it was; use the returned instance.
   *
   * @since 7.0.0
   */
  static readonly update = Table.update

  /**
   * Builds an UpdateExpression from `attributes`, encoding each model field
   * with its schema, and returns the updated item. `null` GSI attributes are
   * removed. Fails with `ItemNotFoundError` when there is no item at `key`, or
   * `ConditionalCheckFailedError` when `params` bring a condition that fails.
   *
   * @since 7.0.0
   */
  static readonly updateRaw = Table.updateRaw

  /**
   * Deletes the item at `key`, unconditionally unless `params` add a
   * condition (`ConditionalCheckFailedError` when it fails).
   *
   * @since 7.0.0
   */
  static readonly delete = Table.remove

  /**
   * Deletes `item` if the stored item still has the version `item` was read
   * with (`RaceConditionError` otherwise, including when it is gone).
   *
   * @since 7.0.0
   */
  static readonly deleteItem = Table.deleteItem

  /**
   * Moves `item` to its `$$DELETED$$` keys in one transaction, guarded by its
   * version like `deleteItem`. `load(key, { recover: true })` reads it back.
   *
   * @since 7.0.0
   */
  static readonly softDelete = Table.softDelete

  /**
   * Runs operations in transactions of up to 100 items. A nested array stays
   * in one transaction. When a transaction fails, the ones already committed
   * are rolled back (last to first) and the original error is returned; if a
   * rollback fails too, the error is `BulkWriteRollbackError`.
   *
   * Rolling back runs the `rollback` of each committed `TransactionOperation`:
   * `put`, `softDelete` and key-changing `update` operations carry one. Other
   * operations (a same-key `update`, `updateRaw`, `delete`) stay applied.
   *
   * A defect rolls back the same way, and interruption waits until the bulk
   * write has finished or been rolled back.
   *
   * A group of more than 100 operations is a defect: it can never succeed.
   *
   * @since 7.0.0
   */
  static readonly bulk = Table.bulk
}

/**
 * The errors and parameters of the `DynamoDB` operations.
 *
 * @since 7.0.0
 */
export declare namespace DynamoDB {
  /** @since 7.0.0 */
  export type GetError = ItemNotFoundError | ItemDecodeError | DynamoDBError
  /** @since 7.0.0 */
  export type QueryError = ItemDecodeError | DynamoDBError
  /** @since 7.0.0 */
  export type PaginateError = PaginationError | QueryError
  /** @since 7.0.0 */
  export type PutError =
    | KeyExistsError
    | ConditionalCheckFailedError
    | ItemEncodeError
    | DynamoDBError
  /** @since 7.0.0 */
  export type BulkError =
    | TransactionCanceledError
    | BulkWriteRollbackError
    | ItemEncodeError
    | DynamoDBError
  /**
   * A failed transaction of a single-transaction write. It has no earlier
   * transaction to roll back, so it cannot fail with `BulkWriteRollbackError`.
   *
   * @since 7.0.0
   */
  export type TransactionError =
    | TransactionCanceledError
    | ItemEncodeError
    | DynamoDBError
  /** @since 7.0.0 */
  export type UpdateError =
    | RaceConditionError
    | KeyExistsError
    | TransactionError
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
  export type SoftDeleteError = RaceConditionError | TransactionError

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
      readonly fetchAllPages?: boolean | undefined
    }

  /** @since 7.0.0 */
  export interface QueryMeta {
    readonly lastEvaluatedKey?: Record<string, unknown> | undefined
  }

  /**
   * The models of a query, by the name their items are grouped under.
   * `_unknown` and `meta` are taken by the result.
   *
   * @since 7.0.0
   */
  export type QueryModels = Record<string, Decodable> & {
    readonly _unknown?: never
    readonly meta?: never
  }

  /**
   * `query(params, models)` groups the items by the name each decodable was
   * given. Items of other models end up in `_unknown`.
   *
   * @since 7.0.0
   */
  export type QueryResult<R extends QueryModels> = {
    readonly [K in keyof R]: Array<R[K]["Type"]>
  } & {
    readonly _unknown: Array<RawItem>
    readonly meta: QueryMeta
  }

  /**
   * The items of one decodable, as returned by `Model.query(params)`. The
   * array carries the query's `meta`.
   *
   * @since 7.0.0
   */
  export type QueryItems<A> = Array<A> & { readonly meta: QueryMeta }

  /** @since 7.0.0 */
  export type StreamParams = Omit<QueryParams, "fetchAllPages">

  /**
   * Query parameters for `paginate`. Page size, start key and direction come
   * from the pagination input.
   *
   * @since 7.0.0
   */
  export type PaginationParams = Omit<
    QueryParams,
    "fetchAllPages" | "Limit" | "ExclusiveStartKey" | "ScanIndexForward"
  >
}

const DEFAULT_PAGINATION: Required<PaginationOptions> = {
  limit: 50,
  default: 20,
}

/**
 * How often keys that BatchGetItem leaves unprocessed (usually because of
 * throttling) are read again, waiting 50ms, 100ms, 200ms, ... in between.
 */
const UNPROCESSED_RETRIES = 5

interface GetItemRequest extends Request.Request<
  RawItem | undefined,
  DynamoDBError
> {
  readonly _tag: "GetItemRequest"
  readonly key: Key
  readonly consistentRead: boolean
}
const GetItemRequest = Request.tagged<GetItemRequest>("GetItemRequest")

const make = Effect.fnUntraced(function* (options: DynamoDBOptions) {
  const { tableName } = options
  const invalid = invalidOption(options)
  if (invalid !== undefined) return yield* Effect.die(new RangeError(invalid))
  const cursorKeys = parseCursorKeys(options.cursors)
  if (Result.isFailure(cursorKeys)) {
    return yield* Effect.die(new RangeError(cursorKeys.failure))
  }
  const pagination: Required<PaginationOptions> = {
    limit: options.pagination?.limit ?? DEFAULT_PAGINATION.limit,
    default: options.pagination?.default ?? DEFAULT_PAGINATION.default,
  }

  const client = yield* Effect.acquireRelease(
    Effect.sync(() =>
      DynamoDBDocumentClient.from(new DynamoDBClient(options.client ?? {}), {
        marshallOptions: { removeUndefinedValues: true },
      }),
    ),
    (client) => Effect.sync(() => client.destroy()),
  )

  const send = <Op extends keyof Commands>(
    operation: Op,
    command: Commands[Op],
    attributes: Record<string, unknown> = {},
  ): Effect.Effect<CommandOutput<Op>, DynamoDBError> =>
    Effect.tryPromise({
      try: (signal) =>
        // `client.send` has an overload per command type. TypeScript can't
        // pick one for the union `Commands[Op]` or relate its output to
        // `CommandOutputs[Op]` (microsoft/TypeScript#30581), so both ends
        // are asserted against the maps above.
        client.send(command as Parameters<typeof client.send>[0], {
          abortSignal: signal,
        }) as Promise<CommandOutput<Op>>,
      catch: (cause) => new DynamoDBError({ operation, cause }),
    }).pipe(
      span(`DynamoDB.${operation}`, {
        kind: "client",
        attributes: definedOnly({
          "db.system.name": "aws.dynamodb",
          "db.operation.name": operation,
          "aws.dynamodb.table_names": [tableName],
          "rpc.system": "aws-api",
          "rpc.service": "DynamoDB",
          "rpc.method": operation,
          ...attributes,
        }),
      }),
    )

  const fetchBatch = (
    keys: ReadonlyArray<Key>,
    consistentRead: boolean,
    attempt = 0,
  ): Effect.Effect<Array<RawItem>, DynamoDBError> =>
    Effect.gen(function* () {
      const output = yield* send(
        "BatchGetItem",
        new BatchGetCommand({
          RequestItems: {
            [tableName]: {
              Keys: keys.map(keyOf),
              ConsistentRead: consistentRead,
            },
          },
        }),
        { "aws.dynamodb.keys_count": keys.length },
      )
      const items: Array<RawItem> = output.Responses?.[tableName] ?? []
      const unprocessed = (output.UnprocessedKeys?.[tableName]?.Keys ?? []).map(
        (item): Key => ({ PK: String(item.PK), SK: String(item.SK) }),
      )
      if (unprocessed.length === 0) return items
      if (attempt === UNPROCESSED_RETRIES) {
        return yield* new DynamoDBError({
          operation: "BatchGetItem",
          cause: new Error(
            `${unprocessed.length} keys were still unprocessed after ${UNPROCESSED_RETRIES} retries`,
          ),
        })
      }
      yield* Effect.sleep(Duration.millis(50 * 2 ** attempt))
      return [
        ...items,
        ...(yield* fetchBatch(unprocessed, consistentRead, attempt + 1)),
      ]
    })

  // Every GetItemRequest issued in the same tick reaches one runAll call. The
  // batch span links to the span of every load it serves.
  const resolver = RequestResolver.make<GetItemRequest>((entries) =>
    Effect.gen(function* () {
      const unique = new Map<string, Key>()
      for (const { request } of entries) {
        unique.set(identify(request.key.PK, request.key.SK), request.key)
      }
      const consistentRead = entries.some(
        ({ request }) => request.consistentRead,
      )
      const items = yield* fetchBatch([...unique.values()], consistentRead)
      const byKey = new Map(
        items.map((item) => [identify(item.PK, item.SK), item]),
      )
      for (const entry of entries) {
        entry.completeUnsafe(
          Exit.succeed(
            byKey.get(identify(entry.request.key.PK, entry.request.key.SK)),
          ),
        )
      }
    }),
  ).pipe(
    RequestResolver.batchN(options.maxBatchSize ?? 100),
    RequestResolver.withSpan("DynamoDB.batchGet", { captureStackTrace: false }),
  )

  return DynamoDB.of({
    tableName,
    cursorKeys: cursorKeys.success,
    pagination,
    send,
    getBatched: (key, consistentRead = false) =>
      Effect.request(GetItemRequest({ key, consistentRead }), resolver),
  })
})

/** A string that identifies a key; unambiguous whatever PK and SK contain. */
const identify = (pk: unknown, sk: unknown) => JSON.stringify([pk, sk])

/** Why `options` cannot work, or `undefined` when they can. */
const invalidOption = (options: DynamoDBOptions): string | undefined => {
  const isCount = (value: number | undefined, max = Infinity) =>
    value === undefined ||
    (Number.isSafeInteger(value) && value >= 1 && value <= max)
  if (!isCount(options.maxBatchSize, 100)) {
    return "maxBatchSize must be an integer from 1 to 100"
  }
  if (
    !isCount(options.pagination?.limit) ||
    !isCount(options.pagination?.default)
  ) {
    return "pagination.limit and pagination.default must be positive integers"
  }
  return undefined
}

const definedOnly = (attributes: Record<string, unknown>) =>
  Object.fromEntries(
    Object.entries(attributes).filter(([, value]) => value !== undefined),
  )
