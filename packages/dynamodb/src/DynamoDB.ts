/**
 * The table connection as an Effect service. Every operation names
 * `DynamoDB` in its requirements, so the table is chosen where the program
 * is run: `DynamoDB.layer(...)` in production, a sandbox layer in tests.
 *
 * ```ts
 * const TableLive = DynamoDB.layer({ tableName: "app" })
 * const program = User.load(key).pipe(Effect.provide(TableLive))
 * ```
 *
 * @since 7.0.0
 */
import {
  DynamoDBClient,
  type DynamoDBClientConfig,
} from "@aws-sdk/client-dynamodb"
import {
  BatchGetCommand,
  type DeleteCommand,
  DynamoDBDocumentClient,
  type GetCommand,
  type PutCommand,
  type QueryCommand,
  type TransactWriteCommand,
  type UpdateCommand,
} from "@aws-sdk/lib-dynamodb"
import {
  type Config,
  Context,
  Effect,
  Exit,
  Layer,
  Request,
  RequestResolver,
} from "effect"
import { DynamoDBError } from "./Errors.ts"
import type { RawItem } from "./Item.ts"
import { type Key, keyOf } from "./Keys.ts"
import type { PaginationOptions } from "./Pagination.ts"

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
 * The output of a command.
 *
 * @since 7.0.0
 */
export type CommandOutput<Op extends keyof Commands> = Commands[Op] extends {
  resolveMiddleware: (...args: ReadonlyArray<any>) => {
    (...args: ReadonlyArray<any>): Promise<{ output: infer O }>
  }
}
  ? O
  : never

/**
 * @since 7.0.0
 */
export interface DynamoDBOptions {
  /** The single table every model is stored in. */
  readonly tableName: string
  /** Passed to `new DynamoDBClient(...)`: region, credentials, endpoint, request handler. */
  readonly client?: DynamoDBClientConfig | undefined
  /**
   * Encrypts pagination cursors with AES-256-CTR. Must be 32 bytes. Without
   * it, cursors are base64-encoded keys.
   */
  readonly cursorEncryptionKey?: Uint8Array | undefined
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
  readonly cursorEncryptionKey: Uint8Array | undefined
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
  "@model-ts/dynamodb/DynamoDB",
) {
  /**
   * Connects to a table. The client is created when the layer is built and
   * destroyed when its scope closes.
   */
  static readonly layer = (options: DynamoDBOptions): Layer.Layer<DynamoDB> =>
    Layer.effect(DynamoDB, make(options))

  /**
   * Like `layer`, with options read from `Config` when the layer is built.
   *
   * ```ts
   * DynamoDB.layerConfig(Config.all({ tableName: Config.String("TABLE_NAME") }))
   * ```
   */
  static readonly layerConfig = (
    options: Config.Config<DynamoDBOptions>,
  ): Layer.Layer<DynamoDB, Config.ConfigError> =>
    Layer.unwrap(
      Effect.gen(function* () {
        return DynamoDB.layer(yield* options)
      }),
    )
}

const DEFAULT_PAGINATION: Required<PaginationOptions> = {
  limit: 50,
  default: 20,
}

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
        client.send(command as Parameters<typeof client.send>[0], {
          abortSignal: signal,
        }) as Promise<CommandOutput<Op>>,
      catch: (cause) => new DynamoDBError({ operation, cause }),
    }).pipe(
      Effect.withSpan(`DynamoDB.${operation}`, {
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
      const items = (output.Responses?.[tableName] ?? []) as Array<RawItem>
      const unprocessed = (output.UnprocessedKeys?.[tableName]?.Keys ??
        []) as Array<Key>
      if (unprocessed.length === 0) return items
      if (items.length === 0) {
        return yield* new DynamoDBError({
          operation: "BatchGetItem",
          cause: new Error("BatchGetItem returned only unprocessed keys"),
        })
      }
      return [...items, ...(yield* fetchBatch(unprocessed, consistentRead))]
    })

  // Every GetItemRequest issued in the same tick reaches one runAll call. The
  // batch span links to the span of every load it serves.
  const resolver = RequestResolver.make<GetItemRequest>((entries) =>
    Effect.gen(function* () {
      const unique = new Map<string, Key>()
      for (const { request } of entries) {
        unique.set(identify(request.key), request.key)
      }
      const consistentRead = entries.some(
        ({ request }) => request.consistentRead,
      )
      const items = yield* fetchBatch([...unique.values()], consistentRead)
      const byKey = new Map(
        items.map((item) => [identify(item as unknown as Key), item]),
      )
      for (const entry of entries) {
        entry.completeUnsafe(
          Exit.succeed(byKey.get(identify(entry.request.key))),
        )
      }
    }),
  ).pipe(
    RequestResolver.batchN(options.maxBatchSize ?? 100),
    RequestResolver.withSpan("Dynamo.batchGet"),
  )

  return DynamoDB.of({
    tableName,
    cursorEncryptionKey: options.cursorEncryptionKey,
    pagination: { ...DEFAULT_PAGINATION, ...options.pagination },
    send,
    getBatched: (key, consistentRead = false) =>
      Effect.request(GetItemRequest({ key, consistentRead }), resolver),
  })
})

/** A string that identifies a key; unambiguous whatever PK and SK contain. */
const identify = (key: Key) => JSON.stringify([key.PK, key.SK])

const definedOnly = (attributes: Record<string, unknown>) =>
  Object.fromEntries(
    Object.entries(attributes).filter(([, value]) => value !== undefined),
  )
