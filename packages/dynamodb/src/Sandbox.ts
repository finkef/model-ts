/**
 * Test tables as layers. Each layer provides `DynamoDB` (so the code under
 * test runs unchanged) and `Sandbox` (to seed and inspect the table), and
 * every build of the layer gets a fresh, empty table.
 *
 * - `Sandbox.layerMemory()`: an in-process engine behind the real AWS SDK.
 *   No DynamoDB process, no network, fast enough for every unit test.
 * - `Sandbox.layerLocal()`: a new table on DynamoDB Local, deleted when the
 *   layer's scope closes.
 *
 * Both print the same diff, so snapshot tests pass against either, and both
 * emit the table's change records as DynamoDB Streams would.
 *
 * ```ts
 * it.effect("renames a post", () =>
 *   Effect.gen(function* () {
 *     const sandbox = yield* Sandbox
 *     yield* sandbox.seed(post)
 *     const before = yield* sandbox.snapshot
 *     yield* post.update({ title: "Renamed" })
 *     expect(yield* sandbox.diff(before)).toMatchInlineSnapshot()
 *   }).pipe(Effect.provide(Sandbox.layerMemory())),
 * )
 * ```
 *
 * @since 7.0.0
 */
import { createHash, randomBytes } from "node:crypto"
import {
  DeleteTableCommand,
  DynamoDBClient,
  type DynamoDBClientConfig,
} from "@aws-sdk/client-dynamodb"
import {
  BatchWriteCommand,
  DynamoDBDocumentClient,
  ScanCommand,
} from "@aws-sdk/lib-dynamodb"
import { Context, Effect, Layer, Schema, type Scope, type Stream } from "effect"
import { DynamoDB, type DynamoDBOptions } from "./DynamoDB.ts"
import type { ItemEncodeError } from "./Errors.ts"
import { formatSnapshotDiff } from "./internal/diff.ts"
import { encodeItem } from "./internal/item.ts"
import {
  makeMemoryEngine,
  type MemoryEngine,
} from "./internal/memory/engine.ts"
import { memoryClientConfig } from "./internal/memory/handler.ts"
import { makeMemoryStream } from "./internal/memory/stream.ts"
import { snapshotKey } from "./internal/snapshot.ts"
import { streamChanges } from "./internal/streams.ts"
import { createTableCommand, marshallOptions } from "./internal/table.ts"
import type { RawItem } from "./Item.ts"
import type { Key } from "./Keys.ts"
import type { CursorOptions } from "./Pagination.ts"
import type { StreamRecord } from "./Stream.ts"

/**
 * Every item of a table, keyed by `JSON.stringify([PK, SK])`. Use
 * `Sandbox.get` to read one item.
 *
 * @since 7.0.0
 */
export type TableSnapshot = Record<string, RawItem>

/**
 * @since 7.0.0
 */
export interface SandboxService {
  readonly tableName: string
  /**
   * Writes model instances (encoded with keys and `_docVersion`) or raw items
   * straight into the table, without spans or conditions.
   */
  readonly seed: (
    ...items: ReadonlyArray<Key | RawItem>
  ) => Effect.Effect<void, ItemEncodeError>
  /** The current contents of the table. */
  readonly snapshot: Effect.Effect<TableSnapshot>
  /**
   * What changed since `before`: one `+`, `-` or `~` block per item, with the
   * attributes that changed.
   */
  readonly diff: (before: TableSnapshot) => Effect.Effect<string>
  /** The stored item at `key`. */
  readonly get: (key: Key) => Effect.Effect<RawItem | undefined>
  /**
   * Subscribes to the table's stream. The stream carries a record for every
   * write committed after the subscription, in commit order, and ends with
   * the scope. Subscribe before writing:
   *
   * ```ts
   * const changes = yield* sandbox.changes
   * yield* post.update({ title: "Renamed" })
   * const [record] = yield* changes.pipe(Stream.take(1), Stream.runCollect)
   * ```
   *
   * On DynamoDB Local this reads the table's real stream and needs
   * `@aws-sdk/client-dynamodb-streams`.
   *
   * @since 7.1.0
   */
  readonly changes: Effect.Effect<
    Stream.Stream<StreamRecord>,
    never,
    Scope.Scope
  >
}

/**
 * Options for the table's `DynamoDB` service. `cursors` defaults to
 * encryption with a fixed test key, so the same items get the same cursors on
 * every run and cursors can appear in snapshots. Pass `cursors: "plain"` for
 * readable ones.
 *
 * @since 7.0.0
 */
export interface SandboxOptions extends Omit<
  DynamoDBOptions,
  "tableName" | "client" | "cursors"
> {
  readonly cursors?: CursorOptions | undefined
}

/**
 * @since 7.0.0
 */
export interface LocalSandboxOptions extends SandboxOptions {
  /** Defaults to `http://localhost:8000`. */
  readonly endpoint?: string | undefined
  /** Extra client options, e.g. a different region or credentials. */
  readonly client?: DynamoDBClientConfig | undefined
}

/**
 * @since 7.0.0
 */
export class Sandbox extends Context.Service<Sandbox, SandboxService>()(
  "@model-ts/dynamodb/Sandbox",
) {
  /** A fresh in-memory table on every build. */
  static readonly layerMemory = (
    options: SandboxOptions = {},
  ): Layer.Layer<Sandbox | DynamoDB> =>
    Layer.unwrap(
      Effect.gen(function* () {
        const tableName = newTableName()
        const stream = yield* makeMemoryStream(tableName)
        const engine = makeMemoryEngine({ onChange: stream.onChange })
        const client = memoryClientConfig(engine)
        return Layer.merge(
          Layer.succeed(
            Sandbox,
            make(tableName, {
              write: (items) => writeItems(engine, tableName, items),
              read: async () => structuredClone(engine.snapshot(tableName)),
              changes: stream.changes,
            }),
          ),
          DynamoDB.layer({ ...withCursors(options), tableName, client }),
        )
      }),
    )

  /**
   * A fresh table on DynamoDB Local on every build, deleted when the scope
   * closes. Start DynamoDB Local with
   * `docker run -p 8000:8000 amazon/dynamodb-local`.
   */
  static readonly layerLocal = (
    options: LocalSandboxOptions = {},
  ): Layer.Layer<Sandbox | DynamoDB> =>
    Layer.unwrap(
      Effect.gen(function* () {
        const {
          endpoint = "http://localhost:8000",
          client: config,
          ...rest
        } = options
        const client: DynamoDBClientConfig = {
          endpoint,
          region: "local",
          credentials: { accessKeyId: "local", secretAccessKey: "local" },
          ...config,
        }
        const documents = yield* Effect.acquireRelease(
          Effect.sync(() =>
            DynamoDBDocumentClient.from(new DynamoDBClient(client), {
              marshallOptions: marshallOptions(),
            }),
          ),
          (documents) => Effect.sync(() => documents.destroy()),
        )
        const tableName = newTableName()
        const { TableDescription } = yield* Effect.acquireRelease(
          Effect.promise(() =>
            documents.send(createTableCommand(tableName, { stream: true })),
          ),
          () =>
            Effect.promise(() =>
              documents.send(new DeleteTableCommand({ TableName: tableName })),
            ),
        )
        const streamArn = TableDescription?.LatestStreamArn
        if (!streamArn) {
          return yield* Effect.die(
            new Error(`DynamoDB Local created ${tableName} without a stream`),
          )
        }
        return Layer.merge(
          Layer.succeed(
            Sandbox,
            make(tableName, {
              write: (items) => batchWrite(documents, tableName, items),
              read: () => scan(documents, tableName),
              changes: streamChanges(client, streamArn),
            }),
          ),
          DynamoDB.layer({ ...withCursors(rest), tableName, client }),
        )
      }),
    )
}

/**
 * The cursor key of every sandbox. It is public, which is fine for test
 * tables and keeps cursors identical across runs and machines.
 */
const SANDBOX_CURSOR_KEY = createHash("sha256")
  .update("@model-ts/dynamodb sandbox cursors")
  .digest()

const withCursors = <O extends SandboxOptions>(options: O) => ({
  ...options,
  cursors: options.cursors ?? { encryptionKey: SANDBOX_CURSOR_KEY },
})

interface Backend {
  readonly write: (items: ReadonlyArray<RawItem>) => Promise<void>
  readonly read: () => Promise<TableSnapshot>
  readonly changes: SandboxService["changes"]
}

const make = (tableName: string, backend: Backend) => {
  const snapshot = Effect.promise(backend.read)
  return Sandbox.of({
    tableName,
    seed: (...items) =>
      Effect.forEach(items, (item) =>
        isModelInstance(item) ? encodeItem(item) : Effect.succeed(item),
      ).pipe(
        Effect.flatMap((encoded) =>
          Effect.promise(() => backend.write(encoded)),
        ),
      ),
    snapshot,
    diff: (before) =>
      Effect.map(snapshot, (after) => formatSnapshotDiff(before, after)),
    get: (key) => Effect.map(snapshot, (table) => table[snapshotKey(key)]),
    changes: backend.changes,
  })
}

const newTableName = () => `sandbox-${randomBytes(8).toString("hex")}`

const isModelInstance = (item: Key | RawItem): item is Key =>
  Schema.isSchema(item.constructor)

const writeItems = async (
  engine: MemoryEngine,
  TableName: string,
  items: ReadonlyArray<RawItem>,
) => {
  for (const Item of items) await engine.put({ TableName, Item })
}

const batchWrite = async (
  documents: DynamoDBDocumentClient,
  tableName: string,
  items: ReadonlyArray<RawItem>,
) => {
  for (let i = 0; i < items.length; i += 25) {
    await documents.send(
      new BatchWriteCommand({
        RequestItems: {
          [tableName]: items
            .slice(i, i + 25)
            .map((Item) => ({ PutRequest: { Item } })),
        },
      }),
    )
  }
}

const scan = async (documents: DynamoDBDocumentClient, tableName: string) => {
  const table: TableSnapshot = {}
  let ExclusiveStartKey: Record<string, unknown> | undefined
  do {
    const page = await documents.send(
      new ScanCommand({ TableName: tableName, ExclusiveStartKey }),
    )
    for (const item of page.Items ?? []) {
      table[snapshotKey({ PK: String(item.PK), SK: String(item.SK) })] = item
    }
    ExclusiveStartKey = page.LastEvaluatedKey
  } while (ExclusiveStartKey)
  return table
}
