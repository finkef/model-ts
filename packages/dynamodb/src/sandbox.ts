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
 * Both print the same diff, so snapshot tests pass against either.
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
import { randomBytes } from "node:crypto"
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
import { Context, Effect, Layer } from "effect"
import { DynamoDB, type DynamoDBOptions } from "./DynamoDB.ts"
import type { ItemEncodeError } from "./Errors.ts"
import { encodeItem, type RawItem } from "./Item.ts"
import type { Key } from "./Keys.ts"
import { formatSnapshotDiff } from "./internal/diff.ts"
import {
  makeMemoryEngine,
  type MemoryEngine,
} from "./internal/memory/engine.ts"
import { memoryClientConfig } from "./internal/memory/handler.ts"
import { createTableCommand } from "./internal/table.ts"

/**
 * Every item of a table, keyed by `"PK__SK"`.
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
}

/**
 * Options for the table's `DynamoDB` service.
 *
 * @since 7.0.0
 */
export type SandboxOptions = Omit<DynamoDBOptions, "tableName" | "client">

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
      Effect.sync(() => {
        const engine = makeMemoryEngine()
        const client = memoryClientConfig(engine)
        const tableName = newTableName()
        return Layer.merge(
          Layer.succeed(
            Sandbox,
            make(tableName, {
              write: (items) => writeItems(engine, tableName, items),
              read: async () => structuredClone(engine.snapshot(tableName)),
            }),
          ),
          DynamoDB.layer({ ...options, tableName, client }),
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
              marshallOptions: { removeUndefinedValues: true },
            }),
          ),
          (documents) => Effect.sync(() => documents.destroy()),
        )
        const tableName = newTableName()
        yield* Effect.acquireRelease(
          Effect.promise(() => documents.send(createTableCommand(tableName))),
          () =>
            Effect.promise(() =>
              documents.send(new DeleteTableCommand({ TableName: tableName })),
            ),
        )
        return Layer.merge(
          Layer.succeed(
            Sandbox,
            make(tableName, {
              write: (items) => batchWrite(documents, tableName, items),
              read: () => scan(documents, tableName),
            }),
          ),
          DynamoDB.layer({ ...rest, tableName, client }),
        )
      }),
    )
}

interface Backend {
  readonly write: (items: ReadonlyArray<RawItem>) => Promise<void>
  readonly read: () => Promise<TableSnapshot>
}

const make = (tableName: string, backend: Backend) => {
  const snapshot = Effect.promise(backend.read)
  return Sandbox.of({
    tableName,
    seed: (...items) =>
      Effect.forEach(items, (item) =>
        isModelInstance(item)
          ? encodeItem(item)
          : Effect.succeed(item as RawItem),
      ).pipe(
        Effect.flatMap((encoded) =>
          Effect.promise(() => backend.write(encoded)),
        ),
      ),
    snapshot,
    diff: (before) =>
      Effect.map(snapshot, (after) => formatSnapshotDiff(before, after)),
    get: (key) =>
      Effect.map(snapshot, (table) => table[`${key.PK}__${key.SK}`]),
  })
}

const newTableName = () => `sandbox-${randomBytes(8).toString("hex")}`

const isModelInstance = (item: object): item is Key =>
  typeof (item.constructor as { identifier?: unknown }).identifier === "string"

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
    for (const item of (page.Items ?? []) as Array<RawItem>) {
      table[`${item.PK}__${item.SK}`] = item
    }
    ExclusiveStartKey = page.LastEvaluatedKey
  } while (ExclusiveStartKey)
  return table
}
