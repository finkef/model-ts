/**
 * The engine tests talk to a table through the plain document client, the
 * way the in-memory engine was specified: once against DynamoDB Local and
 * once against the engine behind its request handler. Both go through the
 * same AWS SDK client, so any difference is the engine's.
 */
import { randomBytes } from "node:crypto"
import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient,
  type DynamoDBClientConfig,
  waitUntilTableExists,
  waitUntilTableNotExists,
} from "@aws-sdk/client-dynamodb"
import {
  BatchGetCommand,
  BatchWriteCommand,
  DeleteCommand,
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  ScanCommand,
  TransactGetCommand,
  TransactWriteCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb"
import { makeMemoryEngine } from "../src/internal/memory/engine.ts"
import { memoryClientConfig } from "../src/internal/memory/handler.ts"
import { createTableCommand } from "../src/internal/table.ts"

export type Engine = "local" | "memory"

type Params = Record<string, any>
type Request = { promise: () => Promise<any> }

/** The promise-style client the engine's test vectors were written against. */
export interface DocumentClient {
  get(params: Params): Request
  put(params: Params): Request
  update(params: Params): Request
  delete(params: Params): Request
  query(params: Params): Request
  scan(params: Params): Request
  batchGet(params: Params): Request
  batchWrite(params: Params): Request
  transactWrite(params: Params): Request
  transactGet(params: Params): Request
}

export interface Harness {
  readonly engine: Engine
  readonly tableName: string
  readonly client: {
    readonly documentClient: DocumentClient
    readonly tableName: string
  }
  readonly sandbox: {
    readonly snapshot: () => Promise<Record<string, any>>
    readonly get: (pk: string, sk: string) => Promise<any>
  }
  readonly createAuxTable: (tableName: string) => Promise<void>
  readonly deleteAuxTable: (tableName: string) => Promise<void>
  readonly destroy: () => Promise<void>
}

const LOCAL: DynamoDBClientConfig = {
  endpoint: "http://localhost:8000",
  region: "local",
  credentials: { accessKeyId: "local", secretAccessKey: "local" },
}

/** AWS SDK errors carry their name; the vectors compare `code`. */
const withCode = (error: any) => {
  if (error && typeof error === "object" && !error.code) error.code = error.name
  return error
}

const withoutMetadata = (output: any) => {
  if (!output || typeof output !== "object") return output
  const { $metadata: _metadata, ...rest } = output
  return rest
}

const promiseClient = (client: DynamoDBDocumentClient): DocumentClient => {
  const request = (command: any): Request => ({
    promise: () =>
      client.send(command).then(withoutMetadata, (error) => {
        throw withCode(error)
      }),
  })
  return {
    get: (params) => request(new GetCommand(params as any)),
    put: (params) => request(new PutCommand(params as any)),
    update: (params) => request(new UpdateCommand(params as any)),
    delete: (params) => request(new DeleteCommand(params as any)),
    query: (params) => request(new QueryCommand(params as any)),
    scan: (params) => request(new ScanCommand(params as any)),
    batchGet: (params) => request(new BatchGetCommand(params as any)),
    batchWrite: (params) => ({
      promise: () =>
        request(new BatchWriteCommand(params as any))
          .promise()
          .then((output) => ({ UnprocessedItems: {}, ...output })),
    }),
    transactWrite: (params) => request(new TransactWriteCommand(params as any)),
    transactGet: (params) => request(new TransactGetCommand(params as any)),
  }
}

const scan = async (documents: DocumentClient, TableName: string) => {
  const table: Record<string, any> = {}
  let ExclusiveStartKey: Params | undefined
  do {
    const page = await documents
      .scan({ TableName, ExclusiveStartKey })
      .promise()
    for (const item of page.Items ?? []) table[`${item.PK}__${item.SK}`] = item
    ExclusiveStartKey = page.LastEvaluatedKey
  } while (ExclusiveStartKey)
  return table
}

export const makeHarness = async (engine: Engine): Promise<Harness> => {
  const tableName = randomBytes(20).toString("hex")
  const memory = engine === "memory" ? makeMemoryEngine() : undefined
  const raw = new DynamoDBClient(memory ? memoryClientConfig(memory) : LOCAL)
  const client = DynamoDBDocumentClient.from(raw, {
    marshallOptions: { removeUndefinedValues: true },
  })
  const documentClient = promiseClient(client)
  if (!memory) await raw.send(createTableCommand(tableName))

  const local = async (run: () => Promise<unknown>) => {
    if (!memory) await run()
  }

  return {
    engine,
    tableName,
    client: { documentClient, tableName },
    sandbox: {
      snapshot: async () =>
        memory ? memory.snapshot(tableName) : scan(documentClient, tableName),
      get: async (PK, SK) => {
        const { Item } = await documentClient
          .get({ TableName: tableName, Key: { PK, SK } })
          .promise()
        return Item ?? null
      },
    },
    createAuxTable: (TableName) =>
      local(async () => {
        await raw
          .send(
            new CreateTableCommand({
              TableName,
              AttributeDefinitions: [
                { AttributeName: "PK", AttributeType: "S" },
                { AttributeName: "SK", AttributeType: "S" },
              ],
              KeySchema: [
                { AttributeName: "PK", KeyType: "HASH" },
                { AttributeName: "SK", KeyType: "RANGE" },
              ],
              BillingMode: "PAY_PER_REQUEST",
            }),
          )
          .catch((error) => {
            if (error?.name !== "ResourceInUseException") throw error
          })
        await waitUntilTableExists(
          { client: raw, maxWaitTime: 30 },
          { TableName },
        )
      }),
    deleteAuxTable: (TableName) =>
      local(async () => {
        await raw.send(new DeleteTableCommand({ TableName })).catch((error) => {
          if (error?.name !== "ResourceNotFoundException") throw error
        })
        await waitUntilTableNotExists(
          { client: raw, maxWaitTime: 30 },
          { TableName },
        )
      }),
    destroy: async () => {
      if (memory) memory.resetTable(tableName)
      else await raw.send(new DeleteTableCommand({ TableName: tableName }))
      client.destroy()
    },
  }
}

export const withHarness = async <T>(
  engine: Engine,
  run: (harness: Harness) => Promise<T>,
): Promise<T> => {
  const harness = await makeHarness(engine)
  try {
    return await run(harness)
  } finally {
    await harness.destroy()
  }
}
