import { Client } from "../client"
import { createSandbox, Sandbox } from "../sandbox"
import { IN_MEMORY_SPEC } from "../in-memory/spec"
import DynamoDB from "aws-sdk/clients/dynamodb"

type Engine = "local" | "memory"

type Context = {
  engine: Engine
  client: Client
  sandbox: Sandbox
  tableName: string
}

type NormalizedSuccess = {
  ok: true
  value: any
  snapshot: { [key: string]: any }
}

type NormalizedError = {
  ok: false
  error: {
    code: string
    message?: string
    method?: string
    featurePath?: string
    reason?: string
  }
  snapshot: { [key: string]: any }
}

type NormalizedOutcome = NormalizedSuccess | NormalizedError

type Vector = {
  id: string
  method: keyof typeof IN_MEMORY_SPEC.methods
  setup?: (ctx: Context) => Promise<void>
  execute: (ctx: Context) => Promise<any>
  normalizeResult?: (value: any) => any
  coverage?: {
    supported?: string[]
    unsupported?: string[]
  }
}

const LOCAL_DDB = new DynamoDB({
  accessKeyId: "xxx",
  secretAccessKey: "xxx",
  endpoint: process.env.LOCAL_ENDPOINT,
  region: "local",
})

const withEngine = async <T>(engine: Engine, run: (ctx: Context) => Promise<T>) => {
  const previous = process.env.EXPERIMENTAL_DYNAMODB_IN_MEMORY

  if (engine === "memory") process.env.EXPERIMENTAL_DYNAMODB_IN_MEMORY = "1"
  else delete process.env.EXPERIMENTAL_DYNAMODB_IN_MEMORY

  const client = new Client({ tableName: "table" })
  const sandbox = await createSandbox(client)

  try {
    return await run({
      engine,
      client,
      sandbox,
      tableName: client.tableName,
    })
  } finally {
    await sandbox.destroy()

    if (typeof previous === "undefined")
      delete process.env.EXPERIMENTAL_DYNAMODB_IN_MEMORY
    else process.env.EXPERIMENTAL_DYNAMODB_IN_MEMORY = previous
  }
}

const normalizeScalar = (value: any): any => {
  if (typeof value === "string") {
    return value.replace(/[0-9a-f]{40}/gi, "<table>")
  }

  return value
}

const normalizeObject = (value: any): any => {
  if (value === undefined) return undefined
  if (Array.isArray(value)) return value.map(normalizeObject)

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .filter((key) => typeof value[key] !== "undefined")
        .map((key) => [normalizeScalar(key), normalizeObject(value[key])])
    )
  }

  return normalizeScalar(value)
}

const normalizeSnapshot = (snapshot: { [key: string]: any }) =>
  Object.fromEntries(
    Object.keys(snapshot)
      .sort()
      .map((key) => [key, normalizeObject(snapshot[key])])
  )

const sanitizeErrorMessage = (message: string): string =>
  canonicalizeValidationMessage(
    message
      .replace(/[0-9a-f]{40}/gi, "<table>")
      .replace(/\s+/g, " ")
      .trim()
      .replace(/\.$/, "")
      .replace(/\([^)]*\)/g, (match) => {
        if (match.includes("<table>")) return "(<table>)"
        return match
      })
  )

const canonicalizeValidationMessage = (message: string): string => {
  if (
    message ===
      "Consistent read cannot be true when querying a GSI" ||
    message ===
      "Consistent reads are not supported on global secondary indexes"
  ) {
    return "Consistent reads are not supported on global secondary indexes"
  }

  if (
    message === "The provided starting key is invalid" ||
    message === "Exclusive Start Key must have same size as table's key schema"
  ) {
    return "The provided starting key is invalid"
  }

  return message
}

const normalizeError = (error: any) => {
  const code =
    error?.code ??
    (typeof error?.name === "string" && error.name !== "Error"
      ? error.name
      : "UnknownError")

  if (code === "NotSupportedError") {
    return {
      code,
      message: sanitizeErrorMessage(String(error?.message ?? "")),
      method: error?.method,
      featurePath: error?.featurePath,
      reason: error?.reason,
    }
  }

  return {
    code,
    message: sanitizeErrorMessage(String(error?.message ?? "")),
  }
}

const normalizeResult = (method: Vector["method"], value: any): any => {
  if (!value || typeof value !== "object") return value

  if (method === "batchGet") {
    const responses = value.Responses ?? {}
    const normalizedResponses = Object.fromEntries(
      Object.keys(responses)
        .sort()
        .map((tableName) => [
          normalizeScalar(tableName),
          [...responses[tableName]].sort(compareItemsByPKSK).map(normalizeObject),
        ])
    )

    const unprocessed = value.UnprocessedKeys ?? value.UnprocessedItems ?? {}
    const normalizedUnprocessed = Object.fromEntries(
      Object.keys(unprocessed)
        .sort()
        .map((tableName) => [normalizeScalar(tableName), normalizeObject(unprocessed[tableName])])
    )

    return { Responses: normalizedResponses, Unprocessed: normalizedUnprocessed }
  }

  if (method === "delete") {
    const normalized = normalizeObject(value) ?? {}
    if (!normalized || typeof normalized !== "object") return normalized
    const { ConsumedCapacity: _ignored, ...rest } = normalized
    return rest
  }

  if (method === "scan") {
    return {
      ...normalizeObject(value),
      Items: [...(value.Items ?? [])].sort(compareItemsByPKSK).map(normalizeObject),
    }
  }

  return normalizeObject(value)
}

const compareItemsByPKSK = (left: any, right: any) => {
  const leftPK = String(left?.PK ?? "")
  const rightPK = String(right?.PK ?? "")
  if (leftPK !== rightPK) return leftPK < rightPK ? -1 : 1

  const leftSK = String(left?.SK ?? "")
  const rightSK = String(right?.SK ?? "")
  if (leftSK !== rightSK) return leftSK < rightSK ? -1 : 1

  return 0
}

const runVector = async (engine: Engine, vector: Vector): Promise<NormalizedOutcome> => {
  return withEngine(engine, async (ctx) => {
    if (vector.setup) {
      await vector.setup(ctx)
    }

    try {
      const raw = await vector.execute(ctx)
      const snapshot = normalizeSnapshot(await ctx.sandbox.snapshot())

      return {
        ok: true,
        value: vector.normalizeResult
          ? vector.normalizeResult(raw)
          : normalizeResult(vector.method, raw),
        snapshot,
      }
    } catch (error) {
      const snapshot = normalizeSnapshot(await ctx.sandbox.snapshot())

      return {
        ok: false,
        error: normalizeError(error),
        snapshot,
      }
    }
  })
}

const createSeed = async (ctx: Context) => {
  await ctx.client.documentClient
    .batchWrite({
      RequestItems: {
        [ctx.tableName]: [
          {
            PutRequest: {
              Item: {
                PK: "USER#1",
                SK: "PROFILE#001",
                GSI2PK: "EMAIL#ada@example.com",
                GSI2SK: "USER#1",
                name: "Ada",
                age: 30,
                status: "active",
                score: 10,
                tags: ["math", "history"],
              },
            },
          },
          {
            PutRequest: {
              Item: {
                PK: "USER#1",
                SK: "ORDER#001",
                GSI2PK: "ORDER#OPEN",
                GSI2SK: "2021-01-01T00:00:00.000Z",
                total: 120,
                currency: "USD",
                status: "open",
              },
            },
          },
          {
            PutRequest: {
              Item: {
                PK: "USER#1",
                SK: "ORDER#002",
                GSI2PK: "ORDER#CLOSED",
                GSI2SK: "2021-01-02T00:00:00.000Z",
                total: 99,
                currency: "USD",
                status: "closed",
              },
            },
          },
          {
            PutRequest: {
              Item: {
                PK: "USER#2",
                SK: "PROFILE#001",
                GSI2PK: "EMAIL#grace@example.com",
                GSI2SK: "USER#2",
                name: "Grace",
                age: 35,
                status: "pending",
                score: 20,
              },
            },
          },
          {
            PutRequest: {
              Item: {
                PK: "USER#3",
                SK: "PROFILE#001",
                GSI2PK: "EMAIL#alan@example.com",
                GSI2SK: "USER#3",
                name: "Alan",
                age: 28,
                status: "active",
                score: 15,
              },
            },
          },
        ],
      },
    })
    .promise()
}

const createAuxTableIfNeeded = async (ctx: Context, tableName: string) => {
  if (ctx.engine !== "local") return

  await LOCAL_DDB.createTable({
    TableName: tableName,
    AttributeDefinitions: [
      { AttributeName: "PK", AttributeType: "S" },
      { AttributeName: "SK", AttributeType: "S" },
    ],
    KeySchema: [
      { AttributeName: "PK", KeyType: "HASH" },
      { AttributeName: "SK", KeyType: "RANGE" },
    ],
    BillingMode: "PAY_PER_REQUEST",
  })
    .promise()
    .catch((error: any) => {
      if (error?.code === "ResourceInUseException") return
      throw error
    })

  await LOCAL_DDB.waitFor("tableExists", { TableName: tableName }).promise()
}

const destroyAuxTableIfNeeded = async (ctx: Context, tableName: string) => {
  if (ctx.engine !== "local") return

  await LOCAL_DDB.deleteTable({ TableName: tableName })
    .promise()
    .catch((error: any) => {
      if (error?.code === "ResourceNotFoundException") return
      throw error
    })

  await LOCAL_DDB.waitFor("tableNotExists", { TableName: tableName })
    .promise()
    .catch((error: any) => {
      if (error?.code === "ResourceNotFoundException") return
      throw error
    })
}

const baseVectorsByMethod: {
  [K in keyof typeof IN_MEMORY_SPEC.methods]: Vector[]
} = {
  get: [
    {
      id: "get.existing",
      method: "get",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .get({ TableName: tableName, Key: { PK: "USER#1", SK: "PROFILE#001" } })
          .promise(),
    },
    {
      id: "get.missing",
      method: "get",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .get({ TableName: tableName, Key: { PK: "USER#404", SK: "PROFILE#001" } })
          .promise(),
    },
    {
      id: "get.consistent-read",
      method: "get",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .get({
            TableName: tableName,
            Key: { PK: "USER#2", SK: "PROFILE#001" },
            ConsistentRead: true,
          })
          .promise(),
    },
    {
      id: "get.bad-key",
      method: "get",
      execute: ({ client, tableName }) =>
        client.documentClient
          .get({ TableName: tableName, Key: { PK: 123, SK: "A" } as any })
          .promise(),
    },
  ],
  put: [
    {
      id: "put.insert",
      method: "put",
      execute: ({ client, tableName }) =>
        client.documentClient
          .put({
            TableName: tableName,
            Item: {
              PK: "USER#10",
              SK: "PROFILE#001",
              GSI2PK: "EMAIL#new@example.com",
              GSI2SK: "USER#10",
              name: "New",
              score: 1,
            },
          })
          .promise(),
    },
    {
      id: "put.overwrite",
      method: "put",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .put({
            TableName: tableName,
            Item: {
              PK: "USER#1",
              SK: "PROFILE#001",
              GSI2PK: "EMAIL#ada@example.com",
              GSI2SK: "USER#1",
              name: "Ada Lovelace",
              age: 31,
            },
          })
          .promise(),
    },
    {
      id: "put.conditional-fail",
      method: "put",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .put({
            TableName: tableName,
            Item: {
              PK: "USER#1",
              SK: "PROFILE#001",
              name: "Nope",
            },
            ConditionExpression: "attribute_not_exists(PK)",
          })
          .promise(),
    },
    {
      id: "put.conditional-pass-with-placeholders",
      method: "put",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .put({
            TableName: tableName,
            Item: {
              PK: "USER#11",
              SK: "PROFILE#001",
              name: "Placeholder",
              status: "active",
            },
            ConditionExpression: "attribute_not_exists(#pk) and :status = :status",
            ExpressionAttributeNames: { "#pk": "PK" },
            ExpressionAttributeValues: { ":status": "active" },
          })
          .promise(),
    },
    {
      id: "put.bad-condition-expression",
      method: "put",
      execute: ({ client, tableName }) =>
        client.documentClient
          .put({
            TableName: tableName,
            Item: { PK: "A", SK: "A" },
            ConditionExpression: "unknown_fn(PK)",
          })
          .promise(),
    },
  ],
  update: [
    {
      id: "update.set-and-return-all-new",
      method: "update",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .update({
            TableName: tableName,
            Key: { PK: "USER#1", SK: "PROFILE#001" },
            UpdateExpression: "SET #name = :name, #score = :score",
            ExpressionAttributeNames: { "#name": "name", "#score": "score" },
            ExpressionAttributeValues: { ":name": "Ada Updated", ":score": 11 },
            ReturnValues: "ALL_NEW",
          })
          .promise(),
    },
    {
      id: "update.remove-gsi",
      method: "update",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .update({
            TableName: tableName,
            Key: { PK: "USER#1", SK: "PROFILE#001" },
            UpdateExpression: "REMOVE GSI2PK, GSI2SK",
            ReturnValues: "ALL_NEW",
          })
          .promise(),
    },
    {
      id: "update.upsert",
      method: "update",
      execute: ({ client, tableName }) =>
        client.documentClient
          .update({
            TableName: tableName,
            Key: { PK: "USER#99", SK: "PROFILE#001" },
            UpdateExpression: "SET #name = :name",
            ExpressionAttributeNames: { "#name": "name" },
            ExpressionAttributeValues: { ":name": "Created" },
            ReturnValues: "ALL_NEW",
          })
          .promise(),
    },
    {
      id: "update.conditional-fail",
      method: "update",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .update({
            TableName: tableName,
            Key: { PK: "USER#404", SK: "PROFILE#001" },
            ConditionExpression: "attribute_exists(PK)",
            UpdateExpression: "SET #name = :name",
            ExpressionAttributeNames: { "#name": "name" },
            ExpressionAttributeValues: { ":name": "Nope" },
          })
          .promise(),
    },
    {
      id: "update.bad-update-expression",
      method: "update",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .update({
            TableName: tableName,
            Key: { PK: "USER#1", SK: "PROFILE#001" },
            UpdateExpression: "SET",
          })
          .promise(),
    },
    {
      id: "update.missing-placeholder",
      method: "update",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .update({
            TableName: tableName,
            Key: { PK: "USER#1", SK: "PROFILE#001" },
            UpdateExpression: "SET #name = :missing",
            ExpressionAttributeNames: { "#name": "name" },
          })
          .promise(),
    },
    {
      id: "update.key-mutation-error",
      method: "update",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .update({
            TableName: tableName,
            Key: { PK: "USER#1", SK: "PROFILE#001" },
            UpdateExpression: "SET PK = :pk",
            ExpressionAttributeValues: { ":pk": "USER#X" },
          })
          .promise(),
    },
  ],
  delete: [
    {
      id: "delete.existing",
      method: "delete",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .delete({ TableName: tableName, Key: { PK: "USER#3", SK: "PROFILE#001" } })
          .promise(),
    },
    {
      id: "delete.missing",
      method: "delete",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .delete({ TableName: tableName, Key: { PK: "USER#404", SK: "PROFILE#001" } })
          .promise(),
    },
    {
      id: "delete.conditional-fail",
      method: "delete",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .delete({
            TableName: tableName,
            Key: { PK: "USER#2", SK: "PROFILE#001" },
            ConditionExpression: "#status = :status",
            ExpressionAttributeNames: { "#status": "status" },
            ExpressionAttributeValues: { ":status": "active" },
          })
          .promise(),
    },
    {
      id: "delete.conditional-pass",
      method: "delete",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .delete({
            TableName: tableName,
            Key: { PK: "USER#2", SK: "PROFILE#001" },
            ConditionExpression: "#status = :status",
            ExpressionAttributeNames: { "#status": "status" },
            ExpressionAttributeValues: { ":status": "pending" },
          })
          .promise(),
    },
  ],
  query: [
    {
      id: "query.hash-only",
      method: "query",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .query({
            TableName: tableName,
            KeyConditionExpression: "PK = :pk",
            ExpressionAttributeValues: { ":pk": "USER#1" },
          })
          .promise(),
    },
    {
      id: "query.begins-with",
      method: "query",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .query({
            TableName: tableName,
            KeyConditionExpression: "PK = :pk and begins_with(SK, :prefix)",
            ExpressionAttributeValues: {
              ":pk": "USER#1",
              ":prefix": "ORDER#",
            },
          })
          .promise(),
    },
    {
      id: "query.range-between",
      method: "query",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .query({
            TableName: tableName,
            KeyConditionExpression: "PK = :pk and SK between :from and :to",
            ExpressionAttributeValues: {
              ":pk": "USER#1",
              ":from": "ORDER#001",
              ":to": "ORDER#010",
            },
          })
          .promise(),
    },
    {
      id: "query.range-operator-and-backward",
      method: "query",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .query({
            TableName: tableName,
            KeyConditionExpression: "PK = :pk and SK >= :from",
            ExpressionAttributeValues: {
              ":pk": "USER#1",
              ":from": "ORDER#001",
            },
            ScanIndexForward: false,
          })
          .promise(),
    },
    {
      id: "query.filter-limit-scanned-count",
      method: "query",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .query({
            TableName: tableName,
            KeyConditionExpression: "PK = :pk",
            FilterExpression: "#status = :status",
            ExpressionAttributeNames: { "#status": "status" },
            ExpressionAttributeValues: {
              ":pk": "USER#1",
              ":status": "open",
            },
            Limit: 1,
          })
          .promise(),
    },
    {
      id: "query.pagination-exclusive-start",
      method: "query",
      setup: async (ctx) => {
        await createSeed(ctx)
      },
      execute: ({ client, tableName }) =>
        client.documentClient
          .query({
            TableName: tableName,
            KeyConditionExpression: "PK = :pk",
            ExpressionAttributeValues: { ":pk": "USER#1" },
            Limit: 1,
          })
          .promise(),
    },
    {
      id: "query.gsi",
      method: "query",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .query({
            TableName: tableName,
            IndexName: "GSI2",
            KeyConditionExpression: "GSI2PK = :pk",
            ExpressionAttributeValues: { ":pk": "ORDER#OPEN" },
          })
          .promise(),
    },
    {
      id: "query.gsi-consistent-read-error",
      method: "query",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .query({
            TableName: tableName,
            IndexName: "GSI2",
            KeyConditionExpression: "GSI2PK = :pk",
            ExpressionAttributeValues: { ":pk": "ORDER#OPEN" },
            ConsistentRead: true,
          })
          .promise(),
    },
    {
      id: "query.bad-key-condition",
      method: "query",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .query({
            TableName: tableName,
            KeyConditionExpression: "PK = :pk and invalid(SK, :x)",
            ExpressionAttributeValues: {
              ":pk": "USER#1",
              ":x": "A",
            },
          })
          .promise(),
    },
  ],
  scan: [
    {
      id: "scan.all",
      method: "scan",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient.scan({ TableName: tableName }).promise(),
    },
    {
      id: "scan.filter-limit",
      method: "scan",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .scan({
            TableName: tableName,
            FilterExpression: "attribute_exists(GSI2PK) and #status = :status",
            ExpressionAttributeNames: { "#status": "status" },
            ExpressionAttributeValues: { ":status": "active" },
          })
          .promise(),
    },
    {
      id: "scan.bad-limit",
      method: "scan",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .scan({ TableName: tableName, Limit: 0 })
          .promise(),
    },
  ],
  batchGet: [
    {
      id: "batch-get.basic",
      method: "batchGet",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .batchGet({
            RequestItems: {
              [tableName]: {
                Keys: [
                  { PK: "USER#1", SK: "PROFILE#001" },
                  { PK: "USER#2", SK: "PROFILE#001" },
                ],
              },
            },
          })
          .promise(),
    },
    {
      id: "batch-get.missing-items",
      method: "batchGet",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .batchGet({
            RequestItems: {
              [tableName]: {
                Keys: [
                  { PK: "USER#1", SK: "PROFILE#001" },
                  { PK: "USER#404", SK: "PROFILE#001" },
                ],
              },
            },
          })
          .promise(),
    },
    {
      id: "batch-get.duplicate-keys-error",
      method: "batchGet",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .batchGet({
            RequestItems: {
              [tableName]: {
                Keys: [
                  { PK: "USER#1", SK: "PROFILE#001" },
                  { PK: "USER#1", SK: "PROFILE#001" },
                ],
              },
            },
          })
          .promise(),
    },
    {
      id: "batch-get.too-many-keys-error",
      method: "batchGet",
      execute: ({ client, tableName }) =>
        client.documentClient
          .batchGet({
            RequestItems: {
              [tableName]: {
                Keys: Array.from({ length: 101 }).map((_, i) => ({
                  PK: `USER#${i}`,
                  SK: "PROFILE#001",
                })),
              },
            },
          })
          .promise(),
    },
  ],
  batchWrite: [
    {
      id: "batch-write.put-and-delete",
      method: "batchWrite",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .batchWrite({
            RequestItems: {
              [tableName]: [
                {
                  PutRequest: {
                    Item: {
                      PK: "USER#20",
                      SK: "PROFILE#001",
                      name: "BatchPut",
                    },
                  },
                },
                {
                  DeleteRequest: {
                    Key: { PK: "USER#3", SK: "PROFILE#001" },
                  },
                },
              ],
            },
          })
          .promise(),
    },
    {
      id: "batch-write.too-many-items-error",
      method: "batchWrite",
      execute: ({ client, tableName }) =>
        client.documentClient
          .batchWrite({
            RequestItems: {
              [tableName]: Array.from({ length: 26 }).map((_, i) => ({
                PutRequest: {
                  Item: { PK: `U#${i}`, SK: "S#1" },
                },
              })),
            },
          })
          .promise(),
    },
    {
      id: "batch-write.invalid-entry-error",
      method: "batchWrite",
      execute: ({ client, tableName }) =>
        client.documentClient
          .batchWrite({
            RequestItems: {
              [tableName]: [{ Nope: true } as any],
            },
          })
          .promise(),
    },
  ],
  transactWrite: [
    {
      id: "transact-write.success",
      method: "transactWrite",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .transactWrite({
            TransactItems: [
              {
                ConditionCheck: {
                  TableName: tableName,
                  Key: { PK: "USER#1", SK: "PROFILE#001" },
                  ConditionExpression: "attribute_exists(PK)",
                },
              },
              {
                Update: {
                  TableName: tableName,
                  Key: { PK: "USER#2", SK: "PROFILE#001" },
                  UpdateExpression: "SET #score = :score",
                  ExpressionAttributeNames: { "#score": "score" },
                  ExpressionAttributeValues: { ":score": 25 },
                },
              },
              {
                Put: {
                  TableName: tableName,
                  Item: {
                    PK: "USER#30",
                    SK: "PROFILE#001",
                    name: "FromTx",
                  },
                  ConditionExpression: "attribute_not_exists(PK)",
                },
              },
              {
                Delete: {
                  TableName: tableName,
                  Key: { PK: "USER#3", SK: "PROFILE#001" },
                },
              },
            ],
          })
          .promise(),
    },
    {
      id: "transact-write.conditional-fail-and-rollback",
      method: "transactWrite",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .transactWrite({
            TransactItems: [
              {
                Update: {
                  TableName: tableName,
                  Key: { PK: "USER#2", SK: "PROFILE#001" },
                  UpdateExpression: "SET #score = :score",
                  ExpressionAttributeNames: { "#score": "score" },
                  ExpressionAttributeValues: { ":score": 100 },
                },
              },
              {
                ConditionCheck: {
                  TableName: tableName,
                  Key: { PK: "USER#404", SK: "PROFILE#001" },
                  ConditionExpression: "attribute_exists(PK)",
                },
              },
            ],
          })
          .promise(),
    },
    {
      id: "transact-write.duplicate-target-error",
      method: "transactWrite",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .transactWrite({
            TransactItems: [
              {
                Update: {
                  TableName: tableName,
                  Key: { PK: "USER#2", SK: "PROFILE#001" },
                  UpdateExpression: "SET #score = :score",
                  ExpressionAttributeNames: { "#score": "score" },
                  ExpressionAttributeValues: { ":score": 100 },
                },
              },
              {
                Delete: {
                  TableName: tableName,
                  Key: { PK: "USER#2", SK: "PROFILE#001" },
                },
              },
            ],
          })
          .promise(),
    },
    {
      id: "transact-write.too-many-items-error",
      method: "transactWrite",
      execute: ({ client, tableName }) =>
        client.documentClient
          .transactWrite({
            TransactItems: Array.from({ length: 101 }).map((_, i) => ({
              Put: {
                TableName: tableName,
                Item: { PK: `TX#${i}`, SK: "S#1" },
              },
            })),
          })
          .promise(),
    },
    {
      id: "transact-write.bad-update-expression-error",
      method: "transactWrite",
      setup: createSeed,
      execute: ({ client, tableName }) =>
        client.documentClient
          .transactWrite({
            TransactItems: [
              {
                Update: {
                  TableName: tableName,
                  Key: { PK: "USER#2", SK: "PROFILE#001" },
                  UpdateExpression: "SET",
                },
              },
            ],
          })
          .promise(),
    },
  ],
}

const generatedVectors: Vector[] = Object.keys(IN_MEMORY_SPEC.methods).flatMap(
  (method) => {
    const typedMethod = method as keyof typeof IN_MEMORY_SPEC.methods
    return baseVectorsByMethod[typedMethod]
  }
)

const additionalDifferentialVectors: Vector[] = [
  {
    id: "query.pagination-continuity",
    method: "query",
    setup: createSeed,
    execute: async ({ client, tableName }) => {
      const p1 = await client.documentClient
        .query({
          TableName: tableName,
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "USER#1" },
          Limit: 1,
        })
        .promise()

      const p2 = await client.documentClient
        .query({
          TableName: tableName,
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "USER#1" },
          Limit: 1,
          ExclusiveStartKey: p1.LastEvaluatedKey,
        })
        .promise()

      const p3 = await client.documentClient
        .query({
          TableName: tableName,
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "USER#1" },
          Limit: 10,
          ExclusiveStartKey: p2.LastEvaluatedKey,
        })
        .promise()

      return { p1, p2, p3 }
    },
  },
  {
    id: "scan.pagination-continuity",
    method: "scan",
    setup: createSeed,
    execute: async ({ client, tableName }) => {
      const pages: any[] = []
      let startKey: any = undefined

      do {
        const page = await client.documentClient
          .scan({
            TableName: tableName,
            Limit: 2,
            ExclusiveStartKey: startKey,
          })
          .promise()

        pages.push(page)
        startKey = page.LastEvaluatedKey
      } while (startKey)

      const full = await client.documentClient
        .scan({
          TableName: tableName,
        })
        .promise()

      return {
        pagedItems: pages.flatMap((page) => page.Items ?? []),
        fullItems: full.Items ?? [],
        pageCount: pages.length,
      }
    },
    normalizeResult: (value) => ({
      ...normalizeObject(value),
      pagedItems: [...(value.pagedItems ?? [])]
        .sort(compareItemsByPKSK)
        .map(normalizeObject),
      fullItems: [...(value.fullItems ?? [])]
        .sort(compareItemsByPKSK)
        .map(normalizeObject),
      pageCount: value.pageCount,
      pagedMatchesFull:
        JSON.stringify(
          [...(value.pagedItems ?? [])].sort(compareItemsByPKSK).map(normalizeObject)
        ) ===
        JSON.stringify(
          [...(value.fullItems ?? [])].sort(compareItemsByPKSK).map(normalizeObject)
        ),
    }),
  },
  {
    id: "query.bad-exclusive-start-key",
    method: "query",
    setup: createSeed,
    execute: ({ client, tableName }) =>
      client.documentClient
        .query({
          TableName: tableName,
          IndexName: "GSI2",
          KeyConditionExpression: "GSI2PK = :pk",
          ExpressionAttributeValues: { ":pk": "ORDER#OPEN" },
          ExclusiveStartKey: { PK: "USER#1", SK: "ORDER#001" },
        })
        .promise(),
  },
  {
    id: "query.missing-expression-attribute-name",
    method: "query",
    setup: createSeed,
    execute: ({ client, tableName }) =>
      client.documentClient
        .query({
          TableName: tableName,
          KeyConditionExpression: "#pk = :pk",
          ExpressionAttributeValues: { ":pk": "USER#1" },
        })
        .promise(),
  },
  {
    id: "query.limit-non-integer",
    method: "query",
    setup: createSeed,
    execute: ({ client, tableName }) =>
      client.documentClient
        .query({
          TableName: tableName,
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "USER#1" },
          Limit: 1.5,
        })
        .promise(),
  },
  {
    id: "update.if-not-exists-existing-plus",
    method: "update",
    setup: createSeed,
    execute: ({ client, tableName }) =>
      client.documentClient
        .update({
          TableName: tableName,
          Key: { PK: "USER#1", SK: "PROFILE#001" },
          UpdateExpression: "SET #score = if_not_exists(#score, :zero) + :inc",
          ExpressionAttributeNames: { "#score": "score" },
          ExpressionAttributeValues: { ":zero": 0, ":inc": 2 },
          ReturnValues: "ALL_NEW",
        })
        .promise(),
  },
  {
    id: "update.if-not-exists-missing-plus",
    method: "update",
    setup: createSeed,
    execute: ({ client, tableName }) =>
      client.documentClient
        .update({
          TableName: tableName,
          Key: { PK: "USER#2", SK: "PROFILE#001" },
          UpdateExpression: "SET #visits = if_not_exists(#visits, :zero) + :inc",
          ExpressionAttributeNames: { "#visits": "visits" },
          ExpressionAttributeValues: { ":zero": 0, ":inc": 1 },
          ReturnValues: "ALL_NEW",
        })
        .promise(),
  },
  {
    id: "update.list-append-with-if-not-exists",
    method: "update",
    setup: createSeed,
    execute: ({ client, tableName }) =>
      client.documentClient
        .update({
          TableName: tableName,
          Key: { PK: "USER#2", SK: "PROFILE#001" },
          UpdateExpression:
            "SET #tags = list_append(if_not_exists(#tags, :empty), :more)",
          ExpressionAttributeNames: { "#tags": "tags" },
          ExpressionAttributeValues: { ":empty": [], ":more": ["new", "vip"] },
          ReturnValues: "ALL_NEW",
        })
        .promise(),
  },
  {
    id: "scan.filter.contains",
    method: "scan",
    setup: createSeed,
    execute: ({ client, tableName }) =>
      client.documentClient
        .scan({
          TableName: tableName,
          FilterExpression: "contains(#tags, :tag)",
          ExpressionAttributeNames: { "#tags": "tags" },
          ExpressionAttributeValues: { ":tag": "math" },
        })
        .promise(),
  },
  {
    id: "scan.filter.attribute-type-and-size",
    method: "scan",
    setup: createSeed,
    execute: ({ client, tableName }) =>
      client.documentClient
        .scan({
          TableName: tableName,
          FilterExpression: "attribute_type(#name, :t) and size(#name) > :min",
          ExpressionAttributeNames: { "#name": "name" },
          ExpressionAttributeValues: { ":t": "S", ":min": 2 },
        })
        .promise(),
  },
  {
    id: "scan.missing-expression-attribute-value",
    method: "scan",
    setup: createSeed,
    execute: ({ client, tableName }) =>
      client.documentClient
        .scan({
          TableName: tableName,
          FilterExpression: "#status = :missing",
          ExpressionAttributeNames: { "#status": "status" },
        })
        .promise(),
  },
  {
    id: "batch-get.multi-table",
    method: "batchGet",
    setup: createSeed,
    execute: async (ctx) => {
      const auxTable = `${ctx.tableName}-aux-batch-get`
      await createAuxTableIfNeeded(ctx, auxTable)

      try {
        await ctx.client.documentClient
          .batchWrite({
            RequestItems: {
              [auxTable]: [
                {
                  PutRequest: {
                    Item: { PK: "AUX#1", SK: "ITEM#1", flag: true },
                  },
                },
              ],
            },
          })
          .promise()

        return await ctx.client.documentClient
          .batchGet({
            RequestItems: {
              [ctx.tableName]: {
                Keys: [{ PK: "USER#1", SK: "PROFILE#001" }],
              },
              [auxTable]: {
                Keys: [{ PK: "AUX#1", SK: "ITEM#1" }],
              },
            },
          })
          .promise()
      } finally {
        await destroyAuxTableIfNeeded(ctx, auxTable)
      }
    },
  },
  {
    id: "batch-write.multi-table",
    method: "batchWrite",
    setup: createSeed,
    normalizeResult: () => ({ UnprocessedItems: {} }),
    execute: async (ctx) => {
      const auxTable = `${ctx.tableName}-aux-batch-write`
      await createAuxTableIfNeeded(ctx, auxTable)

      try {
        return await ctx.client.documentClient
          .batchWrite({
            RequestItems: {
              [ctx.tableName]: [
                {
                  PutRequest: { Item: { PK: "USER#70", SK: "PROFILE#001", ok: true } },
                },
              ],
              [auxTable]: [
                {
                  PutRequest: { Item: { PK: "AUX#2", SK: "ITEM#1", ok: true } },
                },
              ],
            },
          })
          .promise()
      } finally {
        await destroyAuxTableIfNeeded(ctx, auxTable)
      }
    },
  },
  {
    id: "transact-write.rollback-on-parser-error-mid-transaction",
    method: "transactWrite",
    setup: createSeed,
    execute: ({ client, tableName }) =>
      client.documentClient
        .transactWrite({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { PK: "USER#2", SK: "PROFILE#001" },
                UpdateExpression: "SET #score = :score",
                ExpressionAttributeNames: { "#score": "score" },
                ExpressionAttributeValues: { ":score": 999 },
              },
            },
            {
              Update: {
                TableName: tableName,
                Key: { PK: "USER#1", SK: "PROFILE#001" },
                UpdateExpression: "SET",
              },
            },
          ],
        })
        .promise(),
  },
  {
    id: "transact-write.key-mutation-error",
    method: "transactWrite",
    setup: createSeed,
    execute: ({ client, tableName }) =>
      client.documentClient
        .transactWrite({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { PK: "USER#2", SK: "PROFILE#001" },
                UpdateExpression: "SET PK = :pk",
                ExpressionAttributeValues: { ":pk": "USER#X" },
              },
            },
          ],
        })
        .promise(),
  },
]

const supportedParamCoverageVectors: Vector[] = [
  {
    id: "coverage.get.supported",
    method: "get",
    setup: createSeed,
    coverage: { supported: ["TableName", "Key", "ConsistentRead"] },
    execute: ({ client, tableName }) =>
      client.documentClient
        .get({
          TableName: tableName,
          Key: { PK: "USER#1", SK: "PROFILE#001" },
          ConsistentRead: true,
        })
        .promise(),
  },
  {
    id: "coverage.put.supported",
    method: "put",
    setup: createSeed,
    coverage: {
      supported: [
        "TableName",
        "Item",
        "ConditionExpression",
        "ExpressionAttributeNames",
        "ExpressionAttributeValues",
      ],
    },
    execute: ({ client, tableName }) =>
      client.documentClient
        .put({
          TableName: tableName,
          Item: { PK: "COVER#PUT", SK: "1", status: "ok" },
          ConditionExpression: "attribute_not_exists(#pk) and :v = :v",
          ExpressionAttributeNames: { "#pk": "PK" },
          ExpressionAttributeValues: { ":v": "ok" },
        })
        .promise(),
  },
  {
    id: "coverage.update.supported",
    method: "update",
    setup: createSeed,
    coverage: {
      supported: [
        "TableName",
        "Key",
        "ConditionExpression",
        "UpdateExpression",
        "ExpressionAttributeNames",
        "ExpressionAttributeValues",
        "ReturnValues",
      ],
    },
    execute: ({ client, tableName }) =>
      client.documentClient
        .update({
          TableName: tableName,
          Key: { PK: "USER#1", SK: "PROFILE#001" },
          ConditionExpression: "attribute_exists(PK)",
          UpdateExpression: "SET #score = :score",
          ExpressionAttributeNames: { "#score": "score" },
          ExpressionAttributeValues: { ":score": 12 },
          ReturnValues: "ALL_NEW",
        })
        .promise(),
  },
  {
    id: "coverage.delete.supported",
    method: "delete",
    setup: createSeed,
    coverage: {
      supported: [
        "TableName",
        "Key",
        "ConditionExpression",
        "ExpressionAttributeNames",
        "ExpressionAttributeValues",
      ],
    },
    execute: ({ client, tableName }) =>
      client.documentClient
        .delete({
          TableName: tableName,
          Key: { PK: "USER#3", SK: "PROFILE#001" },
          ConditionExpression: "#status = :status",
          ExpressionAttributeNames: { "#status": "status" },
          ExpressionAttributeValues: { ":status": "active" },
        })
        .promise(),
  },
  {
    id: "coverage.query.supported",
    method: "query",
    setup: createSeed,
    coverage: {
      supported: [
        "TableName",
        "IndexName",
        "KeyConditionExpression",
        "FilterExpression",
        "ExpressionAttributeNames",
        "ExpressionAttributeValues",
        "Limit",
        "ExclusiveStartKey",
        "ScanIndexForward",
        "ConsistentRead",
      ],
    },
    execute: async ({ client, tableName }) => {
      const firstPrimary = await client.documentClient
        .query({
          TableName: tableName,
          KeyConditionExpression: "PK = :pk",
          FilterExpression: "attribute_exists(#status)",
          ExpressionAttributeNames: { "#status": "status" },
          ExpressionAttributeValues: { ":pk": "USER#1" },
          ScanIndexForward: true,
          Limit: 1,
        })
        .promise()

      const secondPrimary = await client.documentClient
        .query({
          TableName: tableName,
          KeyConditionExpression: "PK = :pk",
          FilterExpression: "attribute_exists(#status)",
          ExpressionAttributeNames: { "#status": "status" },
          ExpressionAttributeValues: { ":pk": "USER#1" },
          ScanIndexForward: true,
          Limit: 2,
          ExclusiveStartKey: firstPrimary.LastEvaluatedKey,
        })
        .promise()

      const gsi = await client.documentClient
        .query({
          TableName: tableName,
          IndexName: "GSI2",
          KeyConditionExpression: "GSI2PK = :pk",
          ExpressionAttributeValues: { ":pk": "ORDER#OPEN" },
          ConsistentRead: false,
          Limit: 1,
        })
        .promise()

      return { secondPrimary, gsi }
    },
  },
  {
    id: "coverage.scan.supported",
    method: "scan",
    setup: createSeed,
    coverage: {
      supported: [
        "TableName",
        "FilterExpression",
        "ExpressionAttributeNames",
        "ExpressionAttributeValues",
        "Limit",
        "ExclusiveStartKey",
      ],
    },
    execute: async ({ client, tableName }) => {
      const first = await client.documentClient
        .scan({
          TableName: tableName,
          FilterExpression: "#status = :status",
          ExpressionAttributeNames: { "#status": "status" },
          ExpressionAttributeValues: { ":status": "active" },
          Limit: 1,
        })
        .promise()

      await client.documentClient
        .scan({
          TableName: tableName,
          FilterExpression: "#status = :status",
          ExpressionAttributeNames: { "#status": "status" },
          ExpressionAttributeValues: { ":status": "active" },
          Limit: 2,
          ExclusiveStartKey: first.LastEvaluatedKey,
        })
        .promise()

      return client.documentClient
        .scan({
          TableName: tableName,
          FilterExpression: "#status = :status",
          ExpressionAttributeNames: { "#status": "status" },
          ExpressionAttributeValues: { ":status": "active" },
        })
        .promise()
    },
  },
  {
    id: "coverage.batchGet.supported",
    method: "batchGet",
    setup: createSeed,
    coverage: { supported: ["RequestItems"] },
    execute: ({ client, tableName }) =>
      client.documentClient
        .batchGet({
          RequestItems: { [tableName]: { Keys: [{ PK: "USER#1", SK: "PROFILE#001" }] } },
        })
        .promise(),
  },
  {
    id: "coverage.batchWrite.supported",
    method: "batchWrite",
    setup: createSeed,
    coverage: { supported: ["RequestItems"] },
    execute: ({ client, tableName }) =>
      client.documentClient
        .batchWrite({
          RequestItems: {
            [tableName]: [{ PutRequest: { Item: { PK: "COVER#BW", SK: "1" } } }],
          },
        })
        .promise(),
  },
  {
    id: "coverage.transactWrite.supported",
    method: "transactWrite",
    setup: createSeed,
    coverage: { supported: ["TransactItems"] },
    execute: ({ client, tableName }) =>
      client.documentClient
        .transactWrite({
          TransactItems: [
            {
              ConditionCheck: {
                TableName: tableName,
                Key: { PK: "USER#1", SK: "PROFILE#001" },
                ConditionExpression: "attribute_exists(PK)",
              },
            },
          ],
        })
        .promise(),
  },
]

const unsupportedParamVectors: Vector[] = [
  ...Object.entries(IN_MEMORY_SPEC.methods).flatMap(([method, spec]) =>
    (spec.unsupportedParams ?? []).map((param) => ({
      id: `unsupported.${method}.${param}`,
      method: method as keyof typeof IN_MEMORY_SPEC.methods,
      coverage: { unsupported: [param] },
      setup:
        method === "get" ||
        method === "put" ||
        method === "update" ||
        method === "delete" ||
        method === "query" ||
        method === "scan" ||
        method === "batchGet" ||
        method === "batchWrite" ||
        method === "transactWrite"
          ? createSeed
          : undefined,
      execute: (ctx: Context) => {
        const baseByMethod: Record<string, any> = {
          get: {
            TableName: ctx.tableName,
            Key: { PK: "USER#1", SK: "PROFILE#001" },
          },
          put: {
            TableName: ctx.tableName,
            Item: { PK: "UNSUPPORTED#PUT", SK: "1" },
          },
          update: {
            TableName: ctx.tableName,
            Key: { PK: "USER#1", SK: "PROFILE#001" },
            UpdateExpression: "SET #name = :name",
            ExpressionAttributeNames: { "#name": "name" },
            ExpressionAttributeValues: { ":name": "x" },
          },
          delete: {
            TableName: ctx.tableName,
            Key: { PK: "USER#1", SK: "PROFILE#001" },
          },
          query: {
            TableName: ctx.tableName,
            KeyConditionExpression: "PK = :pk",
            ExpressionAttributeValues: { ":pk": "USER#1" },
          },
          scan: {
            TableName: ctx.tableName,
          },
          batchGet: {
            RequestItems: {
              [ctx.tableName]: {
                Keys: [{ PK: "USER#1", SK: "PROFILE#001" }],
              },
            },
          },
          batchWrite: {
            RequestItems: {
              [ctx.tableName]: [{ PutRequest: { Item: { PK: "UNSUPPORTED#BW", SK: "1" } } }],
            },
          },
          transactWrite: {
            TransactItems: [
              {
                ConditionCheck: {
                  TableName: ctx.tableName,
                  Key: { PK: "USER#1", SK: "PROFILE#001" },
                  ConditionExpression: "attribute_exists(PK)",
                },
              },
            ],
          },
        }

        const valueByParam: Record<string, any> = {
          AttributesToGet: ["PK"],
          ProjectionExpression: "PK",
          ExpressionAttributeNames: { "#pk": "PK" },
          Expected: {},
          ReturnValues: "ALL_OLD",
          ReturnConsumedCapacity: "TOTAL",
          ReturnItemCollectionMetrics: "SIZE",
          AttributeUpdates: {},
          Select: "ALL_ATTRIBUTES",
          KeyConditions: {},
          QueryFilter: {},
          ConditionalOperator: "AND",
          Segment: 1,
          TotalSegments: 2,
          ScanFilter: {},
          ClientRequestToken: "token-1",
        }

        const params = {
          ...baseByMethod[method],
          [param]: valueByParam[param],
        }

        return (ctx.client.documentClient as any)[method](params).promise()
      },
    }))
  ),
]

const seededFuzzVectors: Vector[] = [11, 42, 99].map((seed) => ({
  id: `fuzz.seed-${seed}`,
  method: "transactWrite",
  execute: async ({ client, tableName }) => {
    const state = {
      seed,
      next: seed,
    }

    const random = () => {
      state.next = (state.next * 48271) % 0x7fffffff
      return state.next / 0x7fffffff
    }

    const results: any[] = []

    for (let i = 0; i < 80; i += 1) {
      const dice = random()
      const user = `F#${Math.floor(random() * 8)}`
      const key = { PK: user, SK: `S#${Math.floor(random() * 5)}` }

      if (dice < 0.25) {
        try {
          await client.documentClient
            .put({
              TableName: tableName,
              Item: {
                ...key,
                GSI2PK: `GX#${key.PK}`,
                GSI2SK: key.SK,
                score: Math.floor(random() * 100),
                status: random() > 0.5 ? "active" : "pending",
              },
            })
            .promise()
          results.push({ op: "put", key, ok: true })
        } catch (error) {
          results.push({ op: "put", key, ok: false, error: normalizeError(error) })
        }
        continue
      }

      if (dice < 0.5) {
        try {
          const mutateGsi = random() > 0.6
          const response = await client.documentClient
            .update({
              TableName: tableName,
              Key: key,
              UpdateExpression: mutateGsi
                ? random() > 0.5
                  ? "SET #score = :score, GSI2PK = :gpk, GSI2SK = :gsk"
                  : "SET #score = :score REMOVE GSI2PK, GSI2SK"
                : "SET #score = :score",
              ExpressionAttributeNames: { "#score": "score" },
              ExpressionAttributeValues: mutateGsi
                ? {
                    ":score": Math.floor(random() * 100),
                    ":gpk": `GX#${key.PK}`,
                    ":gsk": key.SK,
                  }
                : { ":score": Math.floor(random() * 100) },
              ReturnValues: "ALL_NEW",
            })
            .promise()

          results.push({
            op: "update",
            key,
            ok: true,
            attrs: normalizeObject(response.Attributes ?? {}),
          })
        } catch (error) {
          results.push({ op: "update", key, ok: false, error: normalizeError(error) })
        }
        continue
      }

      if (dice < 0.7) {
        try {
          await client.documentClient
            .delete({
              TableName: tableName,
              Key: key,
              ConditionExpression: "attribute_not_exists(blocked)",
            })
            .promise()
          results.push({ op: "delete", key, ok: true })
        } catch (error) {
          results.push({ op: "delete", key, ok: false, error: normalizeError(error) })
        }
        continue
      }

      if (dice < 0.85) {
        try {
          const useGsi = random() > 0.5
          const response = useGsi
            ? await client.documentClient
                .query({
                  TableName: tableName,
                  IndexName: "GSI2",
                  KeyConditionExpression: "GSI2PK = :pk",
                  ExpressionAttributeValues: { ":pk": `GX#${user}` },
                  Limit: 3,
                })
                .promise()
            : await client.documentClient
                .query({
                  TableName: tableName,
                  KeyConditionExpression: "PK = :pk",
                  ExpressionAttributeValues: { ":pk": user },
                  Limit: 3,
                })
                .promise()

          results.push({
            op: useGsi ? "query-gsi" : "query",
            key,
            ok: true,
            count: response.Count,
            scannedCount: response.ScannedCount,
            items: normalizeObject(response.Items ?? []),
          })
        } catch (error) {
          results.push({ op: "query", key, ok: false, error: normalizeError(error) })
        }
        continue
      }

      try {
        await client.documentClient
          .transactWrite({
            TransactItems: [
              {
                Put: {
                  TableName: tableName,
                  Item: {
                    PK: `${user}#tx`,
                    SK: key.SK,
                    score: Math.floor(random() * 100),
                  },
                },
              },
              {
                ConditionCheck: {
                  TableName: tableName,
                  Key: key,
                  ConditionExpression:
                    random() > 0.5 ? "attribute_exists(PK)" : "attribute_not_exists(PK)",
                },
              },
            ],
          })
          .promise()

        results.push({ op: "transactWrite", key, ok: true })
      } catch (error) {
        results.push({
          op: "transactWrite",
          key,
          ok: false,
          error: normalizeError(error),
        })
      }
    }

    return results
  },
}))

describe("dynamodb conformance (local vs in-memory)", () => {
  const differentialVectors = [
    ...generatedVectors,
    ...additionalDifferentialVectors,
    ...supportedParamCoverageVectors,
    ...seededFuzzVectors,
  ]

  test("every supported method in the manifest has differential vector coverage", () => {
    const coveredMethods = new Set(differentialVectors.map((vector) => vector.method))
    const expectedMethods = new Set(Object.keys(IN_MEMORY_SPEC.methods))

    expect(coveredMethods).toEqual(expectedMethods)
  })

  test("supported and unsupported parameter coverage matches the spec", () => {
    const supportedCoverage = new Map<string, Set<string>>()
    const unsupportedCoverage = new Map<string, Set<string>>()

    for (const vector of [...differentialVectors, ...unsupportedParamVectors]) {
      if (!vector.coverage?.supported && !vector.coverage?.unsupported) continue

      const method = vector.method as string

      if (!supportedCoverage.has(method)) supportedCoverage.set(method, new Set())
      if (!unsupportedCoverage.has(method)) unsupportedCoverage.set(method, new Set())

      for (const param of vector.coverage?.supported ?? []) {
        supportedCoverage.get(method)!.add(param)
      }

      for (const param of vector.coverage?.unsupported ?? []) {
        unsupportedCoverage.get(method)!.add(param)
      }
    }

    for (const [method, spec] of Object.entries(IN_MEMORY_SPEC.methods)) {
      expect(supportedCoverage.get(method) ?? new Set()).toEqual(
        new Set(spec.supportedParams)
      )
      expect(unsupportedCoverage.get(method) ?? new Set()).toEqual(
        new Set(spec.unsupportedParams ?? [])
      )
    }
  })

  test.each(differentialVectors)("vector $id", async (vector) => {
    const [local, memory] = await Promise.all([
      runVector("local", vector),
      runVector("memory", vector),
    ])

    expect(memory).toEqual(local)
  })

  test.each(unsupportedParamVectors)(
    "unsupported vector $id throws deterministic NotSupportedError in memory",
    async (vector) => {
      const result = await runVector("memory", vector)
      expect(result.ok).toBe(false)
      if (result.ok) return

      const [methodName, param] = vector.id
        .replace("unsupported.", "")
        .split(".")

      expect(result.error).toEqual({
        code: "NotSupportedError",
        message: `${param} is not supported in in-memory mode`,
        method: methodName,
        featurePath: `${methodName}.${param}`,
        reason: `${param} is not supported in in-memory mode.`,
      })
    }
  )
})
