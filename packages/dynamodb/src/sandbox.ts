import crypto from "crypto"
import { chunksOf } from "fp-ts/lib/Array"
import DynamoDB from "aws-sdk/clients/dynamodb"
import { formatSnapshotDiff } from "./diff"
import { Client } from "./client"
import { GSI_NAMES } from "./gsi"
import { createInMemoryDocumentClient } from "./in-memory"

const ddb = new DynamoDB({
  accessKeyId: "xxx",
  secretAccessKey: "xxx",
  endpoint: process.env.LOCAL_ENDPOINT,
  region: "local",
})

const docClient = new DynamoDB.DocumentClient({
  accessKeyId: "xxx",
  secretAccessKey: "xxx",
  endpoint: process.env.LOCAL_ENDPOINT,
  region: "local",
})

export const createTable = async () => {
  const tableName = crypto.randomBytes(20).toString("hex")

  await ddb
    .createTable({
      TableName: tableName,
      AttributeDefinitions: [
        { AttributeName: "PK", AttributeType: "S" },
        { AttributeName: "SK", AttributeType: "S" },
        ...GSI_NAMES.flatMap((GSI) => [
          { AttributeName: `${GSI}PK`, AttributeType: "S" },
          { AttributeName: `${GSI}SK`, AttributeType: "S" },
        ]),
      ],
      KeySchema: [
        { AttributeName: "PK", KeyType: "HASH" },
        { AttributeName: "SK", KeyType: "RANGE" },
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: "GSI1",
          KeySchema: [
            { AttributeName: "SK", KeyType: "HASH" },
            { AttributeName: "PK", KeyType: "RANGE" },
          ],
          Projection: {
            ProjectionType: "ALL",
          },
        },
        ...GSI_NAMES.map((GSI) => ({
          IndexName: GSI,
          KeySchema: [
            { AttributeName: `${GSI}PK`, KeyType: "HASH" },
            { AttributeName: `${GSI}SK`, KeyType: "RANGE" },
          ],
          Projection: {
            ProjectionType: "ALL",
          },
        })),
      ],
      BillingMode: "PAY_PER_REQUEST",
    })
    .promise()
    .catch((e: any) => {
      console.log("Failed to create table, exiting.", e)
      process.exit(1)
    })

  return tableName
}

export const destroyTable = async (tableName: string) => {
  return ddb
    .deleteTable({ TableName: tableName })
    .promise()
    .then(() => {})
    .catch((e) => {
      console.log("Failed to destroy table, exiting.", e)
      process.exit(1)
    })
}

export const getTableContents = async (
  tableName: string
): Promise<{ [key: string]: any }> => {
  const scan = async (ExclusiveStartKey?: any): Promise<any[]> => {
    const { Items = [], LastEvaluatedKey } = await docClient
      .scan({
        TableName: tableName,
        ExclusiveStartKey,
      })
      .promise()

    if (LastEvaluatedKey) return [...Items, ...(await scan(LastEvaluatedKey))]
    return Items
  }

  const items = await scan()

  const acc: { [key: string]: any } = {}

  items.forEach((item) => (acc[`${item.PK}__${item.SK}`] = item))

  return acc
}

// -------------------------------------------------------------------------------------
// Tracking
// -------------------------------------------------------------------------------------

interface TrackedEntry {
  pk: string
  sk: string
  original: any | null
}

const WRITE_METHODS = new Set([
  "put",
  "update",
  "delete",
  "batchWrite",
  "transactWrite",
])

function createTrackedDocClient(
  original: DynamoDB.DocumentClient,
  tableName: string
) {
  let isTracking = false
  const trackedKeys = new Map<string, TrackedEntry>()

  const captureKey = async (pk: string, sk: string) => {
    const compositeKey = `${pk}__${sk}`
    if (trackedKeys.has(compositeKey)) return

    const { Item } = await original
      .get({ TableName: tableName, Key: { PK: pk, SK: sk } })
      .promise()

    trackedKeys.set(compositeKey, { pk, sk, original: Item ?? null })
  }

  const captureKeysForOperation = async (method: string, params: any) => {
    switch (method) {
      case "put":
        if (params.TableName === tableName && params.Item) {
          await captureKey(params.Item.PK, params.Item.SK)
        }
        break
      case "update":
      case "delete":
        if (params.TableName === tableName && params.Key) {
          await captureKey(params.Key.PK, params.Key.SK)
        }
        break
      case "batchWrite": {
        const tableItems = params.RequestItems?.[tableName] || []
        await Promise.all(
          tableItems.map((item: any) => {
            if (item.PutRequest) {
              return captureKey(
                item.PutRequest.Item.PK,
                item.PutRequest.Item.SK
              )
            }
            if (item.DeleteRequest) {
              return captureKey(
                item.DeleteRequest.Key.PK,
                item.DeleteRequest.Key.SK
              )
            }
          })
        )
        break
      }
      case "transactWrite": {
        const transactItems = params.TransactItems || []
        await Promise.all(
          transactItems
            .map((item: any) => {
              if (item.Put?.TableName === tableName) {
                return captureKey(item.Put.Item.PK, item.Put.Item.SK)
              }
              if (item.Update?.TableName === tableName) {
                return captureKey(item.Update.Key.PK, item.Update.Key.SK)
              }
              if (item.Delete?.TableName === tableName) {
                return captureKey(item.Delete.Key.PK, item.Delete.Key.SK)
              }
            })
            .filter(Boolean)
        )
        break
      }
    }
  }

  const proxy = new Proxy(original, {
    get(target, prop) {
      const value = (target as any)[prop]
      if (value === undefined) return undefined

      if (typeof value === "function") {
        if (isTracking && WRITE_METHODS.has(prop as string)) {
          return (params: any) => {
            const request = value.call(target, params)
            const origPromise = request.promise.bind(request)
            request.promise = async () => {
              await captureKeysForOperation(prop as string, params)
              return origPromise()
            }
            return request
          }
        }
        return value.bind(target)
      }

      return value
    },
  })

  return {
    proxy: proxy as DynamoDB.DocumentClient,
    startTracking: () => {
      isTracking = true
      trackedKeys.clear()
    },
    rollback: async () => {
      isTracking = false

      const entries = Array.from(trackedKeys.values())
      const toDelete = entries.filter((e) => e.original === null)
      const toRestore = entries.filter((e) => e.original !== null)

      const deleteChunks = chunksOf(25)(toDelete)
      const restoreChunks = chunksOf(25)(toRestore)

      await Promise.all([
        ...deleteChunks.map((chunk) =>
          original
            .batchWrite({
              RequestItems: {
                [tableName]: chunk.map(({ pk, sk }) => ({
                  DeleteRequest: { Key: { PK: pk, SK: sk } },
                })),
              },
            })
            .promise()
        ),
        ...restoreChunks.map((chunk) =>
          original
            .batchWrite({
              RequestItems: {
                [tableName]: chunk.map(({ original: item }) => ({
                  PutRequest: { Item: item },
                })),
              },
            })
            .promise()
        ),
      ])

      trackedKeys.clear()
    },
  }
}

// -------------------------------------------------------------------------------------
// Sandbox
// -------------------------------------------------------------------------------------

export interface Sandbox {
  destroy: () => Promise<void>
  snapshot: () => Promise<{ [key: string]: any }>
  seed: (...args: Array<{ [key: string]: any }>) => Promise<void>
  get: (pk: string, sk: string) => Promise<null | any>
  diff: (before: { [key: string]: any }) => Promise<string>
  startTracking: () => void
  rollback: () => Promise<void>
}

export const createSandbox = async (client: Client): Promise<Sandbox> => {
  if (process.env.EXPERIMENTAL_DYNAMODB_IN_MEMORY === "1") {
    const tableName = crypto.randomBytes(20).toString("hex")
    const inMemoryClient =
      createInMemoryDocumentClient() as any as DynamoDB.DocumentClient & {
        __inMemorySnapshot: (name: string) => { [key: string]: any }
        __inMemoryResetTable: (name: string) => void
      }

    const tracked = createTrackedDocClient(inMemoryClient, tableName)

    client.setDocumentClient(tracked.proxy)
    client.setTableName(tableName)

    return {
      destroy: async () => {
        inMemoryClient.__inMemoryResetTable(tableName)
      },
      snapshot: async () => inMemoryClient.__inMemorySnapshot(tableName),
      seed: async (...args: Array<{ [key: string]: any }>) => {
        const chunks = chunksOf(25)(args)

        await Promise.all(
          chunks.map(async (chunk) => {
            const items = chunk.map((i) =>
              typeof i?._model?.__dynamoDBEncode === "function"
                ? i._model.__dynamoDBEncode(i)
                : typeof i.encode === "function"
                ? i.encode()
                : i
            )

            return client.documentClient
              .batchWrite({
                RequestItems: {
                  [tableName]: items.map((i) => ({ PutRequest: { Item: i } })),
                },
              })
              .promise()
          })
        )
      },
      get: (pk: string, sk: string) =>
        client.documentClient
          .get({ TableName: tableName, Key: { PK: pk, SK: sk } })
          .promise()
          .then(({ Item }) => Item ?? null),
      diff: async (before) => {
        const snapshot = inMemoryClient.__inMemorySnapshot(tableName)
        return formatSnapshotDiff(before, snapshot)
      },
      startTracking: tracked.startTracking,
      rollback: tracked.rollback,
    }
  }

  const tableName = await createTable()

  const tracked = createTrackedDocClient(docClient, tableName)
  client.setDocumentClient(tracked.proxy)
  client.setTableName(tableName)

  return {
    destroy: () => destroyTable(tableName),
    snapshot: () => getTableContents(tableName),
    seed: async (...args: Array<{ [key: string]: any }>) => {
      const chunks = chunksOf(25)(args)

      await Promise.all(
        chunks.map(async (chunk) => {
          const items = chunk.map((i) =>
            typeof i?._model?.__dynamoDBEncode === "function"
              ? i._model.__dynamoDBEncode(i)
              : typeof i.encode === "function"
              ? i.encode()
              : i
          )

          return client.documentClient
            .batchWrite({
              RequestItems: {
                [tableName]: items.map((i) => ({ PutRequest: { Item: i } })),
              },
            })
            .promise()
        })
      )
    },
    get: (pk: string, sk: string) =>
      client.documentClient
        .get({ TableName: tableName, Key: { PK: pk, SK: sk } })
        .promise()
        .then(({ Item }) => Item ?? null),
    diff: async (before) => {
      const snapshot = await getTableContents(tableName)

      return formatSnapshotDiff(before, snapshot)
    },
    startTracking: tracked.startTracking,
    rollback: tracked.rollback,
  }
}
