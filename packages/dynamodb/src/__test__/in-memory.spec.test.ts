import * as t from "io-ts"
import { model } from "@model-ts/core"
import { Client } from "../client"
import { getProvider } from "../provider"
import { createSandbox, Sandbox } from "../sandbox"
import { NotSupportedError } from "../errors"
import { IN_MEMORY_SPEC } from "../in-memory/spec"

const CODEC = t.type({
  id: t.string,
  group: t.string,
  value: t.number,
})

const withInMemory = async <T>(
  run: (ctx: {
    client: Client
    sandbox: Sandbox
    Item: any
  }) => Promise<T>
): Promise<T> => {
  const previous = process.env.EXPERIMENTAL_DYNAMODB_IN_MEMORY
  process.env.EXPERIMENTAL_DYNAMODB_IN_MEMORY = "1"

  const client = new Client({ tableName: "table" })
  const provider = getProvider(client)

  class Item extends model("Item", CODEC, provider) {
    get PK() {
      return `PK#${this.group}`
    }

    get SK() {
      return `SK#${String(this.value).padStart(3, "0")}`
    }

    get GSI2PK() {
      return `GSI2PK#${this.group}`
    }

    get GSI2SK() {
      return `GSI2SK#${this.id}`
    }
  }

  const sandbox = await createSandbox(client)

  try {
    return await run({ client, sandbox, Item })
  } finally {
    await sandbox.destroy()

    if (typeof previous === "undefined")
      delete process.env.EXPERIMENTAL_DYNAMODB_IN_MEMORY
    else process.env.EXPERIMENTAL_DYNAMODB_IN_MEMORY = previous
  }
}

describe("in-memory spec", () => {
  test("locks projection and index scope", () => {
    expect(IN_MEMORY_SPEC.projection).toBe("ALL")
    expect(IN_MEMORY_SPEC.excludedIndexes).toEqual(["GSI1"])
    expect(IN_MEMORY_SPEC.indexes.includes("primary")).toBe(true)
    expect(IN_MEMORY_SPEC.indexes.includes("GSI2")).toBe(true)
    expect(IN_MEMORY_SPEC.indexes.includes("GSI19")).toBe(true)
    expect(IN_MEMORY_SPEC.indexes.includes("GSI1" as any)).toBe(false)
  })

  test("throws deterministic NotSupportedError for unsupported method", async () => {
    await withInMemory(async ({ client }) => {
      await expect(
        (client.documentClient as any).transactGet({}).promise()
      ).rejects.toMatchObject({
        name: "NotSupportedError",
        code: "NotSupportedError",
        method: "transactGet",
        featurePath: "transactGet",
      })
    })
  })

  test("rejects GSI1 with deterministic NotSupportedError", async () => {
    await withInMemory(async ({ client, Item }) => {
      await new Item({ id: "1", group: "g", value: 1 }).put()

      await expect(
        client.documentClient
          .query({
            TableName: client.tableName,
            IndexName: "GSI1",
            KeyConditionExpression: "PK = :pk",
            ExpressionAttributeValues: { ":pk": "PK#g" },
          })
          .promise()
      ).rejects.toMatchObject({
        name: "NotSupportedError",
        code: "NotSupportedError",
        method: "query",
        featurePath: "query.IndexName",
      })
    })
  })

  test("keeps GSI membership in sync after update/removal", async () => {
    await withInMemory(async ({ client, Item }) => {
      const item = await new Item({ id: "42", group: "test", value: 42 }).put()

      const before = await client.documentClient
        .query({
          TableName: client.tableName,
          IndexName: "GSI2",
          KeyConditionExpression: "GSI2PK = :pk",
          ExpressionAttributeValues: { ":pk": "GSI2PK#test" },
        })
        .promise()

      expect(before.Count).toBe(1)

      await Item.updateRaw(
        { PK: item.PK, SK: item.SK },
        { GSI2PK: null, GSI2SK: null } as any
      )

      const after = await client.documentClient
        .query({
          TableName: client.tableName,
          IndexName: "GSI2",
          KeyConditionExpression: "GSI2PK = :pk",
          ExpressionAttributeValues: { ":pk": "GSI2PK#test" },
        })
        .promise()

      expect(after.Count).toBe(0)
    })
  })

  test("supports if_not_exists in update expressions", async () => {
    await withInMemory(async ({ client, Item }) => {
      const item = await new Item({ id: "100", group: "expr", value: 1 }).put()

      const first = await client.documentClient
        .update({
          TableName: client.tableName,
          Key: { PK: item.PK, SK: item.SK },
          UpdateExpression: "SET #count = if_not_exists(#count, :zero) + :inc",
          ExpressionAttributeNames: { "#count": "count" },
          ExpressionAttributeValues: { ":zero": 0, ":inc": 2 },
          ReturnValues: "ALL_NEW",
        })
        .promise()

      expect(first.Attributes?.count).toBe(2)

      const second = await client.documentClient
        .update({
          TableName: client.tableName,
          Key: { PK: item.PK, SK: item.SK },
          UpdateExpression: "SET #count = if_not_exists(#count, :zero) + :inc",
          ExpressionAttributeNames: { "#count": "count" },
          ExpressionAttributeValues: { ":zero": 0, ":inc": 3 },
          ReturnValues: "ALL_NEW",
        })
        .promise()

      expect(second.Attributes?.count).toBe(5)
    })
  })

  test("supports list_append and filter functions", async () => {
    await withInMemory(async ({ client, Item }) => {
      const item = await new Item({ id: "200", group: "expr", value: 2 }).put()

      await client.documentClient
        .update({
          TableName: client.tableName,
          Key: { PK: item.PK, SK: item.SK },
          UpdateExpression:
            "SET #tags = list_append(if_not_exists(#tags, :empty), :more)",
          ExpressionAttributeNames: { "#tags": "tags" },
          ExpressionAttributeValues: { ":empty": [], ":more": ["a", "b"] },
          ReturnValues: "ALL_NEW",
        })
        .promise()

      const filtered = await client.documentClient
        .scan({
          TableName: client.tableName,
          FilterExpression:
            "contains(#tags, :tag) and attribute_type(#id, :t) and size(#id) > :min",
          ExpressionAttributeNames: { "#tags": "tags", "#id": "id" },
          ExpressionAttributeValues: { ":tag": "a", ":t": "S", ":min": 1 },
        })
        .promise()

      expect(filtered.Count).toBe(1)
      expect(filtered.Items?.[0]?.id).toBe("200")
    })
  })

  test("supports document paths with list indexes in condition expressions", async () => {
    await withInMemory(async ({ client }) => {
      await client.documentClient
        .put({
          TableName: client.tableName,
          Item: {
            PK: "PK#expr",
            SK: "SK#300",
            id: "300",
            group: "expr",
            value: 3,
            nested: {
              items: [{ code: "code-1" }],
            },
          },
        })
        .promise()

      const updated = await client.documentClient
        .update({
          TableName: client.tableName,
          Key: { PK: "PK#expr", SK: "SK#300" },
          ConditionExpression: "nested.items[0].code = :code",
          UpdateExpression: "SET #value = :next",
          ExpressionAttributeNames: { "#value": "value" },
          ExpressionAttributeValues: { ":code": "code-1", ":next": 4 },
          ReturnValues: "ALL_NEW",
        })
        .promise()

      expect(updated.Attributes?.value).toBe(4)

      const updatedWithNames = await client.documentClient
        .update({
          TableName: client.tableName,
          Key: { PK: "PK#expr", SK: "SK#300" },
          ConditionExpression: "#nested.#items[0].#code = :code",
          UpdateExpression: "SET #value = :next",
          ExpressionAttributeNames: {
            "#value": "value",
            "#nested": "nested",
            "#items": "items",
            "#code": "code",
          },
          ExpressionAttributeValues: { ":code": "code-1", ":next": 5 },
          ReturnValues: "ALL_NEW",
        })
        .promise()

      expect(updatedWithNames.Attributes?.value).toBe(5)
    })
  })

  test("supports document paths with list indexes in update expressions", async () => {
    await withInMemory(async ({ client }) => {
      await client.documentClient
        .put({
          TableName: client.tableName,
          Item: {
            PK: "PK#expr",
            SK: "SK#301",
            id: "301",
            group: "expr",
            value: 3,
            nested: {
              items: [{ key: "k1" }],
            },
          },
        })
        .promise()

      const withInbox = await client.documentClient
        .update({
          TableName: client.tableName,
          Key: { PK: "PK#expr", SK: "SK#301" },
          ConditionExpression:
            "attribute_not_exists(nested.items[0].state) and nested.items[0].key = :key",
          UpdateExpression: "SET nested.items[0].state = :state",
          ExpressionAttributeValues: { ":key": "k1", ":state": "active" },
          ReturnValues: "ALL_NEW",
        })
        .promise()

      expect(withInbox.Attributes?.nested?.items?.[0]?.state).toBe("active")

      const removed = await client.documentClient
        .update({
          TableName: client.tableName,
          Key: { PK: "PK#expr", SK: "SK#301" },
          UpdateExpression: "REMOVE nested.items[0].state",
          ReturnValues: "ALL_NEW",
        })
        .promise()

      expect(removed.Attributes?.nested?.items?.[0]?.state).toBeUndefined()
    })
  })

  test("is deterministic for repeated seeded runs", async () => {
    const execute = async () =>
      withInMemory(async ({ client, sandbox, Item }) => {
        const order = [7, 1, 9, 2, 8, 3, 6, 4, 5, 0]

        await Promise.all(
          order.map((value) =>
            new Item({ id: String(value), group: "det", value }).put()
          )
        )

        const page = await client.documentClient
          .query({
            TableName: client.tableName,
            KeyConditionExpression: "PK = :pk",
            ExpressionAttributeValues: { ":pk": "PK#det" },
          })
          .promise()

        return {
          snapshot: await sandbox.snapshot(),
          queryKeys: (page.Items ?? []).map((entry) => `${entry.PK}::${entry.SK}`),
        }
      })

    const first = await execute()
    const second = await execute()

    expect(second).toEqual(first)
  })
})
