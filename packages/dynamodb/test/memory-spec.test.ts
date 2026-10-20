import { describe, expect, test } from "vitest"
import { IN_MEMORY_SPEC } from "../src/internal/memory/spec.ts"
import { type Harness, withHarness } from "./harness.ts"

const withMemory = <T>(run: (harness: Harness) => Promise<T>) =>
  withHarness("memory", run)

/** An item shaped like the ones models write: keys, GSI2 and a few fields. */
const item = (id: string, group: string, value: number) => ({
  PK: `PK#${group}`,
  SK: `SK#${String(value).padStart(3, "0")}`,
  GSI2PK: `GSI2PK#${group}`,
  GSI2SK: `GSI2SK#${id}`,
  id,
  group,
  value,
})

const put = (harness: Harness, values: Record<string, unknown>) =>
  harness.client.documentClient
    .put({ TableName: harness.tableName, Item: values })
    .promise()

describe("in-memory spec", () => {
  test("locks projection and index scope", () => {
    expect(IN_MEMORY_SPEC.projection).toBe("ALL")
    expect(IN_MEMORY_SPEC.excludedIndexes).toEqual(["GSI1"])
    expect(IN_MEMORY_SPEC.indexes).toContain("primary")
    expect(IN_MEMORY_SPEC.indexes).toContain("GSI2")
    expect(IN_MEMORY_SPEC.indexes).toContain("GSI19")
    expect(IN_MEMORY_SPEC.indexes).not.toContain("GSI1")
  })

  test("rejects an unsupported operation with NotSupportedError", async () => {
    await withMemory(async ({ client }) => {
      await expect(
        client.documentClient
          .transactGet({
            TransactItems: [
              { Get: { TableName: "t", Key: { PK: "a", SK: "b" } } },
            ],
          })
          .promise(),
      ).rejects.toMatchObject({
        name: "NotSupportedError",
        method: "TransactGetItems",
        featurePath: "TransactGetItems",
      })
    })
  })

  test("rejects GSI1 with NotSupportedError", async () => {
    await withMemory(async (harness) => {
      await put(harness, item("1", "g", 1))

      await expect(
        harness.client.documentClient
          .query({
            TableName: harness.tableName,
            IndexName: "GSI1",
            KeyConditionExpression: "PK = :pk",
            ExpressionAttributeValues: { ":pk": "PK#g" },
          })
          .promise(),
      ).rejects.toMatchObject({
        name: "NotSupportedError",
        method: "query",
        featurePath: "query.IndexName",
      })
    })
  })

  test("keeps GSI membership in sync after removing the index keys", async () => {
    await withMemory(async (harness) => {
      const { documentClient } = harness.client
      const stored = item("42", "test", 42)
      await put(harness, stored)
      const byGroup = {
        TableName: harness.tableName,
        IndexName: "GSI2",
        KeyConditionExpression: "GSI2PK = :pk",
        ExpressionAttributeValues: { ":pk": "GSI2PK#test" },
      }

      expect((await documentClient.query(byGroup).promise()).Count).toBe(1)

      await documentClient
        .update({
          TableName: harness.tableName,
          Key: { PK: stored.PK, SK: stored.SK },
          UpdateExpression: "REMOVE #pk, #sk",
          ExpressionAttributeNames: { "#pk": "GSI2PK", "#sk": "GSI2SK" },
        })
        .promise()

      expect((await documentClient.query(byGroup).promise()).Count).toBe(0)
    })
  })

  test("supports if_not_exists in update expressions", async () => {
    await withMemory(async (harness) => {
      const stored = item("100", "expr", 1)
      await put(harness, stored)
      const increment = (by: number) =>
        harness.client.documentClient
          .update({
            TableName: harness.tableName,
            Key: { PK: stored.PK, SK: stored.SK },
            UpdateExpression:
              "SET #count = if_not_exists(#count, :zero) + :inc",
            ExpressionAttributeNames: { "#count": "count" },
            ExpressionAttributeValues: { ":zero": 0, ":inc": by },
            ReturnValues: "ALL_NEW",
          })
          .promise()

      expect((await increment(2)).Attributes?.count).toBe(2)
      expect((await increment(3)).Attributes?.count).toBe(5)
    })
  })

  test("supports list_append and filter functions", async () => {
    await withMemory(async (harness) => {
      const { documentClient } = harness.client
      const stored = item("200", "expr", 2)
      await put(harness, stored)

      await documentClient
        .update({
          TableName: harness.tableName,
          Key: { PK: stored.PK, SK: stored.SK },
          UpdateExpression:
            "SET #tags = list_append(if_not_exists(#tags, :empty), :more)",
          ExpressionAttributeNames: { "#tags": "tags" },
          ExpressionAttributeValues: { ":empty": [], ":more": ["a", "b"] },
        })
        .promise()

      const filtered = await documentClient
        .scan({
          TableName: harness.tableName,
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
    await withMemory(async (harness) => {
      const { documentClient } = harness.client
      await put(harness, {
        PK: "PK#expr",
        SK: "SK#300",
        value: 3,
        nested: { items: [{ code: "code-1" }] },
      })
      const setValue = (
        next: number,
        condition: string,
        names: Record<string, string> = {},
      ) =>
        documentClient
          .update({
            TableName: harness.tableName,
            Key: { PK: "PK#expr", SK: "SK#300" },
            ConditionExpression: condition,
            UpdateExpression: "SET #value = :next",
            ExpressionAttributeNames: { "#value": "value", ...names },
            ExpressionAttributeValues: { ":code": "code-1", ":next": next },
            ReturnValues: "ALL_NEW",
          })
          .promise()

      expect(
        (await setValue(4, "nested.items[0].code = :code")).Attributes?.value,
      ).toBe(4)
      expect(
        (
          await setValue(5, "#nested.#items[0].#code = :code", {
            "#nested": "nested",
            "#items": "items",
            "#code": "code",
          })
        ).Attributes?.value,
      ).toBe(5)
    })
  })

  test("supports document paths with list indexes in update expressions", async () => {
    await withMemory(async (harness) => {
      const { documentClient } = harness.client
      const Key = { PK: "PK#expr", SK: "SK#301" }
      await put(harness, { ...Key, nested: { items: [{ key: "k1" }] } })

      const set = await documentClient
        .update({
          TableName: harness.tableName,
          Key,
          ConditionExpression:
            "attribute_not_exists(nested.items[0].state) and nested.items[0].key = :key",
          UpdateExpression: "SET nested.items[0].state = :state",
          ExpressionAttributeValues: { ":key": "k1", ":state": "active" },
          ReturnValues: "ALL_NEW",
        })
        .promise()
      expect(set.Attributes?.nested?.items?.[0]?.state).toBe("active")

      const removed = await documentClient
        .update({
          TableName: harness.tableName,
          Key,
          UpdateExpression: "REMOVE nested.items[0].state",
          ReturnValues: "ALL_NEW",
        })
        .promise()
      expect(removed.Attributes?.nested?.items?.[0]?.state).toBeUndefined()
    })
  })

  test("is deterministic across runs", async () => {
    const execute = () =>
      withMemory(async (harness) => {
        const order = [7, 1, 9, 2, 8, 3, 6, 4, 5, 0]
        await Promise.all(
          order.map((value) => put(harness, item(String(value), "det", value))),
        )
        const page = await harness.client.documentClient
          .query({
            TableName: harness.tableName,
            KeyConditionExpression: "PK = :pk",
            ExpressionAttributeValues: { ":pk": "PK#det" },
          })
          .promise()
        const snapshot = await harness.sandbox.snapshot()
        return {
          snapshot: Object.keys(snapshot).sort(),
          queryKeys: (page.Items ?? []).map(
            (entry: { PK: string; SK: string }) => `${entry.PK}::${entry.SK}`,
          ),
        }
      })

    expect(await execute()).toEqual(await execute())
  })

  test("reports every failed index of a cancelled transaction and rolls back the rest", async () => {
    await withMemory(async ({ client, sandbox, tableName: TableName }) => {
      const failure = await client.documentClient
        .transactWrite({
          TransactItems: [
            {
              Delete: {
                TableName,
                Key: { PK: "first", SK: "first" },
                ConditionExpression: "attribute_exists(PK)",
              },
            },
            { Put: { TableName, Item: { PK: "middle", SK: "middle" } } },
            {
              Delete: {
                TableName,
                Key: { PK: "last", SK: "last" },
                ConditionExpression: "attribute_exists(PK)",
              },
            },
          ],
        })
        .promise()
        .catch((error: unknown) => error)

      expect(failure).toMatchObject({
        name: "TransactionCanceledException",
        CancellationReasons: [
          { Code: "ConditionalCheckFailed" },
          { Code: "None" },
          { Code: "ConditionalCheckFailed" },
        ],
      })
      expect(await sandbox.get("middle", "middle")).toBeNull()
    })
  })
})
