import * as t from "io-ts"
import { model } from "@model-ts/core"
import { Client } from "../client"
import { getProvider } from "../provider"
import { Sandbox } from "../sandbox"
import { DeleteOptions } from "../operations"
import { RaceConditionError, BulkWriteTransactionError } from "../errors"
import { itemDelete } from "../delete"

// Registered by both client suites; their existing sandbox hooks own isolation.
export function versionedDeleteTests(client: Client, sandbox: () => Sandbox) {
  class Item extends model(
    "VersionedDelete",
    t.type({ id: t.string, value: t.number }),
    getProvider(client)
  ) {
    get PK() {
      return `VERSIONED#${this.id}`
    }
    get SK() {
      return this.PK
    }
  }
  // Compile-only fixtures, checked by TS 4.5 in both suites; never invoke writes.
  const checkTypes = (item: Item) => {
    const hard: Promise<null> = item.delete({ ignoreVersion: true })
    const soft: Promise<Item> = item.softDelete()
    const staticSoft: Promise<Item> = Item.softDelete(item)
    const clientSoft: Promise<Item> = client.softDelete(item)
    // @ts-expect-error Versions are derived from snapshots, not public options.
    item.delete({ expectedVersion: 0 })
    // @ts-expect-error Key-only APIs accept raw conditions, not instance options.
    Item.delete(item.keys(), { ignoreVersion: true })
    client.delete({
      _model: Item,
      _operation: "delete",
      key: item.keys(),
      // @ts-expect-error Raw delete operations do not expose a version parameter.
      expectedVersion: 0,
    })
    return [hard, soft, staticSoft, clientSoft]
  }
  void checkTypes
  const create = async (id = "item") => {
    const item = await new Item({ id, value: 0 }).put()
    return Item.load(item.keys())
  }
  const archived = (item: Item) =>
    sandbox().get(`$$DELETED$$${item.PK}`, `$$DELETED$$${item.SK}`)
  const routes: {
    name: string
    soft: boolean
    bulk?: boolean
    run: (item: Item, options?: DeleteOptions) => Promise<unknown>
  }[] = [
    {
      name: "instance delete",
      soft: false,
      run: (item, options) => item.delete(options),
    },
    {
      name: "instance softDelete",
      soft: true,
      run: (item, options) => item.softDelete(options),
    },
    {
      name: "model softDelete",
      soft: true,
      run: (item, options) => Item.softDelete(item, options),
    },
    {
      name: "client softDelete",
      soft: true,
      run: (item, options) => client.softDelete(item, options),
    },
    {
      name: "instance delete builder",
      soft: false,
      bulk: true,
      run: (item, options) => client.bulk([item.operation("delete", options)]),
    },
    {
      name: "instance softDelete builder",
      soft: true,
      bulk: true,
      run: (item, options) =>
        client.bulk([item.operation("softDelete", options)]),
    },
    {
      name: "model softDelete builder",
      soft: true,
      bulk: true,
      run: (item, options) =>
        client.bulk([Item.operation("softDelete", item, options)]),
    },
  ]

  describe("versioned deletes", () => {
    let originalClient = client.documentClient
    beforeEach(() => {
      originalClient = client.documentClient
      // Sandbox proxies bind methods; give Jest an own method to spy on.
      client.setDocumentClient(
        Object.assign(Object.create(originalClient), {
          transactWrite: originalClient.transactWrite.bind(originalClient),
        })
      )
    })
    afterEach(() => {
      jest.restoreAllMocks()
      client.setDocumentClient(originalClient)
    })

    test.each(routes)(
      "$name guards stale snapshots and supports ignoreVersion",
      async ({ run, soft, bulk }) => {
        const item = await create()
        const concurrent = await Item.load(item.keys())
        await concurrent.update({ value: 1 })
        const error = bulk ? BulkWriteTransactionError : RaceConditionError
        await expect(run(item)).rejects.toBeInstanceOf(error)
        await expect(
          run(item, { ignoreVersion: false })
        ).rejects.toBeInstanceOf(error)
        expect(await sandbox().get(item.PK, item.SK)).toMatchObject({
          value: 1,
          _docVersion: 1,
        })
        expect(await archived(item)).toBeNull()
        await run(item, { ignoreVersion: true })
        expect(await sandbox().get(item.PK, item.SK)).toBeNull()
        if (soft) expect(await archived(item)).toMatchObject({ value: 0 })

        const fresh = await create("fresh")
        const result = await run(fresh)
        if (!bulk) expect(result).toBe(soft ? fresh : null)
        expect(await sandbox().get(fresh.PK, fresh.SK)).toBeNull()
      }
    )

    test.each(["delete", "softDelete"] as const)(
      "%s guards legacy zero, newer and absent rows",
      async (method) => {
        for (const stored of [undefined, 0, 1]) {
          const seed = new Item({ id: `legacy-${stored}`, value: 0 })
          const raw = { ...seed.encode(), ...seed.keys() }
          await client.documentClient
            .put({ TableName: client.tableName, Item: raw })
            .promise()
          const loaded = await Item.load(seed.keys()) // missing metadata decodes to zero
          if (stored !== undefined) {
            await client.documentClient
              .put({
                TableName: client.tableName,
                Item: { ...raw, _docVersion: stored },
              })
              .promise()
          }
          if (stored === 1) {
            await expect(loaded[method]()).rejects.toBeInstanceOf(
              RaceConditionError
            )
            expect(await archived(loaded)).toBeNull()
          } else {
            await loaded[method]()
            expect(await sandbox().get(seed.PK, seed.SK)).toBeNull()
          }
        }
        const item = await (await create("nonzero")).update({ value: 1 })
        const raw = { ...item.encode(), ...item.keys() }
        await client.documentClient
          .put({ TableName: client.tableName, Item: raw })
          .promise()
        await expect(item[method]()).rejects.toBeInstanceOf(RaceConditionError)
        for (const snapshot of [item, await create("missing-zero")]) {
          await Item.delete(snapshot.keys())
          await expect(snapshot[method]()).rejects.toBeInstanceOf(
            RaceConditionError
          )
          expect(await archived(snapshot)).toBeNull()
        }
      }
    )

    test("key-only deletion stays unconditional; raw conditions survive both request paths", async () => {
      const item = await create()
      await item.update({ value: 1 })
      const params = {
        ConditionExpression: "#value = :value",
        ExpressionAttributeNames: { "#value": "value" },
        ExpressionAttributeValues: { ":value": 0 },
      }
      await expect(Item.delete(item.keys(), params)).rejects.toMatchObject({
        code: "ConditionalCheckFailedException",
      })
      await expect(
        client.bulk([Item.operation("delete", item.keys(), params)])
      ).rejects.toBeInstanceOf(BulkWriteTransactionError)
      await client.delete(
        Item.operation("delete", item.keys(), {
          ...params,
          ExpressionAttributeValues: { ":value": 1 },
        })
      )
      await expect(Item.delete(item.keys())).resolves.toBeNull()
      const other = await create("static-bulk")
      await client.bulk([Item.operation("delete", other.keys())])
      expect(await sandbox().get(other.PK, other.SK)).toBeNull()
    })

    test("manual instances retain the unversioned fallback", async () => {
      const saved = await create()
      await saved.update({ value: 1 })
      await new Item({ id: saved.id, value: 0 }).delete()
      const other = await create("manual-soft")
      await other.update({ value: 1 })
      await client.softDelete(new Item({ id: other.id, value: 0 }))
      expect(await archived(other)).toMatchObject({ value: 0 })
    })

    test("builders capture versions and direct client keeps raw conditional errors", async () => {
      const item = await create()
      const hard = item.operation("delete")
      const soft = item.operation("softDelete")
      // These accesses also check the corrected tuple declaration with TS 4.5.
      expect(soft[0].action.key).toEqual(item.keys())
      expect(soft[1].action.item).toBe(item)
      expect(soft[1].rollback.ConditionExpression).toBeUndefined()
      await item.update({ value: 1 })
      Object.assign(item, { _docVersion: 1 })
      await expect(client.delete(hard)).rejects.toMatchObject({
        code: "ConditionalCheckFailedException",
      })
      const unrelated = new Item({ id: "unrelated", value: 0 })
      await expect(
        client.bulk([unrelated.operation("put"), soft])
      ).rejects.toBeInstanceOf(BulkWriteTransactionError)
      expect(await sandbox().get(unrelated.PK, unrelated.SK)).toBeNull()
      expect(await archived(item)).toBeNull()
    })

    test("soft delete puts the guard on Delete and retains archive collision protection", async () => {
      const item = await create()
      const write = jest.spyOn(client.documentClient, "transactWrite")
      await item.softDelete()
      const actions = write.mock.calls[0][0].TransactItems!
      expect(actions).toHaveLength(2)
      expect(actions[0].Delete?.ConditionExpression).toContain(
        "attribute_exists(PK)"
      )
      expect(actions[1].Put?.Item?.PK).toBe(`$$DELETED$$${item.PK}`)
      expect(actions.every((action) => !action.ConditionCheck)).toBe(true)
      const replacement = await create()
      await expect(replacement.softDelete()).rejects.toBeInstanceOf(
        BulkWriteTransactionError
      )
      await expect(
        replacement.softDelete({ ignoreVersion: true })
      ).rejects.toBeInstanceOf(BulkWriteTransactionError)
      await replacement.update({ value: 1 })
      await expect(replacement.softDelete()).rejects.toBeInstanceOf(
        BulkWriteTransactionError
      ) // both predicates fail
      expect(await sandbox().get(item.PK, item.SK)).toMatchObject({ value: 1 })
      expect(await archived(item)).toMatchObject({ value: 0 })
    })

    test("internal condition composition preserves OR precedence and colliding aliases", async () => {
      const item = await create()
      const op = itemDelete(Item, item, undefined, {
        ConditionExpression:
          "#docVersion = :docVersion OR #docVersion = :other",
        ExpressionAttributeNames: { "#docVersion": "value" },
        ExpressionAttributeValues: { ":docVersion": 1, ":other": 0 },
      })
      await item.update({ value: 1 })
      await expect(client.delete(op)).rejects.toMatchObject({
        code: "ConditionalCheckFailedException",
      })
      await client.delete(
        itemDelete(Item, await Item.load(item.keys()), undefined, {
          ConditionExpression:
            "#docVersion = :docVersion OR #docVersion = :other",
          ExpressionAttributeNames: { "#docVersion": "value" },
          ExpressionAttributeValues: { ":docVersion": 1, ":other": 0 },
        })
      )
    })

    test.each(
      [
        undefined,
        [],
        [{ Code: "ConditionalCheckFailed" }],
        [{ Code: "None" }, { Code: "ConditionalCheckFailed" }],
        [
          { Code: "ConditionalCheckFailed" },
          { Code: "ConditionalCheckFailed" },
        ],
        [{ Code: "ConditionalCheckFailed" }, { Code: "ValidationError" }],
        [null, { Code: "None" }],
      ].map((reasons) => ({ reasons }))
    )(
      "does not infer a soft-delete race from ambiguous reasons %#",
      async ({ reasons }) => {
        const item = await create()
        const write = jest
          .spyOn(client.documentClient, "transactWrite")
          .mockImplementation(() => ({
            promise: async () => {
              throw Object.assign(new Error("cancelled"), {
                code: "TransactionCanceledException",
                CancellationReasons: reasons,
              })
            },
          }))
        await expect(item.softDelete()).rejects.toBeInstanceOf(
          BulkWriteTransactionError
        )
        expect(write).toHaveBeenCalledTimes(1)
      }
    )

    test("direct soft-delete mapping is guarded and does not alter generic bulk errors", async () => {
      const item = await create()
      const write = jest
        .spyOn(client.documentClient, "transactWrite")
        .mockImplementation(() => ({
          promise: async () => {
            throw Object.assign(new Error("cancelled"), {
              code: "TransactionCanceledException",
              CancellationReasons: [
                { Code: "ConditionalCheckFailed" },
                { Code: "None" },
              ],
            })
          },
        }))
      await expect(item.softDelete()).rejects.toBeInstanceOf(RaceConditionError)
      await expect(
        item.softDelete({ ignoreVersion: true })
      ).rejects.toBeInstanceOf(BulkWriteTransactionError)
      await expect(
        client.bulk([item.operation("softDelete")])
      ).rejects.toBeInstanceOf(BulkWriteTransactionError)
      expect(write).toHaveBeenCalledTimes(3)
    })
  })
}
