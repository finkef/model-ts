import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import * as t from "io-ts"
import { model, union } from "@model-ts/core"
import { Client } from "../client"
import {
  BulkWriteTransactionError,
  ConditionalCheckFailedError,
  DynamoDB,
  DynamoDBClientError,
  getEffectProvider,
  ItemNotFoundError,
  KeyExistsError,
  PaginationError,
  RaceConditionError,
  RuntimeTypeValidationError,
} from "../effect"
import { Sandbox, createSandbox } from "../sandbox"

const client = new Client({ tableName: "table" })
const provider = getEffectProvider(client)

class Simple extends model(
  "EffectSimple",
  t.type({ foo: t.string, bar: t.number }),
  provider
) {
  get PK() {
    return `PK#${this.foo}`
  }

  get SK() {
    return `SK#${this.bar}`
  }
}

class InPlace extends model(
  "EffectInPlace",
  t.type({ foo: t.string, bar: t.number }),
  provider
) {
  get PK() {
    return "FIXED#PK"
  }

  get SK() {
    return "FIXED#SK"
  }
}

class A extends model(
  "EffectA",
  t.type({ pk: t.string, sk: t.string, value: t.number }),
  provider
) {
  get PK() {
    return this.pk
  }

  get SK() {
    return this.sk
  }
}

class B extends model(
  "EffectB",
  t.type({ pk: t.string, sk: t.string, name: t.string }),
  provider
) {
  get PK() {
    return this.pk
  }

  get SK() {
    return this.sk
  }
}

class Union extends union([A, B], provider) {}

let sandbox: Sandbox

beforeEach(async () => {
  sandbox = await createSandbox(client)
})

afterEach(async () => {
  await sandbox.destroy()
})

const queryParams = {
  KeyConditionExpression: "PK = :pk and begins_with(SK, :sk)",
  ExpressionAttributeValues: { ":pk": "query", ":sk": "item#" },
}

type IsExact<T, U> = (<G>() => G extends T ? 1 : 2) extends <G>() => G extends U
  ? 1
  : 2
  ? true
  : false

type ErrorOf<T> = T extends Effect.Effect<any, infer E, any> ? E : never

const nullLoad = Simple.load(
  { PK: "PK#compile-time", SK: "SK#compile-time" },
  { null: true }
)
const nullLoadErrorType: IsExact<
  ErrorOf<typeof nullLoad>,
  RuntimeTypeValidationError | DynamoDBClientError
> = true

const instanceUpdate = new Simple({ foo: "compile-time", bar: 1 }).update({
  foo: "updated",
})
const instanceUpdateErrorType: IsExact<
  ErrorOf<typeof instanceUpdate>,
  | RaceConditionError
  | BulkWriteTransactionError
  | import("../effect").BulkWriteRollbackError
  | DynamoDBClientError
> = true

void nullLoadErrorType
void instanceUpdateErrorType

describe("Effect provider", () => {
  test("is lazy, returns model instances, and exposes tagged domain errors", async () => {
    const item = new Simple({ foo: "lazy", bar: 1 })
    const put = jest.spyOn(client, "put")

    const program = item.put()
    expect(put).not.toHaveBeenCalled()

    await Effect.runPromise(program)
    expect(put).toHaveBeenCalledTimes(1)

    const loaded = await Effect.runPromise(Simple.get(item.keys()))
    expect(loaded).toBeInstanceOf(Simple)
    expect(loaded.foo).toBe("lazy")

    const duplicate = await Effect.runPromise(
      Effect.flip(new Simple({ foo: "lazy", bar: 1 }).put())
    )
    expect(duplicate).toBeInstanceOf(KeyExistsError)
    expect(duplicate._tag).toBe("KeyExistsError")

    const recovered = await Effect.runPromise(
      new Simple({ foo: "lazy", bar: 1 })
        .put()
        .pipe(Effect.catchTag("KeyExistsError", () => Effect.succeed("exists")))
    )
    expect(recovered).toBe("exists")
  })

  test("preserves load null and soft-delete recovery semantics", async () => {
    const key = { PK: "PK#missing", SK: "SK#1" }
    const missing = await Effect.runPromise(Effect.flip(Simple.get(key)))
    expect(missing).toBeInstanceOf(ItemNotFoundError)
    expect(await Effect.runPromise(Simple.load(key, { null: true }))).toBeNull()

    const item = new Simple({ foo: "recover", bar: 2 })
    await Effect.runPromise(item.put())
    await Effect.runPromise(item.softDelete())

    const recovered = await Effect.runPromise(
      Simple.load(item.keys(), { recover: true })
    )
    expect(recovered.isDeleted).toBe(true)
  })

  test("keeps update race detection and key-change bulk updates", async () => {
    const stale = await Effect.runPromise(
      new InPlace({ foo: "before", bar: 1 }).put()
    )
    await Effect.runPromise(stale.update({ foo: "first" }))

    const race = await Effect.runPromise(
      Effect.flip(stale.update({ foo: "stale" }))
    )
    expect(race).toBeInstanceOf(RaceConditionError)
    expect(race._tag).toBe("RaceConditionError")

    const item = await Effect.runPromise(
      new Simple({ foo: "old", bar: 3 }).put()
    )
    const updated = await Effect.runPromise(item.update({ foo: "new" }))
    await expect(
      Effect.runPromise(Simple.get({ PK: "PK#old", SK: "SK#3" }))
    ).rejects.toBeInstanceOf(ItemNotFoundError)
    expect(updated.foo).toBe("new")
  })

  test("updates raw values after removing undefined attributes and reports conditions", async () => {
    const item = await Effect.runPromise(
      new Simple({ foo: "raw", bar: 1 }).put()
    )
    const updateRaw = jest.spyOn(client, "updateRaw")

    const updated = await Effect.runPromise(
      Simple.updateRaw(item.keys(), { bar: 2, foo: undefined })
    )
    expect(updated.bar).toBe(2)
    expect(updateRaw).toHaveBeenCalledWith(
      expect.objectContaining({ attributes: { bar: 2 } })
    )

    const conditional = await Effect.runPromise(
      Effect.flip(
        Simple.updateRaw(
          { PK: "missing", SK: "missing" },
          { bar: 2 },
          { ConditionExpression: "attribute_exists(PK)" }
        )
      )
    )
    expect(conditional).toBeInstanceOf(ConditionalCheckFailedError)
  })

  test("supports deletion and retains soft-deleted items at prefixed keys", async () => {
    const deleted = await Effect.runPromise(
      new Simple({ foo: "deleted", bar: 1 }).put()
    )
    await Effect.runPromise(deleted.softDelete())

    expect(await sandbox.get(deleted.PK, deleted.SK)).toBeNull()
    expect(
      await sandbox.get(`$$DELETED$$${deleted.PK}`, `$$DELETED$$${deleted.SK}`)
    ).toMatchObject({ foo: "deleted", bar: 1 })

    const removable = await Effect.runPromise(
      new Simple({ foo: "remove", bar: 2 }).put()
    )
    await Effect.runPromise(removable.delete())
    expect(await sandbox.get(removable.PK, removable.SK)).toBeNull()
  })

  test("returns load-many item errors and runtime codec failures", async () => {
    const item = await Effect.runPromise(
      new Simple({ foo: "many", bar: 1 }).put()
    )
    const [loaded, missing] = await Effect.runPromise(
      Simple.loadMany([item.keys(), { PK: "missing", SK: "missing" }])
    )
    expect(loaded).toBeInstanceOf(Simple)
    expect(missing).toBeInstanceOf(ItemNotFoundError)

    const invalidKey = { PK: "invalid", SK: "invalid" }
    await sandbox.seed({ ...invalidKey, foo: "invalid", bar: "not-a-number" })
    const invalid = await Effect.runPromise(Effect.flip(Simple.get(invalidKey)))
    expect(invalid).toBeInstanceOf(RuntimeTypeValidationError)
  })

  test("returns query metadata and streams iterator chunks lazily", async () => {
    await sandbox.seed(
      ...Array.from(
        { length: 5 },
        (_, value) => new A({ pk: "query", sk: `item#${value}`, value })
      )
    )

    const queried = await Effect.runPromise(A.query(queryParams))
    expect(queried).toHaveLength(5)
    expect(queried.meta.lastEvaluatedKey).toBeUndefined()

    const chunks = await Effect.runPromise(
      Stream.runCollect(A.iterator({ ...queryParams, ChunkSize: 2 }))
    )
    expect(
      Array.from(chunks as Array<unknown[]>, (chunk) => chunk.length)
    ).toEqual([2, 2, 1])

    const flattened = await Effect.runPromise(
      Stream.runCollect(
        Stream.flattenIterable(A.iterator({ ...queryParams, ChunkSize: 2 }))
      )
    )
    expect(Array.from(flattened)).toHaveLength(5)

    const iteratorError = await Effect.runPromise(
      Effect.flip(
        Stream.runCollect(A.iterator({ ...queryParams, ChunkSize: 0 }))
      )
    )
    expect(iteratorError).toBeInstanceOf(DynamoDBClientError)
    if (iteratorError instanceof DynamoDBClientError)
      expect(iteratorError.operation).toBe("iterator")
  })

  test("wraps pagination, batch, bulk, and union operations", async () => {
    const paginationError = await Effect.runPromise(
      Effect.flip(
        A.paginate(
          { first: 1, after: "not-a-cursor" },
          {
            KeyConditionExpression: "PK = :pk",
            ExpressionAttributeValues: { ":pk": "none" },
          }
        )
      )
    )
    expect(paginationError).toBeInstanceOf(PaginationError)

    const a = new A({ pk: "union", sk: "a", value: 1 })
    const b = new B({ pk: "union", sk: "b", name: "b" })
    await Effect.runPromise(a.put())
    await Effect.runPromise(b.put())

    const batch = await Effect.runPromise(
      Effect.gen(function* () {
        const db = yield* Effect.service(DynamoDB)
        return yield* db.batchGet({ a: A.operation("get", a.keys()) })
      }).pipe(Effect.provide(DynamoDB.layerFromClient(client)))
    )
    expect(batch.a).toBeInstanceOf(A)

    const unionItem = await Effect.runPromise(Union.get(a.keys()))
    expect(unionItem).toBeInstanceOf(A)
    const unionItems = await Effect.runPromise(
      Union.query({
        KeyConditionExpression: "PK = :pk",
        ExpressionAttributeValues: { ":pk": "union" },
      })
    )
    expect(unionItems).toHaveLength(2)

    const missing = await Effect.runPromise(
      Effect.flip(
        Effect.gen(function* () {
          const db = yield* Effect.service(DynamoDB)
          return yield* db.batchGet({
            missing: A.operation("get", { PK: "no", SK: "no" }),
          })
        }).pipe(Effect.provide(DynamoDB.layerFromClient(client)))
      )
    )
    expect(missing).toBeInstanceOf(ItemNotFoundError)
  })

  test("returns batch item errors and bulk transaction failures", async () => {
    const item = await Effect.runPromise(
      new A({ pk: "batch", sk: "item", value: 1 }).put()
    )

    const batch = await Effect.runPromise(
      Effect.service(DynamoDB).pipe(
        Effect.flatMap((db) =>
          db.batchGet(
            {
              item: A.operation("get", item.keys()),
              missing: A.operation("get", { PK: "missing", SK: "missing" }),
            },
            { individualErrors: true }
          )
        ),
        Effect.provide(DynamoDB.layerFromClient(client))
      )
    )
    expect(batch.item).toBeInstanceOf(A)
    expect(batch.missing).toBeInstanceOf(ItemNotFoundError)

    const bulk = await Effect.runPromise(
      Effect.flip(
        Effect.service(DynamoDB).pipe(
          Effect.flatMap((db) =>
            db.bulk([
              A.operation("put", new A({ pk: "bulk", sk: "put", value: 1 })),
              A.operation(
                "updateRaw",
                { PK: "missing", SK: "missing" },
                { value: 2 }
              ),
            ])
          ),
          Effect.provide(DynamoDB.layerFromClient(client))
        )
      )
    )
    expect(bulk).toBeInstanceOf(BulkWriteTransactionError)
  })

  test("wraps unknown client rejections with operation metadata", async () => {
    const get = jest.spyOn(client, "get").mockRejectedValue(new Error("boom"))

    const error = await Effect.runPromise(
      Effect.flip(Simple.get({ PK: "anything", SK: "anything" }))
    )
    expect(error).toBeInstanceOf(DynamoDBClientError)
    if (error instanceof DynamoDBClientError) {
      expect(error.operation).toBe("get")
      expect((error.cause as Error).message).toBe("boom")
    }

    get.mockRestore()
  })
})
