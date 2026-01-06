import * as t from "io-ts"
import { model, RuntimeTypeValidationError, union } from "@model-ts/core"
import { Sandbox, createSandbox } from "../sandbox"
import { Client } from "../client"
import { getProvider } from "../provider"
import {
  KeyExistsError,
  ItemNotFoundError,
  ConditionalCheckFailedError,
  RaceConditionError,
  BulkWriteTransactionError
} from "../errors"

const client = new Client({ tableName: "table" })
const provider = getProvider(client)

const SIMPLE_CODEC = t.type({
  foo: t.string,
  bar: t.number
})

class Simple extends model("Simple", SIMPLE_CODEC, provider) {
  get PK() {
    return `PK#${this.foo}`
  }

  get SK() {
    return `SK#${this.bar}`
  }
}

class SingleGSI extends model("SingleGSI", SIMPLE_CODEC, provider) {
  get PK() {
    return `PK#${this.foo}`
  }
  get SK() {
    return `SK#${this.bar}`
  }
  get GSI2PK() {
    return `GSI2PK#${this.foo}${this.foo}`
  }
  get GSI2SK() {
    return `GSI2SK#FIXED`
  }
}

class MultiGSI extends model("MultiGSI", SIMPLE_CODEC, provider) {
  get PK() {
    return `PK#${this.foo}`
  }
  get SK() {
    return `SK#${this.bar}`
  }
  get GSI2PK() {
    return `GSI2PK#${this.foo}${this.foo}`
  }
  get GSI2SK() {
    return `GSI2SK#FIXED`
  }
  get GSI3PK() {
    return `GSI3PK#FIXED`
  }
  get GSI3SK() {
    return `GSI3SK#${this.bar}${this.bar}`
  }
  get GSI4PK() {
    return `GSI4PK#FIXED`
  }
  get GSI4SK() {
    return `GSI4SK#${this.bar}${this.bar}`
  }
  get GSI5PK() {
    return `GSI5PK#FIXED`
  }
  get GSI5SK() {
    return `GSI5SK#${this.bar}${this.bar}`
  }
}

class A extends model(
  "A",
  t.type({ pk: t.string, sk: t.string, a: t.number }),
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
  "B",
  t.type({ pk: t.string, sk: t.string, b: t.string }),
  provider
) {
  get PK() {
    return this.pk
  }
  get SK() {
    return this.sk
  }
}
class C extends model(
  "C",
  t.type({ pk: t.string, sk: t.string, c: t.string }),
  provider
) {
  get PK() {
    return this.pk
  }
  get SK() {
    return this.sk
  }
}

class D extends model(
  "D",
  t.type({ pk: t.string, sk: t.string, d: t.string }),
  provider
) {
  get PK() {
    return this.pk
  }
  get SK() {
    return this.sk
  }
}

class Union extends union([C, D], provider) {}

let sandbox: Sandbox
beforeEach(async () => {
  sandbox = await createSandbox(client)
})

afterEach(async () => {
  await sandbox.destroy()
})

describe("put", () => {
  describe("via instance", () => {
    test("it inserts a simple model", async () => {
      const before = await sandbox.snapshot()

      await new Simple({ foo: "hi", bar: 42 }).put()

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
+ [PK#hi / SK#42]
+   PK: "PK#hi"
+   SK: "SK#42"
+   _docVersion: 0
+   _tag: "Simple"
+   bar: 42
+   foo: "hi"
`)
    })

    test("it inserts a model with single gsi", async () => {
      const before = await sandbox.snapshot()

      await new SingleGSI({ foo: "yes", bar: 42 }).put()

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
+ [PK#yes / SK#42]
+   PK: "PK#yes"
+   SK: "SK#42"
+   GSI2PK: "GSI2PK#yesyes"
+   GSI2SK: "GSI2SK#FIXED"
+   _docVersion: 0
+   _tag: "SingleGSI"
+   bar: 42
+   foo: "yes"
`)
    })

    test("it inserts a model with multiple gsi", async () => {
      const before = await sandbox.snapshot()

      await new MultiGSI({ foo: "yes", bar: 42 }).put()

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
+ [PK#yes / SK#42]
+   PK: "PK#yes"
+   SK: "SK#42"
+   GSI2PK: "GSI2PK#yesyes"
+   GSI2SK: "GSI2SK#FIXED"
+   GSI3PK: "GSI3PK#FIXED"
+   GSI3SK: "GSI3SK#4242"
+   GSI4PK: "GSI4PK#FIXED"
+   GSI4SK: "GSI4SK#4242"
+   GSI5PK: "GSI5PK#FIXED"
+   GSI5SK: "GSI5SK#4242"
+   _docVersion: 0
+   _tag: "MultiGSI"
+   bar: 42
+   foo: "yes"
`)
    })

    test("it throws KeyExistsError if item exists", async () => {
      await new MultiGSI({ foo: "yes", bar: 42 }).put()

      await expect(
        new MultiGSI({ foo: "yes", bar: 42 }).put()
      ).rejects.toBeInstanceOf(KeyExistsError)
    })

    test("it overwrites item if `ignoreExistence` is set", async () => {
      await new MultiGSI({ foo: "yes", bar: 42 }).put()

      await expect(
        new MultiGSI({ foo: "yes", bar: 42 }).put({ IgnoreExistence: true })
      ).resolves.toBeInstanceOf(MultiGSI)
    })
  })

  describe("via model", () => {
    test("it inserts a simple model", async () => {
      const before = await sandbox.snapshot()

      await Simple.put(new Simple({ foo: "hi", bar: 42 }))

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
+ [PK#hi / SK#42]
+   PK: "PK#hi"
+   SK: "SK#42"
+   _docVersion: 0
+   _tag: "Simple"
+   bar: 42
+   foo: "hi"
`)
    })

    test("it inserts a model with single gsi", async () => {
      const before = await sandbox.snapshot()

      await SingleGSI.put(new SingleGSI({ foo: "yes", bar: 42 }))

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
+ [PK#yes / SK#42]
+   PK: "PK#yes"
+   SK: "SK#42"
+   GSI2PK: "GSI2PK#yesyes"
+   GSI2SK: "GSI2SK#FIXED"
+   _docVersion: 0
+   _tag: "SingleGSI"
+   bar: 42
+   foo: "yes"
`)
    })

    test("it inserts a model with multiple gsi", async () => {
      const before = await sandbox.snapshot()

      await MultiGSI.put(new MultiGSI({ foo: "yes", bar: 42 }))

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
+ [PK#yes / SK#42]
+   PK: "PK#yes"
+   SK: "SK#42"
+   GSI2PK: "GSI2PK#yesyes"
+   GSI2SK: "GSI2SK#FIXED"
+   GSI3PK: "GSI3PK#FIXED"
+   GSI3SK: "GSI3SK#4242"
+   GSI4PK: "GSI4PK#FIXED"
+   GSI4SK: "GSI4SK#4242"
+   GSI5PK: "GSI5PK#FIXED"
+   GSI5SK: "GSI5SK#4242"
+   _docVersion: 0
+   _tag: "MultiGSI"
+   bar: 42
+   foo: "yes"
`)
    })

    test("it throws KeyExistsError if item exists", async () => {
      await MultiGSI.put(new MultiGSI({ foo: "yes", bar: 42 }))

      await expect(
        new MultiGSI({ foo: "yes", bar: 42 }).put()
      ).rejects.toBeInstanceOf(KeyExistsError)
    })

    test("it overwrites item if `ignoreExistence` is set", async () => {
      await MultiGSI.put(new MultiGSI({ foo: "yes", bar: 42 }))

      await expect(
        MultiGSI.put(new MultiGSI({ foo: "yes", bar: 42 }), {
          IgnoreExistence: true
        })
      ).resolves.toBeInstanceOf(MultiGSI)
    })
  })
})

describe("get", () => {
  describe("via model", () => {
    test("it throws `ItemNotFoundError` if item doesn't exist", async () => {
      await expect(
        Simple.get({ PK: "any", SK: "thing" })
      ).rejects.toBeInstanceOf(ItemNotFoundError)
    })

    test("it returns the item", async () => {
      const item = await new Simple({ foo: "hi", bar: 432 }).put()

      const result = await Simple.get({
        PK: item.keys().PK,
        SK: item.keys().SK
      })

      expect(result.values()).toMatchInlineSnapshot(`
              Object {
                "bar": 432,
                "foo": "hi",
              }
          `)

      expect(result.encode()).toEqual(item.encode())
    })

    test("it throws `RuntimeTypeError` if item can't be decoded", async () => {
      await sandbox.seed({ PK: "A", SK: "A", c: 324 })

      await expect(Simple.get({ PK: "A", SK: "A" })).rejects.toBeInstanceOf(
        RuntimeTypeValidationError
      )
    })
  })

  describe("via union", () => {
    test("it throws `ItemNotFoundError` if item doesn't exist", async () => {
      await expect(
        Union.get({ PK: "any", SK: "thing" })
      ).rejects.toBeInstanceOf(ItemNotFoundError)
    })

    test("it returns the item", async () => {
      const item = await new C({ pk: "PK#0", sk: "SK#0", c: "0" }).put()

      const result = await Union.get(item.keys())

      expect(result).toBeInstanceOf(C)
      expect(result.values()).toMatchInlineSnapshot(`
        Object {
          "c": "0",
          "pk": "PK#0",
          "sk": "SK#0",
        }
      `)
    })

    test("it throws `RuntimeTypeError` if item can't be decoded", async () => {
      await sandbox.seed({ PK: "A", SK: "A", a: 324 })

      await expect(Union.get({ PK: "A", SK: "A" })).rejects.toBeInstanceOf(
        RuntimeTypeValidationError
      )
    })
  })
})

describe("delete", () => {
  describe("via client", () => {
    test("it deletes the item and returns null", async () => {
      const item = await new Simple({ foo: "hi", bar: 432 }).put()

      const before = await sandbox.snapshot()

      const result = await client.delete({
        _operation: "delete",
        _model: Simple,
        key: {
          PK: item.keys().PK,
          SK: item.keys().SK
        }
      })

      expect(result).toBeNull()

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
- [PK#hi / SK#432]
-   PK: "PK#hi"
-   SK: "SK#432"
-   _docVersion: 0
-   _tag: "Simple"
-   bar: 432
-   foo: "hi"
`)
    })
  })

  describe("via model", () => {
    test("it deletes the item and returns null", async () => {
      const item = await new Simple({ foo: "hi", bar: 432 }).put()

      const before = await sandbox.snapshot()

      const result = await Simple.delete({
        PK: item.keys().PK,
        SK: item.keys().SK
      })

      expect(result).toBeNull()

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
- [PK#hi / SK#432]
-   PK: "PK#hi"
-   SK: "SK#432"
-   _docVersion: 0
-   _tag: "Simple"
-   bar: 432
-   foo: "hi"
`)
    })
  })

  describe("via instance", () => {
    test("it deletes the item and returns null", async () => {
      const item = await new Simple({ foo: "hi", bar: 432 }).put()

      const before = await sandbox.snapshot()

      const result = await item.delete()

      expect(result).toBeNull()

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
- [PK#hi / SK#432]
-   PK: "PK#hi"
-   SK: "SK#432"
-   _docVersion: 0
-   _tag: "Simple"
-   bar: 432
-   foo: "hi"
`)
    })
  })
})

describe("softDelete", () => {
  describe("via client", () => {
    test("it soft-deletes the item", async () => {
      const item = await new Simple({ foo: "hi", bar: 432 }).put()
      const withGSI = await new MultiGSI({ foo: "hello", bar: 42 }).put()

      const before = await sandbox.snapshot()

      const simpleResult = await client.softDelete(item)
      const withGSIResult = await client.softDelete(withGSI)

      expect(simpleResult.values()).toMatchInlineSnapshot(`
        Object {
          "bar": 432,
          "foo": "hi",
        }
      `)
      expect(withGSIResult.values()).toMatchInlineSnapshot(`
        Object {
          "bar": 42,
          "foo": "hello",
        }
      `)

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
+ [$$DELETED$$PK#hello / $$DELETED$$SK#42]
+   PK: "$$DELETED$$PK#hello"
+   SK: "$$DELETED$$SK#42"
+   GSI2PK: "$$DELETED$$GSI2PK#hellohello"
+   GSI2SK: "$$DELETED$$GSI2SK#FIXED"
+   GSI3PK: "$$DELETED$$GSI3PK#FIXED"
+   GSI3SK: "$$DELETED$$GSI3SK#4242"
+   GSI4PK: "$$DELETED$$GSI4PK#FIXED"
+   GSI4SK: "$$DELETED$$GSI4SK#4242"
+   GSI5PK: "$$DELETED$$GSI5PK#FIXED"
+   GSI5SK: "$$DELETED$$GSI5SK#4242"
+   _deletedAt: "2021-05-01T08:00:00.000Z"
+   _docVersion: 0
+   _tag: "MultiGSI"
+   bar: 42
+   foo: "hello"

+ [$$DELETED$$PK#hi / $$DELETED$$SK#432]
+   PK: "$$DELETED$$PK#hi"
+   SK: "$$DELETED$$SK#432"
+   _deletedAt: "2021-05-01T08:00:00.000Z"
+   _docVersion: 0
+   _tag: "Simple"
+   bar: 432
+   foo: "hi"

- [PK#hello / SK#42]
-   PK: "PK#hello"
-   SK: "SK#42"
-   GSI2PK: "GSI2PK#hellohello"
-   GSI2SK: "GSI2SK#FIXED"
-   GSI3PK: "GSI3PK#FIXED"
-   GSI3SK: "GSI3SK#4242"
-   GSI4PK: "GSI4PK#FIXED"
-   GSI4SK: "GSI4SK#4242"
-   GSI5PK: "GSI5PK#FIXED"
-   GSI5SK: "GSI5SK#4242"
-   _docVersion: 0
-   _tag: "MultiGSI"
-   bar: 42
-   foo: "hello"

- [PK#hi / SK#432]
-   PK: "PK#hi"
-   SK: "SK#432"
-   _docVersion: 0
-   _tag: "Simple"
-   bar: 432
-   foo: "hi"
`)
    })
  })

  describe("via model", () => {
    test("it soft-deletes the item", async () => {
      const item = await new Simple({ foo: "hi", bar: 432 }).put()

      const before = await sandbox.snapshot()

      const result = await Simple.softDelete(item)

      expect(result.values()).toMatchInlineSnapshot(`
        Object {
          "bar": 432,
          "foo": "hi",
        }
      `)

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
+ [$$DELETED$$PK#hi / $$DELETED$$SK#432]
+   PK: "$$DELETED$$PK#hi"
+   SK: "$$DELETED$$SK#432"
+   _deletedAt: "2021-05-01T08:00:00.000Z"
+   _docVersion: 0
+   _tag: "Simple"
+   bar: 432
+   foo: "hi"

- [PK#hi / SK#432]
-   PK: "PK#hi"
-   SK: "SK#432"
-   _docVersion: 0
-   _tag: "Simple"
-   bar: 432
-   foo: "hi"
`)
    })
  })

  describe("via instance", () => {
    test("it soft-deletes the item", async () => {
      const item = await new Simple({ foo: "hi", bar: 432 }).put()

      const before = await sandbox.snapshot()

      const result = await item.softDelete()

      expect(result.values()).toMatchInlineSnapshot(`
        Object {
          "bar": 432,
          "foo": "hi",
        }
      `)

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
+ [$$DELETED$$PK#hi / $$DELETED$$SK#432]
+   PK: "$$DELETED$$PK#hi"
+   SK: "$$DELETED$$SK#432"
+   _deletedAt: "2021-05-01T08:00:00.000Z"
+   _docVersion: 0
+   _tag: "Simple"
+   bar: 432
+   foo: "hi"

- [PK#hi / SK#432]
-   PK: "PK#hi"
-   SK: "SK#432"
-   _docVersion: 0
-   _tag: "Simple"
-   bar: 432
-   foo: "hi"
`)
    })
  })
})

describe("updateRaw", () => {
  test("it throws `ItemNotFoundError` if item doesn't exist", async () => {
    await expect(
      Simple.updateRaw({ PK: "not", SK: "existent" }, { foo: "new foo" })
    ).rejects.toBeInstanceOf(ItemNotFoundError)
  })

  test("it throws `ConditionalCheckFailedError` if custom condition expression fails", async () => {
    await expect(
      Simple.updateRaw(
        { PK: "not", SK: "existent" },
        { foo: "new foo" },
        { ConditionExpression: "PK = somethingelse" }
      )
    ).rejects.toBeInstanceOf(ConditionalCheckFailedError)
  })

  test("IT DOES NOT UPDATE KEYS AUTOMATICALLY", async () => {
    const item = await new Simple({ foo: "old", bar: 43 }).put()

    const result = await Simple.updateRaw(
      { PK: item.PK, SK: item.SK },
      { foo: "new foo" }
    )

    // NOTE: although the result of updateRaw seems to hold the correct keys, it's important to note
    // that it is not reflected in the DB!
    expect(result.PK).toEqual(`PK#new foo`)
    expect(await sandbox.snapshot()).toMatchInlineSnapshot(`
      Object {
        "PK#old__SK#43": Object {
          "PK": "PK#old",
          "SK": "SK#43",
          "_docVersion": 0,
          "_tag": "Simple",
          "bar": 43,
          "foo": "new foo",
        },
      }
    `)
  })
})

describe("update", () => {
  describe("in-place", () => {
    class InPlace extends model(
      "InPlace",
      t.type({ foo: t.string, bar: t.number }),
      provider
    ) {
      get PK() {
        return "FIXEDPK"
      }

      get SK() {
        return "FIXEDSK"
      }
    }

    test("it puts the item if it wasn't stored before", async () => {
      const item = new InPlace({ foo: "hello", bar: 1 })

      await item.update({ foo: "ciao" })

      expect(await sandbox.snapshot()).toMatchInlineSnapshot(`
        Object {
          "FIXEDPK__FIXEDSK": Object {
            "PK": "FIXEDPK",
            "SK": "FIXEDSK",
            "_docVersion": 1,
            "_tag": "InPlace",
            "bar": 1,
            "foo": "ciao",
          },
        }
      `)
    })

    test("it throws `RaceConditionError` if item was manipulated inbetween", async () => {
      const item = await new InPlace({ foo: "hello", bar: 1 }).put()
      await item.update({ foo: "ciao" })

      await expect(item.update({ foo: "good luck" })).rejects.toBeInstanceOf(
        RaceConditionError
      )
    })

    test("it updates an item in-place", async () => {
      const item = await new InPlace({ foo: "hello", bar: 1 }).put()

      const before = await sandbox.snapshot()

      expect((await item.update({ foo: "ciao" })).values())
        .toMatchInlineSnapshot(`
        Object {
          "bar": 1,
          "foo": "ciao",
        }
      `)

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
[FIXEDPK / FIXEDSK]
    PK: "FIXEDPK"
    SK: "FIXEDSK"
-   _docVersion: 0
+   _docVersion: 1
    _tag: "InPlace"
    bar: 1
-   foo: "hello"
+   foo: "ciao"
`)
    })
  })
})

describe("applyUpdate", () => {
  test("it returns the updated item and update operation", async () => {
    const item = await new A({ pk: "PK", sk: "SK", a: 1 }).put()

    const before = await sandbox.snapshot()

    const [updatedItem, updateOp] = item.applyUpdate({ a: 2 })
    expect(updatedItem.values()).toMatchInlineSnapshot(`
      Object {
        "a": 2,
        "pk": "PK",
        "sk": "SK",
      }
    `)

    await client.bulk([updateOp])

    expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
[PK / SK]
    PK: "PK"
    SK: "SK"
-   _docVersion: 0
+   _docVersion: 1
    _tag: "A"
-   a: 1
+   a: 2
    pk: "PK"
    sk: "SK"
`)
  })
})

describe("query", () => {
  describe("via client", () => {
    test("it returns empty results", async () => {
      expect(
        await client.query(
          {
            KeyConditionExpression: `PK = :pk and begins_with(SK, :sk)`,
            ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT" }
          },
          { a: A, b: B, union: Union }
        )
      ).toMatchInlineSnapshot(`
              Object {
                "_unknown": Array [],
                "a": Array [],
                "b": Array [],
                "meta": Object {
                  "lastEvaluatedKey": undefined,
                },
                "union": Array [],
              }
          `)
    })

    test("it returns unknown results", async () => {
      await sandbox.seed({ PK: "abc", SK: "SORT#1", doesnt: "match" })

      expect(
        await client.query(
          {
            KeyConditionExpression: `PK = :pk and begins_with(SK, :sk)`,
            ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT#" }
          },
          { a: A, b: B, union: Union }
        )
      ).toMatchInlineSnapshot(`
              Object {
                "_unknown": Array [
                  Object {
                    "PK": "abc",
                    "SK": "SORT#1",
                    "doesnt": "match",
                  },
                ],
                "a": Array [],
                "b": Array [],
                "meta": Object {
                  "lastEvaluatedKey": undefined,
                },
                "union": Array [],
              }
          `)
    })

    test("it returns results", async () => {
      await sandbox.seed(
        new A({ pk: "abc", sk: "SORT#1", a: 1 }),
        new A({ pk: "abc", sk: "SORT#2", a: 2 }),
        new B({ pk: "abc", sk: "SORT#3", b: "hi" }),
        { PK: "abc", SK: "SORT#4", probably: "unknown" },
        new C({ pk: "abc", sk: "SORT#5", c: "hi" }),
        new D({ pk: "abc", sk: "SORT#6", d: "hi" })
      )

      const { a, b, union, _unknown, meta } = await client.query(
        {
          KeyConditionExpression: `PK = :pk and begins_with(SK, :sk)`,
          ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT#" }
        },
        { a: A, b: B, union: Union }
      )

      expect({
        meta: meta,
        _unknown: _unknown,
        a: a.map(item => item.values()),
        b: b.map(item => item.values()),
        union: union.map(item => item.values())
      }).toMatchInlineSnapshot(`
              Object {
                "_unknown": Array [
                  Object {
                    "PK": "abc",
                    "SK": "SORT#4",
                    "probably": "unknown",
                  },
                ],
                "a": Array [
                  Object {
                    "a": 1,
                    "pk": "abc",
                    "sk": "SORT#1",
                  },
                  Object {
                    "a": 2,
                    "pk": "abc",
                    "sk": "SORT#2",
                  },
                ],
                "b": Array [
                  Object {
                    "b": "hi",
                    "pk": "abc",
                    "sk": "SORT#3",
                  },
                ],
                "meta": Object {
                  "lastEvaluatedKey": undefined,
                },
                "union": Array [
                  Object {
                    "c": "hi",
                    "pk": "abc",
                    "sk": "SORT#5",
                  },
                  Object {
                    "d": "hi",
                    "pk": "abc",
                    "sk": "SORT#6",
                  },
                ],
              }
          `)
    })

    test("it paginates", async () => {
      await sandbox.seed(
        ...Array.from({ length: 20 }).map(
          (_, i) =>
            new A({ pk: "abc", sk: `SORT#${String(i).padStart(2, "0")}`, a: i })
        ),
        ...Array.from({ length: 20 }).map(
          (_, i) => new B({ pk: "abc", sk: `SORT#${i + 20}`, b: "bar" })
        )
      )

      const firstPage = await client.query(
        {
          KeyConditionExpression: `PK = :pk and begins_with(SK, :sk)`,
          ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT#" },
          Limit: 30
        },
        { a: A, b: B }
      )

      expect(firstPage.a.length).toBe(20)
      expect(firstPage.b.length).toBe(10)
      expect(firstPage._unknown.length).toBe(0)
      expect(firstPage.meta.lastEvaluatedKey).not.toBeUndefined()

      const secondPage = await client.query(
        {
          KeyConditionExpression: `PK = :pk and begins_with(SK, :sk)`,
          ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT#" },
          Limit: 30,
          ExclusiveStartKey: firstPage.meta.lastEvaluatedKey
        },
        { a: A, b: B }
      )

      expect(secondPage.a.length).toBe(0)
      expect(secondPage.b.length).toBe(10)
      expect(secondPage._unknown.length).toBe(0)
      expect(secondPage.meta.lastEvaluatedKey).toBeUndefined()
    })

    test("it fetches all pages automatically", async () => {
      await sandbox.seed(
        ...Array.from({ length: 20 }).map(
          (_, i) =>
            new A({ pk: "abc", sk: `SORT#${String(i).padStart(2, "0")}`, a: i })
        ),
        ...Array.from({ length: 20 }).map(
          (_, i) => new B({ pk: "abc", sk: `SORT#${i + 20}`, b: "bar" })
        )
      )

      const { a, b, meta, _unknown } = await client.query(
        {
          KeyConditionExpression: `PK = :pk and begins_with(SK, :sk)`,
          ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT#" },
          FetchAllPages: true,
          // You wouldn't set a limit in a real-world use case here to optimize fetching all items.
          Limit: 10
        },
        { a: A, b: B }
      )

      expect(a.length).toBe(20)
      expect(b.length).toBe(20)
      expect(_unknown.length).toBe(0)
      expect(meta.lastEvaluatedKey).toBeUndefined()
    })
  })

  describe("via model", () => {
    test("it returns empty results for specific model", async () => {
      const result = await A.query({
        KeyConditionExpression: `PK = :pk and begins_with(SK, :sk)`,
        ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT" }
      })

      expect(result.length).toBe(0)
      expect(result.meta).toEqual({ lastEvaluatedKey: undefined })
    })

    test("it returns only matching model items", async () => {
      await sandbox.seed(
        new A({ pk: "abc", sk: "SORT#1", a: 1 }),
        new A({ pk: "abc", sk: "SORT#2", a: 2 }),
        new B({ pk: "abc", sk: "SORT#3", b: "hi" }),
        { PK: "abc", SK: "SORT#4", probably: "unknown" },
        new C({ pk: "abc", sk: "SORT#5", c: "hi" })
      )

      const result = await A.query({
        KeyConditionExpression: `PK = :pk and begins_with(SK, :sk)`,
        ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT#" }
      })

      expect(result.length).toBe(2)
      expect(result.map(item => item.values())).toMatchInlineSnapshot(`
        Array [
          Object {
            "a": 1,
            "pk": "abc",
            "sk": "SORT#1",
          },
          Object {
            "a": 2,
            "pk": "abc",
            "sk": "SORT#2",
          },
        ]
      `)
      expect(result.meta).toEqual({ lastEvaluatedKey: undefined })
    })

    test("it respects query parameters", async () => {
      await sandbox.seed(
        new A({ pk: "abc", sk: "SORT#1", a: 1 }),
        new A({ pk: "abc", sk: "SORT#2", a: 2 }),
        new A({ pk: "abc", sk: "SORT#3", a: 3 }),
        new A({ pk: "abc", sk: "SORT#4", a: 4 })
      )

      const result = await A.query({
        KeyConditionExpression: `PK = :pk and begins_with(SK, :sk)`,
        ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT#" },
        Limit: 2
      })

      expect(result.length).toBe(2)
      expect(result[0].a).toBe(1)
      expect(result[1].a).toBe(2)
      expect(result.meta.lastEvaluatedKey).toBeDefined()
    })

    test("it fetches all pages when FetchAllPages is true", async () => {
      await sandbox.seed(
        ...Array.from({ length: 15 }).map(
          (_, i) =>
            new A({ pk: "abc", sk: `SORT#${String(i).padStart(2, "0")}`, a: i })
        ),
        // Add some B items that should be ignored
        new B({ pk: "abc", sk: "SORT#20", b: "ignored" }),
        new B({ pk: "abc", sk: "SORT#21", b: "ignored" })
      )

      const result = await A.query({
        KeyConditionExpression: `PK = :pk and begins_with(SK, :sk)`,
        ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT#" },
        FetchAllPages: true,
        Limit: 5 // Force pagination
      })

      expect(result.length).toBe(15) // Only A items
      expect(result.map(item => item.a)).toEqual(
        Array.from({ length: 15 }, (_, i) => i)
      )
      expect(result.meta.lastEvaluatedKey).toBeUndefined()
    })

    test("it works with different query conditions", async () => {
      await sandbox.seed(
        new A({ pk: "user1", sk: "post#1", a: 1 }),
        new A({ pk: "user1", sk: "post#2", a: 2 }),
        new A({ pk: "user1", sk: "comment#1", a: 3 }),
        new A({ pk: "user2", sk: "post#1", a: 4 })
      )

      // Query for posts only
      const posts = await A.query({
        KeyConditionExpression: `PK = :pk and begins_with(SK, :sk)`,
        ExpressionAttributeValues: { ":pk": "user1", ":sk": "post#" }
      })

      expect(posts.length).toBe(2)
      expect(posts.map(item => item.a).sort()).toEqual([1, 2])

      // Query for all user1 items
      const allUser1 = await A.query({
        KeyConditionExpression: `PK = :pk`,
        ExpressionAttributeValues: { ":pk": "user1" }
      })

      expect(allUser1.length).toBe(3)
      expect(allUser1.map(item => item.a).sort()).toEqual([1, 2, 3])
    })

    test("it works with FilterExpression", async () => {
      await sandbox.seed(
        new A({ pk: "test", sk: "item#1", a: 1 }),
        new A({ pk: "test", sk: "item#2", a: 2 }),
        new A({ pk: "test", sk: "item#3", a: 3 }),
        new A({ pk: "test", sk: "item#4", a: 4 })
      )

      const result = await A.query({
        KeyConditionExpression: `PK = :pk`,
        FilterExpression: `a > :min`,
        ExpressionAttributeValues: { ":pk": "test", ":min": 2 }
      })

      expect(result.length).toBe(2)
      expect(result.map(item => item.a).sort()).toEqual([3, 4])
    })
  })

  describe("via union", () => {
    test("it returns empty results for union", async () => {
      const result = await Union.query({
        KeyConditionExpression: `PK = :pk and begins_with(SK, :sk)`,
        ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT" }
      })

      expect(result.length).toBe(0)
      expect(result.meta).toEqual({ lastEvaluatedKey: undefined })
    })

    test("it returns only union model items", async () => {
      await sandbox.seed(
        new A({ pk: "abc", sk: "SORT#1", a: 1 }),
        new B({ pk: "abc", sk: "SORT#2", b: "hi" }),
        new C({ pk: "abc", sk: "SORT#3", c: "c1" }),
        new D({ pk: "abc", sk: "SORT#4", d: "d1" }),
        { PK: "abc", SK: "SORT#5", unknown: "data" }
      )

      const result = await Union.query({
        KeyConditionExpression: `PK = :pk and begins_with(SK, :sk)`,
        ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT#" }
      })

      expect(result.length).toBe(2) // Only C and D items (union members)
      expect(result[0]).toBeInstanceOf(C)
      expect(result[1]).toBeInstanceOf(D)
      expect(result.meta).toEqual({ lastEvaluatedKey: undefined })
    })

    test("it fetches all pages for union when FetchAllPages is true", async () => {
      await sandbox.seed(
        ...Array.from({ length: 10 }).map(
          (_, i) =>
            new C({
              pk: "abc",
              sk: `SORT#${String(i).padStart(2, "0")}`,
              c: `c${i}`
            })
        ),
        ...Array.from({ length: 10 }).map(
          (_, i) =>
            new D({
              pk: "abc",
              sk: `SORT#${String(i + 10).padStart(2, "0")}`,
              d: `d${i}`
            })
        ),
        // Add some non-union items that should be ignored
        new A({ pk: "abc", sk: "SORT#30", a: 1 }),
        new B({ pk: "abc", sk: "SORT#31", b: "ignored" })
      )

      const result = await Union.query({
        KeyConditionExpression: `PK = :pk and begins_with(SK, :sk)`,
        ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT#" },
        FetchAllPages: true,
        Limit: 5 // Force pagination
      })

      expect(result.length).toBe(20) // Only union items (C and D)
      expect(result.filter((item: C | D) => item instanceof C).length).toBe(10)
      expect(result.filter((item: C | D) => item instanceof D).length).toBe(10)
      expect(result.meta.lastEvaluatedKey).toBeUndefined()
    })
  })
})

describe("bulk", () => {
  describe("< 100 elements (true transaction)", () => {
    test("it succeeds", async () => {
      const softDeleteTarget = new B({ pk: "PK#3", sk: "SK#3", b: "bar" })

      await sandbox.seed(
        new A({ pk: "PK#1", sk: "SK#1", a: 1 }),
        new A({ pk: "PK#2", sk: "SK#2", a: 2 }),
        softDeleteTarget,
        new B({ pk: "PK#UPDATE", sk: "SK#UPDATE", b: "bar" }),
        new B({ pk: "PK#COND", sk: "SK#COND", b: "cond" })
      )

      const before = await sandbox.snapshot()

      await client.bulk([
        new A({ pk: "PK4", sk: "PK4", a: 4 }).operation("put"),
        A.operation("put", new A({ pk: "PK5", sk: "PK5", a: 5 })),
        new B({ pk: "PK6", sk: "SK6", b: "baz" }).operation("put"),
        A.operation("updateRaw", { PK: "PK#1", SK: "SK#1" }, { a: -1 }),
        new A({ pk: "PK#2", sk: "SK#2", a: 2 }).operation("delete"),
        B.operation("softDelete", softDeleteTarget),
        new B({
          pk: "PK#UPDATE",
          sk: "SK#UPDATE",
          b: "bar"
        }).operation("update", { b: "baz" }),
        new B({
          pk: "PK#COND",
          sk: "SK#COND",
          b: "cond"
        }).operation("condition", {
          ConditionExpression: "b = :cond",
          ExpressionAttributeValues: { ":cond": "cond" }
        })
      ])

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
+ [$$DELETED$$PK#3 / $$DELETED$$SK#3]
+   PK: "$$DELETED$$PK#3"
+   SK: "$$DELETED$$SK#3"
+   _deletedAt: "2021-05-01T08:00:00.000Z"
+   _docVersion: 0
+   _tag: "B"
+   b: "bar"
+   pk: "PK#3"
+   sk: "SK#3"

[PK#1 / SK#1]
    PK: "PK#1"
    SK: "SK#1"
    _docVersion: 0
    _tag: "A"
-   a: 1
+   a: -1
    pk: "PK#1"
    sk: "SK#1"

- [PK#2 / SK#2]
-   PK: "PK#2"
-   SK: "SK#2"
-   _docVersion: 0
-   _tag: "A"
-   a: 2
-   pk: "PK#2"
-   sk: "SK#2"

- [PK#3 / SK#3]
-   PK: "PK#3"
-   SK: "SK#3"
-   _docVersion: 0
-   _tag: "B"
-   b: "bar"
-   pk: "PK#3"
-   sk: "SK#3"

[PK#UPDATE / SK#UPDATE]
    PK: "PK#UPDATE"
    SK: "SK#UPDATE"
-   _docVersion: 0
+   _docVersion: 1
    _tag: "B"
-   b: "bar"
+   b: "baz"
    pk: "PK#UPDATE"
    sk: "SK#UPDATE"

+ [PK4 / PK4]
+   PK: "PK4"
+   SK: "PK4"
+   _docVersion: 0
+   _tag: "A"
+   a: 4
+   pk: "PK4"
+   sk: "PK4"

+ [PK5 / PK5]
+   PK: "PK5"
+   SK: "PK5"
+   _docVersion: 0
+   _tag: "A"
+   a: 5
+   pk: "PK5"
+   sk: "PK5"

+ [PK6 / SK6]
+   PK: "PK6"
+   SK: "SK6"
+   _docVersion: 0
+   _tag: "B"
+   b: "baz"
+   pk: "PK6"
+   sk: "SK6"
`)
    })

    test("it fails", async () => {
      await sandbox.seed(
        new A({ pk: "PK#1", sk: "SK#1", a: 1 }),
        new A({ pk: "PK#2", sk: "SK#2", a: 2 }),
        new B({ pk: "PK#3", sk: "SK#3", b: "bar" }),
        new B({ pk: "PK#UPDATE", sk: "SK#UPDATE", b: "bar" }),
        new B({ pk: "PK#COND", sk: "SK#COND", b: "cond" })
      )

      const before = await sandbox.snapshot()

      await expect(
        client.bulk([
          // Succeed
          new A({ pk: "PK#4", sk: "PK#4", a: 4 }).operation("put"),
          A.operation("put", new A({ pk: "PK5", sk: "PK5", a: 5 })),
          new B({ pk: "PK#6", sk: "SK#6", b: "baz" }).operation("put"),

          // Fails
          A.operation(
            "updateRaw",
            { PK: "PK#nicetry", SK: "SK#nope" },
            { a: 234 }
          )
        ])
      ).rejects.toBeInstanceOf(BulkWriteTransactionError)

      expect(await sandbox.snapshot()).toEqual(before)
    })
  })

  describe("> 100 items (pseudo transaction)", () => {
    test("it succeeds", async () => {
      await sandbox.seed(
        new A({ pk: "PK#1", sk: "SK#1", a: 1 }),
        new A({ pk: "PK#2", sk: "SK#2", a: 2 }),
        new B({ pk: "PK#3", sk: "SK#3", b: "bar" })
      )

      const before = await sandbox.snapshot()

      await client.bulk([
        new A({ pk: "PK4", sk: "PK4", a: 4 }).operation("put"),
        A.operation("put", new A({ pk: "PK5", sk: "PK5", a: 5 })),
        new B({ pk: "PK6", sk: "SK6", b: "baz" }).operation("put"),
        A.operation("updateRaw", { PK: "PK#1", SK: "SK#1" }, { a: -1 }),
        new A({ pk: "PK#2", sk: "SK#2", a: 2 }).operation("delete"),
        B.operation("delete", { PK: "PK#3", SK: "SK#3" }),
        new B({
          pk: "PK#UPDATE",
          sk: "SK#UPDATE",
          b: "bar"
        }).operation("update", { b: "baz" }),
        ...Array.from({ length: 100 }).map((_, i) =>
          new A({ pk: `PK#A${i}`, sk: `SK#A${i}`, a: i }).operation("put")
        )
      ])

      //#region snapshot
      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
[PK#1 / SK#1]
    PK: "PK#1"
    SK: "SK#1"
    _docVersion: 0
    _tag: "A"
-   a: 1
+   a: -1
    pk: "PK#1"
    sk: "SK#1"

- [PK#2 / SK#2]
-   PK: "PK#2"
-   SK: "SK#2"
-   _docVersion: 0
-   _tag: "A"
-   a: 2
-   pk: "PK#2"
-   sk: "SK#2"

- [PK#3 / SK#3]
-   PK: "PK#3"
-   SK: "SK#3"
-   _docVersion: 0
-   _tag: "B"
-   b: "bar"
-   pk: "PK#3"
-   sk: "SK#3"

+ [PK#A0 / SK#A0]
+   PK: "PK#A0"
+   SK: "SK#A0"
+   _docVersion: 0
+   _tag: "A"
+   a: 0
+   pk: "PK#A0"
+   sk: "SK#A0"

+ [PK#A1 / SK#A1]
+   PK: "PK#A1"
+   SK: "SK#A1"
+   _docVersion: 0
+   _tag: "A"
+   a: 1
+   pk: "PK#A1"
+   sk: "SK#A1"

+ [PK#A10 / SK#A10]
+   PK: "PK#A10"
+   SK: "SK#A10"
+   _docVersion: 0
+   _tag: "A"
+   a: 10
+   pk: "PK#A10"
+   sk: "SK#A10"

+ [PK#A11 / SK#A11]
+   PK: "PK#A11"
+   SK: "SK#A11"
+   _docVersion: 0
+   _tag: "A"
+   a: 11
+   pk: "PK#A11"
+   sk: "SK#A11"

+ [PK#A12 / SK#A12]
+   PK: "PK#A12"
+   SK: "SK#A12"
+   _docVersion: 0
+   _tag: "A"
+   a: 12
+   pk: "PK#A12"
+   sk: "SK#A12"

+ [PK#A13 / SK#A13]
+   PK: "PK#A13"
+   SK: "SK#A13"
+   _docVersion: 0
+   _tag: "A"
+   a: 13
+   pk: "PK#A13"
+   sk: "SK#A13"

+ [PK#A14 / SK#A14]
+   PK: "PK#A14"
+   SK: "SK#A14"
+   _docVersion: 0
+   _tag: "A"
+   a: 14
+   pk: "PK#A14"
+   sk: "SK#A14"

+ [PK#A15 / SK#A15]
+   PK: "PK#A15"
+   SK: "SK#A15"
+   _docVersion: 0
+   _tag: "A"
+   a: 15
+   pk: "PK#A15"
+   sk: "SK#A15"

+ [PK#A16 / SK#A16]
+   PK: "PK#A16"
+   SK: "SK#A16"
+   _docVersion: 0
+   _tag: "A"
+   a: 16
+   pk: "PK#A16"
+   sk: "SK#A16"

+ [PK#A17 / SK#A17]
+   PK: "PK#A17"
+   SK: "SK#A17"
+   _docVersion: 0
+   _tag: "A"
+   a: 17
+   pk: "PK#A17"
+   sk: "SK#A17"

+ [PK#A18 / SK#A18]
+   PK: "PK#A18"
+   SK: "SK#A18"
+   _docVersion: 0
+   _tag: "A"
+   a: 18
+   pk: "PK#A18"
+   sk: "SK#A18"

+ [PK#A19 / SK#A19]
+   PK: "PK#A19"
+   SK: "SK#A19"
+   _docVersion: 0
+   _tag: "A"
+   a: 19
+   pk: "PK#A19"
+   sk: "SK#A19"

+ [PK#A2 / SK#A2]
+   PK: "PK#A2"
+   SK: "SK#A2"
+   _docVersion: 0
+   _tag: "A"
+   a: 2
+   pk: "PK#A2"
+   sk: "SK#A2"

+ [PK#A20 / SK#A20]
+   PK: "PK#A20"
+   SK: "SK#A20"
+   _docVersion: 0
+   _tag: "A"
+   a: 20
+   pk: "PK#A20"
+   sk: "SK#A20"

+ [PK#A21 / SK#A21]
+   PK: "PK#A21"
+   SK: "SK#A21"
+   _docVersion: 0
+   _tag: "A"
+   a: 21
+   pk: "PK#A21"
+   sk: "SK#A21"

+ [PK#A22 / SK#A22]
+   PK: "PK#A22"
+   SK: "SK#A22"
+   _docVersion: 0
+   _tag: "A"
+   a: 22
+   pk: "PK#A22"
+   sk: "SK#A22"

+ [PK#A23 / SK#A23]
+   PK: "PK#A23"
+   SK: "SK#A23"
+   _docVersion: 0
+   _tag: "A"
+   a: 23
+   pk: "PK#A23"
+   sk: "SK#A23"

+ [PK#A24 / SK#A24]
+   PK: "PK#A24"
+   SK: "SK#A24"
+   _docVersion: 0
+   _tag: "A"
+   a: 24
+   pk: "PK#A24"
+   sk: "SK#A24"

+ [PK#A25 / SK#A25]
+   PK: "PK#A25"
+   SK: "SK#A25"
+   _docVersion: 0
+   _tag: "A"
+   a: 25
+   pk: "PK#A25"
+   sk: "SK#A25"

+ [PK#A26 / SK#A26]
+   PK: "PK#A26"
+   SK: "SK#A26"
+   _docVersion: 0
+   _tag: "A"
+   a: 26
+   pk: "PK#A26"
+   sk: "SK#A26"

+ [PK#A27 / SK#A27]
+   PK: "PK#A27"
+   SK: "SK#A27"
+   _docVersion: 0
+   _tag: "A"
+   a: 27
+   pk: "PK#A27"
+   sk: "SK#A27"

+ [PK#A28 / SK#A28]
+   PK: "PK#A28"
+   SK: "SK#A28"
+   _docVersion: 0
+   _tag: "A"
+   a: 28
+   pk: "PK#A28"
+   sk: "SK#A28"

+ [PK#A29 / SK#A29]
+   PK: "PK#A29"
+   SK: "SK#A29"
+   _docVersion: 0
+   _tag: "A"
+   a: 29
+   pk: "PK#A29"
+   sk: "SK#A29"

+ [PK#A3 / SK#A3]
+   PK: "PK#A3"
+   SK: "SK#A3"
+   _docVersion: 0
+   _tag: "A"
+   a: 3
+   pk: "PK#A3"
+   sk: "SK#A3"

+ [PK#A30 / SK#A30]
+   PK: "PK#A30"
+   SK: "SK#A30"
+   _docVersion: 0
+   _tag: "A"
+   a: 30
+   pk: "PK#A30"
+   sk: "SK#A30"

+ [PK#A31 / SK#A31]
+   PK: "PK#A31"
+   SK: "SK#A31"
+   _docVersion: 0
+   _tag: "A"
+   a: 31
+   pk: "PK#A31"
+   sk: "SK#A31"

+ [PK#A32 / SK#A32]
+   PK: "PK#A32"
+   SK: "SK#A32"
+   _docVersion: 0
+   _tag: "A"
+   a: 32
+   pk: "PK#A32"
+   sk: "SK#A32"

+ [PK#A33 / SK#A33]
+   PK: "PK#A33"
+   SK: "SK#A33"
+   _docVersion: 0
+   _tag: "A"
+   a: 33
+   pk: "PK#A33"
+   sk: "SK#A33"

+ [PK#A34 / SK#A34]
+   PK: "PK#A34"
+   SK: "SK#A34"
+   _docVersion: 0
+   _tag: "A"
+   a: 34
+   pk: "PK#A34"
+   sk: "SK#A34"

+ [PK#A35 / SK#A35]
+   PK: "PK#A35"
+   SK: "SK#A35"
+   _docVersion: 0
+   _tag: "A"
+   a: 35
+   pk: "PK#A35"
+   sk: "SK#A35"

+ [PK#A36 / SK#A36]
+   PK: "PK#A36"
+   SK: "SK#A36"
+   _docVersion: 0
+   _tag: "A"
+   a: 36
+   pk: "PK#A36"
+   sk: "SK#A36"

+ [PK#A37 / SK#A37]
+   PK: "PK#A37"
+   SK: "SK#A37"
+   _docVersion: 0
+   _tag: "A"
+   a: 37
+   pk: "PK#A37"
+   sk: "SK#A37"

+ [PK#A38 / SK#A38]
+   PK: "PK#A38"
+   SK: "SK#A38"
+   _docVersion: 0
+   _tag: "A"
+   a: 38
+   pk: "PK#A38"
+   sk: "SK#A38"

+ [PK#A39 / SK#A39]
+   PK: "PK#A39"
+   SK: "SK#A39"
+   _docVersion: 0
+   _tag: "A"
+   a: 39
+   pk: "PK#A39"
+   sk: "SK#A39"

+ [PK#A4 / SK#A4]
+   PK: "PK#A4"
+   SK: "SK#A4"
+   _docVersion: 0
+   _tag: "A"
+   a: 4
+   pk: "PK#A4"
+   sk: "SK#A4"

+ [PK#A40 / SK#A40]
+   PK: "PK#A40"
+   SK: "SK#A40"
+   _docVersion: 0
+   _tag: "A"
+   a: 40
+   pk: "PK#A40"
+   sk: "SK#A40"

+ [PK#A41 / SK#A41]
+   PK: "PK#A41"
+   SK: "SK#A41"
+   _docVersion: 0
+   _tag: "A"
+   a: 41
+   pk: "PK#A41"
+   sk: "SK#A41"

+ [PK#A42 / SK#A42]
+   PK: "PK#A42"
+   SK: "SK#A42"
+   _docVersion: 0
+   _tag: "A"
+   a: 42
+   pk: "PK#A42"
+   sk: "SK#A42"

+ [PK#A43 / SK#A43]
+   PK: "PK#A43"
+   SK: "SK#A43"
+   _docVersion: 0
+   _tag: "A"
+   a: 43
+   pk: "PK#A43"
+   sk: "SK#A43"

+ [PK#A44 / SK#A44]
+   PK: "PK#A44"
+   SK: "SK#A44"
+   _docVersion: 0
+   _tag: "A"
+   a: 44
+   pk: "PK#A44"
+   sk: "SK#A44"

+ [PK#A45 / SK#A45]
+   PK: "PK#A45"
+   SK: "SK#A45"
+   _docVersion: 0
+   _tag: "A"
+   a: 45
+   pk: "PK#A45"
+   sk: "SK#A45"

+ [PK#A46 / SK#A46]
+   PK: "PK#A46"
+   SK: "SK#A46"
+   _docVersion: 0
+   _tag: "A"
+   a: 46
+   pk: "PK#A46"
+   sk: "SK#A46"

+ [PK#A47 / SK#A47]
+   PK: "PK#A47"
+   SK: "SK#A47"
+   _docVersion: 0
+   _tag: "A"
+   a: 47
+   pk: "PK#A47"
+   sk: "SK#A47"

+ [PK#A48 / SK#A48]
+   PK: "PK#A48"
+   SK: "SK#A48"
+   _docVersion: 0
+   _tag: "A"
+   a: 48
+   pk: "PK#A48"
+   sk: "SK#A48"

+ [PK#A49 / SK#A49]
+   PK: "PK#A49"
+   SK: "SK#A49"
+   _docVersion: 0
+   _tag: "A"
+   a: 49
+   pk: "PK#A49"
+   sk: "SK#A49"

+ [PK#A5 / SK#A5]
+   PK: "PK#A5"
+   SK: "SK#A5"
+   _docVersion: 0
+   _tag: "A"
+   a: 5
+   pk: "PK#A5"
+   sk: "SK#A5"

+ [PK#A50 / SK#A50]
+   PK: "PK#A50"
+   SK: "SK#A50"
+   _docVersion: 0
+   _tag: "A"
+   a: 50
+   pk: "PK#A50"
+   sk: "SK#A50"

+ [PK#A51 / SK#A51]
+   PK: "PK#A51"
+   SK: "SK#A51"
+   _docVersion: 0
+   _tag: "A"
+   a: 51
+   pk: "PK#A51"
+   sk: "SK#A51"

+ [PK#A52 / SK#A52]
+   PK: "PK#A52"
+   SK: "SK#A52"
+   _docVersion: 0
+   _tag: "A"
+   a: 52
+   pk: "PK#A52"
+   sk: "SK#A52"

+ [PK#A53 / SK#A53]
+   PK: "PK#A53"
+   SK: "SK#A53"
+   _docVersion: 0
+   _tag: "A"
+   a: 53
+   pk: "PK#A53"
+   sk: "SK#A53"

+ [PK#A54 / SK#A54]
+   PK: "PK#A54"
+   SK: "SK#A54"
+   _docVersion: 0
+   _tag: "A"
+   a: 54
+   pk: "PK#A54"
+   sk: "SK#A54"

+ [PK#A55 / SK#A55]
+   PK: "PK#A55"
+   SK: "SK#A55"
+   _docVersion: 0
+   _tag: "A"
+   a: 55
+   pk: "PK#A55"
+   sk: "SK#A55"

+ [PK#A56 / SK#A56]
+   PK: "PK#A56"
+   SK: "SK#A56"
+   _docVersion: 0
+   _tag: "A"
+   a: 56
+   pk: "PK#A56"
+   sk: "SK#A56"

+ [PK#A57 / SK#A57]
+   PK: "PK#A57"
+   SK: "SK#A57"
+   _docVersion: 0
+   _tag: "A"
+   a: 57
+   pk: "PK#A57"
+   sk: "SK#A57"

+ [PK#A58 / SK#A58]
+   PK: "PK#A58"
+   SK: "SK#A58"
+   _docVersion: 0
+   _tag: "A"
+   a: 58
+   pk: "PK#A58"
+   sk: "SK#A58"

+ [PK#A59 / SK#A59]
+   PK: "PK#A59"
+   SK: "SK#A59"
+   _docVersion: 0
+   _tag: "A"
+   a: 59
+   pk: "PK#A59"
+   sk: "SK#A59"

+ [PK#A6 / SK#A6]
+   PK: "PK#A6"
+   SK: "SK#A6"
+   _docVersion: 0
+   _tag: "A"
+   a: 6
+   pk: "PK#A6"
+   sk: "SK#A6"

+ [PK#A60 / SK#A60]
+   PK: "PK#A60"
+   SK: "SK#A60"
+   _docVersion: 0
+   _tag: "A"
+   a: 60
+   pk: "PK#A60"
+   sk: "SK#A60"

+ [PK#A61 / SK#A61]
+   PK: "PK#A61"
+   SK: "SK#A61"
+   _docVersion: 0
+   _tag: "A"
+   a: 61
+   pk: "PK#A61"
+   sk: "SK#A61"

+ [PK#A62 / SK#A62]
+   PK: "PK#A62"
+   SK: "SK#A62"
+   _docVersion: 0
+   _tag: "A"
+   a: 62
+   pk: "PK#A62"
+   sk: "SK#A62"

+ [PK#A63 / SK#A63]
+   PK: "PK#A63"
+   SK: "SK#A63"
+   _docVersion: 0
+   _tag: "A"
+   a: 63
+   pk: "PK#A63"
+   sk: "SK#A63"

+ [PK#A64 / SK#A64]
+   PK: "PK#A64"
+   SK: "SK#A64"
+   _docVersion: 0
+   _tag: "A"
+   a: 64
+   pk: "PK#A64"
+   sk: "SK#A64"

+ [PK#A65 / SK#A65]
+   PK: "PK#A65"
+   SK: "SK#A65"
+   _docVersion: 0
+   _tag: "A"
+   a: 65
+   pk: "PK#A65"
+   sk: "SK#A65"

+ [PK#A66 / SK#A66]
+   PK: "PK#A66"
+   SK: "SK#A66"
+   _docVersion: 0
+   _tag: "A"
+   a: 66
+   pk: "PK#A66"
+   sk: "SK#A66"

+ [PK#A67 / SK#A67]
+   PK: "PK#A67"
+   SK: "SK#A67"
+   _docVersion: 0
+   _tag: "A"
+   a: 67
+   pk: "PK#A67"
+   sk: "SK#A67"

+ [PK#A68 / SK#A68]
+   PK: "PK#A68"
+   SK: "SK#A68"
+   _docVersion: 0
+   _tag: "A"
+   a: 68
+   pk: "PK#A68"
+   sk: "SK#A68"

+ [PK#A69 / SK#A69]
+   PK: "PK#A69"
+   SK: "SK#A69"
+   _docVersion: 0
+   _tag: "A"
+   a: 69
+   pk: "PK#A69"
+   sk: "SK#A69"

+ [PK#A7 / SK#A7]
+   PK: "PK#A7"
+   SK: "SK#A7"
+   _docVersion: 0
+   _tag: "A"
+   a: 7
+   pk: "PK#A7"
+   sk: "SK#A7"

+ [PK#A70 / SK#A70]
+   PK: "PK#A70"
+   SK: "SK#A70"
+   _docVersion: 0
+   _tag: "A"
+   a: 70
+   pk: "PK#A70"
+   sk: "SK#A70"

+ [PK#A71 / SK#A71]
+   PK: "PK#A71"
+   SK: "SK#A71"
+   _docVersion: 0
+   _tag: "A"
+   a: 71
+   pk: "PK#A71"
+   sk: "SK#A71"

+ [PK#A72 / SK#A72]
+   PK: "PK#A72"
+   SK: "SK#A72"
+   _docVersion: 0
+   _tag: "A"
+   a: 72
+   pk: "PK#A72"
+   sk: "SK#A72"

+ [PK#A73 / SK#A73]
+   PK: "PK#A73"
+   SK: "SK#A73"
+   _docVersion: 0
+   _tag: "A"
+   a: 73
+   pk: "PK#A73"
+   sk: "SK#A73"

+ [PK#A74 / SK#A74]
+   PK: "PK#A74"
+   SK: "SK#A74"
+   _docVersion: 0
+   _tag: "A"
+   a: 74
+   pk: "PK#A74"
+   sk: "SK#A74"

+ [PK#A75 / SK#A75]
+   PK: "PK#A75"
+   SK: "SK#A75"
+   _docVersion: 0
+   _tag: "A"
+   a: 75
+   pk: "PK#A75"
+   sk: "SK#A75"

+ [PK#A76 / SK#A76]
+   PK: "PK#A76"
+   SK: "SK#A76"
+   _docVersion: 0
+   _tag: "A"
+   a: 76
+   pk: "PK#A76"
+   sk: "SK#A76"

+ [PK#A77 / SK#A77]
+   PK: "PK#A77"
+   SK: "SK#A77"
+   _docVersion: 0
+   _tag: "A"
+   a: 77
+   pk: "PK#A77"
+   sk: "SK#A77"

+ [PK#A78 / SK#A78]
+   PK: "PK#A78"
+   SK: "SK#A78"
+   _docVersion: 0
+   _tag: "A"
+   a: 78
+   pk: "PK#A78"
+   sk: "SK#A78"

+ [PK#A79 / SK#A79]
+   PK: "PK#A79"
+   SK: "SK#A79"
+   _docVersion: 0
+   _tag: "A"
+   a: 79
+   pk: "PK#A79"
+   sk: "SK#A79"

+ [PK#A8 / SK#A8]
+   PK: "PK#A8"
+   SK: "SK#A8"
+   _docVersion: 0
+   _tag: "A"
+   a: 8
+   pk: "PK#A8"
+   sk: "SK#A8"

+ [PK#A80 / SK#A80]
+   PK: "PK#A80"
+   SK: "SK#A80"
+   _docVersion: 0
+   _tag: "A"
+   a: 80
+   pk: "PK#A80"
+   sk: "SK#A80"

+ [PK#A81 / SK#A81]
+   PK: "PK#A81"
+   SK: "SK#A81"
+   _docVersion: 0
+   _tag: "A"
+   a: 81
+   pk: "PK#A81"
+   sk: "SK#A81"

+ [PK#A82 / SK#A82]
+   PK: "PK#A82"
+   SK: "SK#A82"
+   _docVersion: 0
+   _tag: "A"
+   a: 82
+   pk: "PK#A82"
+   sk: "SK#A82"

+ [PK#A83 / SK#A83]
+   PK: "PK#A83"
+   SK: "SK#A83"
+   _docVersion: 0
+   _tag: "A"
+   a: 83
+   pk: "PK#A83"
+   sk: "SK#A83"

+ [PK#A84 / SK#A84]
+   PK: "PK#A84"
+   SK: "SK#A84"
+   _docVersion: 0
+   _tag: "A"
+   a: 84
+   pk: "PK#A84"
+   sk: "SK#A84"

+ [PK#A85 / SK#A85]
+   PK: "PK#A85"
+   SK: "SK#A85"
+   _docVersion: 0
+   _tag: "A"
+   a: 85
+   pk: "PK#A85"
+   sk: "SK#A85"

+ [PK#A86 / SK#A86]
+   PK: "PK#A86"
+   SK: "SK#A86"
+   _docVersion: 0
+   _tag: "A"
+   a: 86
+   pk: "PK#A86"
+   sk: "SK#A86"

+ [PK#A87 / SK#A87]
+   PK: "PK#A87"
+   SK: "SK#A87"
+   _docVersion: 0
+   _tag: "A"
+   a: 87
+   pk: "PK#A87"
+   sk: "SK#A87"

+ [PK#A88 / SK#A88]
+   PK: "PK#A88"
+   SK: "SK#A88"
+   _docVersion: 0
+   _tag: "A"
+   a: 88
+   pk: "PK#A88"
+   sk: "SK#A88"

+ [PK#A89 / SK#A89]
+   PK: "PK#A89"
+   SK: "SK#A89"
+   _docVersion: 0
+   _tag: "A"
+   a: 89
+   pk: "PK#A89"
+   sk: "SK#A89"

+ [PK#A9 / SK#A9]
+   PK: "PK#A9"
+   SK: "SK#A9"
+   _docVersion: 0
+   _tag: "A"
+   a: 9
+   pk: "PK#A9"
+   sk: "SK#A9"

+ [PK#A90 / SK#A90]
+   PK: "PK#A90"
+   SK: "SK#A90"
+   _docVersion: 0
+   _tag: "A"
+   a: 90
+   pk: "PK#A90"
+   sk: "SK#A90"

+ [PK#A91 / SK#A91]
+   PK: "PK#A91"
+   SK: "SK#A91"
+   _docVersion: 0
+   _tag: "A"
+   a: 91
+   pk: "PK#A91"
+   sk: "SK#A91"

+ [PK#A92 / SK#A92]
+   PK: "PK#A92"
+   SK: "SK#A92"
+   _docVersion: 0
+   _tag: "A"
+   a: 92
+   pk: "PK#A92"
+   sk: "SK#A92"

+ [PK#A93 / SK#A93]
+   PK: "PK#A93"
+   SK: "SK#A93"
+   _docVersion: 0
+   _tag: "A"
+   a: 93
+   pk: "PK#A93"
+   sk: "SK#A93"

+ [PK#A94 / SK#A94]
+   PK: "PK#A94"
+   SK: "SK#A94"
+   _docVersion: 0
+   _tag: "A"
+   a: 94
+   pk: "PK#A94"
+   sk: "SK#A94"

+ [PK#A95 / SK#A95]
+   PK: "PK#A95"
+   SK: "SK#A95"
+   _docVersion: 0
+   _tag: "A"
+   a: 95
+   pk: "PK#A95"
+   sk: "SK#A95"

+ [PK#A96 / SK#A96]
+   PK: "PK#A96"
+   SK: "SK#A96"
+   _docVersion: 0
+   _tag: "A"
+   a: 96
+   pk: "PK#A96"
+   sk: "SK#A96"

+ [PK#A97 / SK#A97]
+   PK: "PK#A97"
+   SK: "SK#A97"
+   _docVersion: 0
+   _tag: "A"
+   a: 97
+   pk: "PK#A97"
+   sk: "SK#A97"

+ [PK#A98 / SK#A98]
+   PK: "PK#A98"
+   SK: "SK#A98"
+   _docVersion: 0
+   _tag: "A"
+   a: 98
+   pk: "PK#A98"
+   sk: "SK#A98"

+ [PK#A99 / SK#A99]
+   PK: "PK#A99"
+   SK: "SK#A99"
+   _docVersion: 0
+   _tag: "A"
+   a: 99
+   pk: "PK#A99"
+   sk: "SK#A99"

+ [PK#UPDATE / SK#UPDATE]
+   PK: "PK#UPDATE"
+   SK: "SK#UPDATE"
+   _docVersion: 1
+   _tag: "B"
+   b: "baz"
+   pk: "PK#UPDATE"
+   sk: "SK#UPDATE"

+ [PK4 / PK4]
+   PK: "PK4"
+   SK: "PK4"
+   _docVersion: 0
+   _tag: "A"
+   a: 4
+   pk: "PK4"
+   sk: "PK4"

+ [PK5 / PK5]
+   PK: "PK5"
+   SK: "PK5"
+   _docVersion: 0
+   _tag: "A"
+   a: 5
+   pk: "PK5"
+   sk: "PK5"

+ [PK6 / SK6]
+   PK: "PK6"
+   SK: "SK6"
+   _docVersion: 0
+   _tag: "B"
+   b: "baz"
+   pk: "PK6"
+   sk: "SK6"
`)
      //#endregion
    })

    test("it fails and rolls back", async () => {
      const before = await sandbox.snapshot()

      await expect(
        client.bulk([
          // Succeeds
          ...Array.from({ length: 110 }).map((_, i) =>
            new A({ pk: `PK#${i}`, sk: `SK#${i}`, a: i }).operation("put")
          ),

          // Fails
          A.operation(
            "condition",
            { PK: "nicetry", SK: "nope" },
            { ConditionExpression: "attribute_exists(PK)" }
          )
        ])
      ).rejects.toBeInstanceOf(BulkWriteTransactionError)

      expect(await sandbox.snapshot()).toEqual(before)
    })
  })
})

describe("batchGet", () => {
  class A extends model(
    "A",
    t.type({ pk: t.string, sk: t.string, a: t.number }),
    provider
  ) {
    get PK() {
      return this.pk
    }
    get SK() {
      return this.sk
    }
  }

  test("it fetches an empty record", async () => {
    expect(await client.batchGet({})).toEqual({})
  })

  test("it throws if some items don't exist", async () => {
    await expect(
      client.batchGet({
        one: A.operation("get", { PK: "PK#1", SK: "SK#1" }),
        two: A.operation("get", { PK: "PK#2", SK: "SK#2" }),
        three: A.operation("get", { PK: "PK#3", SK: "SK#3" }),
        four: A.operation("get", { PK: "PK#4", SK: "SK#4" }),
        duplicate: A.operation("get", { PK: "PK#1", SK: "SK#1" })
      })
    ).rejects.toBeInstanceOf(ItemNotFoundError)
  })

  test("it returns individual errors", async () => {
    await sandbox.seed(
      new A({ pk: "PK#1", sk: "SK#1", a: 1 }),
      new A({ pk: "PK#2", sk: "SK#2", a: 2 })
    )

    const result = await client.batchGet(
      {
        one: A.operation("get", { PK: "PK#1", SK: "SK#1" }),
        two: A.operation("get", { PK: "PK#2", SK: "SK#2" }),
        duplicate: A.operation("get", { PK: "PK#1", SK: "SK#1" }),
        error: A.operation("get", { PK: "PK#error", SK: "SK#error" }),
        error2: A.operation("get", { PK: "PK#error2", SK: "SK#error2" })
      },
      { individualErrors: true }
    )

    expect(result.one).toBeInstanceOf(A)
    expect(result.two).toBeInstanceOf(A)
    expect(result.duplicate).toBeInstanceOf(A)
    expect(result.error).toBeInstanceOf(ItemNotFoundError)
  })

  test("it fetches <=100 entries in one go", async () => {
    await sandbox.seed(
      new A({ pk: "PK#1", sk: "SK#1", a: 1 }),
      new A({ pk: "PK#2", sk: "SK#2", a: 2 }),
      new A({ pk: "PK#3", sk: "SK#3", a: 3 }),
      new A({ pk: "PK#4", sk: "SK#4", a: 4 })
    )

    const results = await client.batchGet({
      one: A.operation("get", { PK: "PK#1", SK: "SK#1" }),
      two: A.operation("get", { PK: "PK#2", SK: "SK#2" }),
      three: A.operation("get", { PK: "PK#3", SK: "SK#3" }),
      four: A.operation("get", { PK: "PK#4", SK: "SK#4" }),
      duplicate: A.operation("get", { PK: "PK#1", SK: "SK#1" })
    })

    expect(
      Object.fromEntries(
        Object.entries(results).map(([key, val]) => [key, val.values()])
      )
    ).toMatchInlineSnapshot(`
      Object {
        "duplicate": Object {
          "a": 1,
          "pk": "PK#1",
          "sk": "SK#1",
        },
        "four": Object {
          "a": 4,
          "pk": "PK#4",
          "sk": "SK#4",
        },
        "one": Object {
          "a": 1,
          "pk": "PK#1",
          "sk": "SK#1",
        },
        "three": Object {
          "a": 3,
          "pk": "PK#3",
          "sk": "SK#3",
        },
        "two": Object {
          "a": 2,
          "pk": "PK#2",
          "sk": "SK#2",
        },
      }
    `)
  })
})

describe("load", () => {
  describe("client", () => {
    test("it throws if item doesn't exist", async () => {
      await expect(
        client.load(A.operation("get", { PK: "PK", SK: "SK" }))
      ).rejects.toBeInstanceOf(ItemNotFoundError)
    })

    test("it returns null instead of throwing if item doesn't exist", async () => {
      await expect(
        client.load(A.operation("get", { PK: "PK", SK: "SK" }), { null: true })
      ).resolves.toBeNull()
    })

    test("it recovers a soft deleted item", async () => {
      const item = new A({ pk: "PK#1", sk: "SK#1", a: 1 })

      await sandbox.seed(item)

      await item.softDelete()

      const recovered = await client.load(
        A.operation("get", { PK: "PK#1", SK: "SK#1" }),
        {
          recover: true
        }
      )

      expect(recovered).toBeInstanceOf(A)
      expect(recovered.isDeleted).toBe(true)
    })

    test("it throws if no item or soft deleted item exists", async () => {
      await expect(
        client.load(A.operation("get", { PK: "PK", SK: "SK" }), {
          recover: true
        })
      ).rejects.toBeInstanceOf(ItemNotFoundError)
    })

    test("it returns null instead of throwing if no item or soft deleted item exists", async () => {
      await expect(
        client.load(A.operation("get", { PK: "PK", SK: "SK" }), {
          recover: true,
          null: true
        })
      ).resolves.toBeNull()
    })

    test("it fetches >100 items", async () => {
      const items = Array.from({ length: 234 }).map((_, i) =>
        i < 100
          ? new A({ pk: String(i), sk: String(i), a: i })
          : new B({ pk: String(i), sk: String(i), b: String(i) })
      )

      const spy = jest.spyOn(client, "batchGet")

      await sandbox.seed(...items)

      const results = await Promise.all<A | B>(
        items.map(({ PK, SK }, i) =>
          i < 100
            ? client.load(A.operation("get", { PK, SK }))
            : client.load(B.operation("get", { PK, SK }))
        )
      )

      expect(results.length).toBe(234)
      expect(spy).toHaveBeenCalledTimes(3)

      spy.mockReset()
      spy.mockRestore()
    })
  })

  describe("model", () => {
    test("it throws if item doesn't exist", async () => {
      await expect(A.load({ PK: "PK", SK: "SK" })).rejects.toBeInstanceOf(
        ItemNotFoundError
      )
    })

    test("it returns null instead of throwing if item doesn't exist", async () => {
      await expect(
        A.load({ PK: "PK", SK: "SK" }, { null: true })
      ).resolves.toBeNull()
    })

    test("it fetches >100 items", async () => {
      const items = Array.from({ length: 234 }).map(
        (_, i) => new A({ pk: String(i), sk: String(i), a: i })
      )

      const spy = jest.spyOn(client, "batchGet")

      await sandbox.seed(...items)

      const results = await Promise.all<A | B>(
        items.map(({ PK, SK }, i) => A.load({ PK, SK }))
      )

      expect(results.length).toBe(234)
      expect(spy).toHaveBeenCalledTimes(3)

      spy.mockReset()
      spy.mockRestore()
    })
  })

  describe("union", () => {
    test("it throws if item doesn't exist", async () => {
      await expect(Union.load({ PK: "PK", SK: "SK" })).rejects.toBeInstanceOf(
        ItemNotFoundError
      )
    })

    test("it returns null instead of throwing if item doesn't exist", async () => {
      await expect(
        Union.load({ PK: "PK", SK: "SK" }, { null: true })
      ).resolves.toBeNull()
    })

    test("it fetches >100 items", async () => {
      const items = Array.from({ length: 234 }).map((_, i) =>
        i < 123
          ? new C({ pk: String(i), sk: String(i), c: String(i) })
          : new D({ pk: String(i), sk: String(i), d: String(i) })
      )

      const spy = jest.spyOn(client, "batchGet")

      await sandbox.seed(...items)

      const results = await Promise.all(
        items.map(({ PK, SK }, i) => Union.load({ PK, SK }))
      )

      expect(results.length).toBe(234)
      expect(results.filter(item => item instanceof C).length).toBe(123)
      expect(results.filter(item => item instanceof D).length).toBe(111)
      expect(spy).toHaveBeenCalledTimes(3)

      spy.mockReset()
      spy.mockRestore()
    })
  })
})

describe("loadMany", () => {
  describe("client", () => {
    test("it fetches >100 items", async () => {
      const items = Array.from({ length: 234 }).map((_, i) =>
        i < 100
          ? new A({ pk: String(i), sk: String(i), a: i })
          : new B({ pk: String(i), sk: String(i), b: String(i) })
      )

      const spy = jest.spyOn(client, "batchGet")

      await sandbox.seed(...items)

      const results = await client.loadMany<typeof A | typeof B>(
        items.map(({ PK, SK }, i) =>
          i < 100
            ? A.operation("get", { PK, SK })
            : B.operation("get", { PK, SK })
        )
      )

      expect(results.length).toBe(234)
      expect(spy).toHaveBeenCalledTimes(3)

      spy.mockReset()
      spy.mockRestore()
    })
  })

  describe("model", () => {
    test("it fetches >100 items", async () => {
      const items = Array.from({ length: 234 }).map(
        (_, i) => new A({ pk: String(i), sk: String(i), a: i })
      )

      const spy = jest.spyOn(client, "batchGet")

      await sandbox.seed(...items)

      const results = await A.loadMany(items.map(({ PK, SK }) => ({ PK, SK })))

      expect(results.length).toBe(234)
      expect(spy).toHaveBeenCalledTimes(3)

      spy.mockReset()
      spy.mockRestore()
    })
  })

  describe("union", () => {
    test("it fetches >100 items", async () => {
      const items = Array.from({ length: 234 }).map((_, i) =>
        i < 123
          ? new C({ pk: String(i), sk: String(i), c: String(i) })
          : new D({ pk: String(i), sk: String(i), d: String(i) })
      )

      const spy = jest.spyOn(client, "batchGet")

      await sandbox.seed(...items)

      const results = await Union.loadMany(
        items.map(({ PK, SK }) => ({ PK, SK }))
      )

      expect(results.length).toBe(234)
      expect(results.filter(item => item instanceof C).length).toBe(123)
      expect(results.filter(item => item instanceof D).length).toBe(111)
      expect(spy).toHaveBeenCalledTimes(3)

      spy.mockReset()
      spy.mockRestore()
    })
  })
})

describe("paginate", () => {
  describe("client", () => {
    test("it paginates a regular model", async () => {
      const items = Array.from({ length: 60 }).map(
        (_, i) =>
          new C({ pk: "PK", sk: String(i).padStart(3, "0"), c: String(i) })
      )

      await sandbox.seed(...items)

      // Forwards
      const page1 = await client.paginate(
        C,
        {},
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page1.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMTkifQ==",
          "hasNextPage": true,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDAifQ==",
        }
      `)
      expect(page1.edges.length).toBe(20)
      expect(page1.edges[0].node.c).toBe("0")
      expect(page1.edges[19].node.c).toBe("19")

      const page2 = await client.paginate(
        C,
        { after: page1.pageInfo.endCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page2.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMzkifQ==",
          "hasNextPage": true,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMjAifQ==",
        }
      `)
      expect(page2.edges.length).toBe(20)
      expect(page2.edges[0].node.c).toBe("20")
      expect(page2.edges[19].node.c).toBe("39")

      const page3 = await client.paginate(
        C,
        { after: page2.pageInfo.endCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page3.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwNTkifQ==",
          "hasNextPage": false,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwNDAifQ==",
        }
      `)
      expect(page3.edges.length).toBe(20)
      expect(page3.edges[0].node.c).toBe("40")
      expect(page3.edges[19].node.c).toBe("59")

      // Backwards
      const backwardsPage2 = await client.paginate(
        C,
        { before: page3.pageInfo.startCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(backwardsPage2.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMzkifQ==",
          "hasNextPage": false,
          "hasPreviousPage": true,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMjAifQ==",
        }
      `)
      expect(backwardsPage2.edges.length).toBe(20)
      expect(backwardsPage2.edges[0].node.c).toBe("20")
      expect(backwardsPage2.edges[19].node.c).toBe("39")

      const backwardsPage1 = await client.paginate(
        C,
        { before: backwardsPage2.pageInfo.startCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(backwardsPage1.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMTkifQ==",
          "hasNextPage": false,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDAifQ==",
        }
      `)
      expect(backwardsPage1.edges.length).toBe(20)
      expect(backwardsPage1.edges[0].node.c).toBe("0")
      expect(backwardsPage1.edges[19].node.c).toBe("19")
    })

    test("it paginates a union model", async () => {
      const items = Array.from({ length: 60 }).map((_, i) =>
        i > 30
          ? new C({ pk: "PK", sk: String(i).padStart(3, "0"), c: String(i) })
          : new D({ pk: "PK", sk: String(i).padStart(3, "0"), d: String(i) })
      )

      await sandbox.seed(...items)

      // Forwards
      const page1 = await client.paginate(
        Union,
        {},
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page1.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMTkifQ==",
          "hasNextPage": true,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDAifQ==",
        }
      `)
      expect(page1.edges.length).toBe(20)
      expect(page1.edges[0].node.SK).toBe("000")
      expect(page1.edges[19].node.SK).toBe("019")

      const page2 = await client.paginate(
        Union,
        { after: page1.pageInfo.endCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page2.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMzkifQ==",
          "hasNextPage": true,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMjAifQ==",
        }
      `)
      expect(page2.edges.length).toBe(20)
      expect(page2.edges[0].node.SK).toBe("020")
      expect(page2.edges[19].node.SK).toBe("039")

      const page3 = await client.paginate(
        Union,
        { after: page2.pageInfo.endCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page3.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwNTkifQ==",
          "hasNextPage": false,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwNDAifQ==",
        }
      `)
      expect(page3.edges.length).toBe(20)
      expect(page3.edges[0].node.SK).toBe("040")
      expect(page3.edges[19].node.SK).toBe("059")

      // Backwards
      const backwardsPage2 = await client.paginate(
        Union,
        { before: page3.pageInfo.startCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(backwardsPage2.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMzkifQ==",
          "hasNextPage": false,
          "hasPreviousPage": true,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMjAifQ==",
        }
      `)
      expect(backwardsPage2.edges.length).toBe(20)
      expect(backwardsPage2.edges[0].node.SK).toBe("020")
      expect(backwardsPage2.edges[19].node.SK).toBe("039")

      const backwardsPage1 = await client.paginate(
        Union,
        { before: backwardsPage2.pageInfo.startCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(backwardsPage1.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMTkifQ==",
          "hasNextPage": false,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDAifQ==",
        }
      `)
      expect(backwardsPage1.edges.length).toBe(20)
      expect(backwardsPage1.edges[0].node.SK).toBe("000")
      expect(backwardsPage1.edges[19].node.SK).toBe("019")
    })

    test("it respects a limit", async () => {
      const items = Array.from({ length: 60 }).map(
        (_, i) =>
          new C({ pk: "PK", sk: String(i).padStart(3, "0"), c: String(i) })
      )

      await sandbox.seed(...items)

      // Forwards
      const page = await client.paginate(
        C,
        { first: 10 },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDkifQ==",
          "hasNextPage": true,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDAifQ==",
        }
      `)
      expect(page.edges.length).toBe(10)
      expect(page.edges[0].node.c).toBe("0")
      expect(page.edges[9].node.c).toBe("9")
    })

    test("it doesn't exceed the max limit", async () => {
      const items = Array.from({ length: 60 }).map(
        (_, i) =>
          new C({ pk: "PK", sk: String(i).padStart(3, "0"), c: String(i) })
      )

      await sandbox.seed(...items)

      // Forwards
      const page1 = await client.paginate(
        C,
        { first: 60 },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page1.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwNDkifQ==",
          "hasNextPage": true,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDAifQ==",
        }
      `)
      expect(page1.edges.length).toBe(50)
      expect(page1.edges[0].node.c).toBe("0")
      expect(page1.edges[49].node.c).toBe("49")
    })

    test("it respects custom pagination default", async () => {
      client.paginationOptions = {
        default: 40
      }

      const items = Array.from({ length: 50 }).map(
        (_, i) =>
          new C({ pk: "PK", sk: String(i).padStart(3, "0"), c: String(i) })
      )

      await sandbox.seed(...items)

      const page = await client.paginate(
        C,
        {},
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page.edges.length).toBe(40)

      delete client.paginationOptions
    })

    test("it respects custom pagination limit", async () => {
      client.paginationOptions = {
        limit: 100
      }

      const items = Array.from({ length: 120 }).map(
        (_, i) =>
          new C({ pk: "PK", sk: String(i).padStart(3, "0"), c: String(i) })
      )

      await sandbox.seed(...items)

      const page = await client.paginate(
        C,
        { first: 110 },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page.edges.length).toBe(100)

      delete client.paginationOptions
    })
  })

  describe("model", () => {
    test("it paginates a regular model", async () => {
      const items = Array.from({ length: 60 }).map(
        (_, i) =>
          new C({ pk: "PK", sk: String(i).padStart(3, "0"), c: String(i) })
      )

      await sandbox.seed(...items)

      // Forwards
      const page1 = await C.paginate(
        {},
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page1.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMTkifQ==",
          "hasNextPage": true,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDAifQ==",
        }
      `)
      expect(page1.edges.length).toBe(20)
      expect(page1.edges[0].node.c).toBe("0")
      expect(page1.edges[19].node.c).toBe("19")

      const page2 = await C.paginate(
        { after: page1.pageInfo.endCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page2.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMzkifQ==",
          "hasNextPage": true,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMjAifQ==",
        }
      `)
      expect(page2.edges.length).toBe(20)
      expect(page2.edges[0].node.c).toBe("20")
      expect(page2.edges[19].node.c).toBe("39")

      const page3 = await C.paginate(
        { after: page2.pageInfo.endCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page3.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwNTkifQ==",
          "hasNextPage": false,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwNDAifQ==",
        }
      `)
      expect(page3.edges.length).toBe(20)
      expect(page3.edges[0].node.c).toBe("40")
      expect(page3.edges[19].node.c).toBe("59")

      // Backwards
      const backwardsPage2 = await C.paginate(
        { before: page3.pageInfo.startCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(backwardsPage2.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMzkifQ==",
          "hasNextPage": false,
          "hasPreviousPage": true,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMjAifQ==",
        }
      `)
      expect(backwardsPage2.edges.length).toBe(20)
      expect(backwardsPage2.edges[0].node.c).toBe("20")
      expect(backwardsPage2.edges[19].node.c).toBe("39")

      const backwardsPage1 = await C.paginate(
        { before: backwardsPage2.pageInfo.startCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(backwardsPage1.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMTkifQ==",
          "hasNextPage": false,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDAifQ==",
        }
      `)
      expect(backwardsPage1.edges.length).toBe(20)
      expect(backwardsPage1.edges[0].node.c).toBe("0")
      expect(backwardsPage1.edges[19].node.c).toBe("19")
    })

    test("it respects a limit", async () => {
      const items = Array.from({ length: 60 }).map(
        (_, i) =>
          new C({ pk: "PK", sk: String(i).padStart(3, "0"), c: String(i) })
      )

      await sandbox.seed(...items)

      // Forwards
      const page = await C.paginate(
        { first: 10 },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDkifQ==",
          "hasNextPage": true,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDAifQ==",
        }
      `)
      expect(page.edges.length).toBe(10)
      expect(page.edges[0].node.c).toBe("0")
      expect(page.edges[9].node.c).toBe("9")
    })

    test("it doesn't exceed the max limit", async () => {
      const items = Array.from({ length: 60 }).map(
        (_, i) =>
          new C({ pk: "PK", sk: String(i).padStart(3, "0"), c: String(i) })
      )

      await sandbox.seed(...items)

      // Forwards
      const page1 = await C.paginate(
        { first: 60 },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page1.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwNDkifQ==",
          "hasNextPage": true,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDAifQ==",
        }
      `)
      expect(page1.edges.length).toBe(50)
      expect(page1.edges[0].node.c).toBe("0")
      expect(page1.edges[49].node.c).toBe("49")
    })

    test("it respects custom pagination default", async () => {
      client.paginationOptions = {
        default: 40
      }

      const items = Array.from({ length: 50 }).map(
        (_, i) =>
          new C({ pk: "PK", sk: String(i).padStart(3, "0"), c: String(i) })
      )

      await sandbox.seed(...items)

      const page = await C.paginate(
        {},
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page.edges.length).toBe(40)

      delete client.paginationOptions
    })

    test("it respects custom pagination limit", async () => {
      client.paginationOptions = {
        limit: 100
      }

      const items = Array.from({ length: 120 }).map(
        (_, i) =>
          new C({ pk: "PK", sk: String(i).padStart(3, "0"), c: String(i) })
      )

      await sandbox.seed(...items)

      const page = await C.paginate(
        { first: 110 },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page.edges.length).toBe(100)

      delete client.paginationOptions
    })
  })

  describe("union", () => {
    test("it paginates a union model", async () => {
      const items = Array.from({ length: 60 }).map((_, i) =>
        i > 30
          ? new C({ pk: "PK", sk: String(i).padStart(3, "0"), c: String(i) })
          : new D({ pk: "PK", sk: String(i).padStart(3, "0"), d: String(i) })
      )

      await sandbox.seed(...items)

      // Forwards
      const page1 = await Union.paginate(
        {},
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page1.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMTkifQ==",
          "hasNextPage": true,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDAifQ==",
        }
      `)
      expect(page1.edges.length).toBe(20)
      expect(page1.edges[0].node.SK).toBe("000")
      expect(page1.edges[19].node.SK).toBe("019")

      const page2 = await Union.paginate(
        { after: page1.pageInfo.endCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page2.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMzkifQ==",
          "hasNextPage": true,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMjAifQ==",
        }
      `)
      expect(page2.edges.length).toBe(20)
      expect(page2.edges[0].node.SK).toBe("020")
      expect(page2.edges[19].node.SK).toBe("039")

      const page3 = await Union.paginate(
        { after: page2.pageInfo.endCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page3.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwNTkifQ==",
          "hasNextPage": false,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwNDAifQ==",
        }
      `)
      expect(page3.edges.length).toBe(20)
      expect(page3.edges[0].node.SK).toBe("040")
      expect(page3.edges[19].node.SK).toBe("059")

      // Backwards
      const backwardsPage2 = await Union.paginate(
        { before: page3.pageInfo.startCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(backwardsPage2.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMzkifQ==",
          "hasNextPage": false,
          "hasPreviousPage": true,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMjAifQ==",
        }
      `)
      expect(backwardsPage2.edges.length).toBe(20)
      expect(backwardsPage2.edges[0].node.SK).toBe("020")
      expect(backwardsPage2.edges[19].node.SK).toBe("039")

      const backwardsPage1 = await Union.paginate(
        { before: backwardsPage2.pageInfo.startCursor },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(backwardsPage1.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMTkifQ==",
          "hasNextPage": false,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDAifQ==",
        }
      `)
      expect(backwardsPage1.edges.length).toBe(20)
      expect(backwardsPage1.edges[0].node.SK).toBe("000")
      expect(backwardsPage1.edges[19].node.SK).toBe("019")
    })

    test("it respects a limit", async () => {
      const items = Array.from({ length: 60 }).map((_, i) =>
        i > 30
          ? new C({ pk: "PK", sk: String(i).padStart(3, "0"), c: String(i) })
          : new D({ pk: "PK", sk: String(i).padStart(3, "0"), d: String(i) })
      )

      await sandbox.seed(...items)

      // Forwards
      const page = await Union.paginate(
        { first: 10 },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDkifQ==",
          "hasNextPage": true,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDAifQ==",
        }
      `)
      expect(page.edges.length).toBe(10)
      expect(page.edges[0].node.SK).toBe("000")
      expect(page.edges[9].node.SK).toBe("009")
    })

    test("it doesn't exceed the max limit", async () => {
      const items = Array.from({ length: 60 }).map((_, i) =>
        i > 30
          ? new C({ pk: "PK", sk: String(i).padStart(3, "0"), c: String(i) })
          : new D({ pk: "PK", sk: String(i).padStart(3, "0"), d: String(i) })
      )

      await sandbox.seed(...items)

      // Forwards
      const page1 = await Union.paginate(
        { first: 60 },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page1.pageInfo).toMatchInlineSnapshot(`
        Object {
          "endCursor": "eyJQSyI6IlBLIiwiU0siOiIwNDkifQ==",
          "hasNextPage": true,
          "hasPreviousPage": false,
          "startCursor": "eyJQSyI6IlBLIiwiU0siOiIwMDAifQ==",
        }
      `)
      expect(page1.edges.length).toBe(50)
      expect(page1.edges[0].node.SK).toBe("000")
      expect(page1.edges[49].node.SK).toBe("049")
    })

    test("it respects custom pagination default", async () => {
      client.paginationOptions = {
        default: 40
      }

      const items = Array.from({ length: 50 }).map((_, i) =>
        i > 30
          ? new C({
              pk: "PK",
              sk: String(i).padStart(3, "0"),
              c: String(i)
            })
          : new D({
              pk: "PK",
              sk: String(i).padStart(3, "0"),
              d: String(i)
            })
      )

      await sandbox.seed(...items)

      const page = await Union.paginate(
        {},
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page.edges.length).toBe(40)

      delete client.paginationOptions
    })

    test("it respects custom pagination limit", async () => {
      client.paginationOptions = {
        limit: 100
      }

      const items = Array.from({ length: 110 }).map((_, i) =>
        i > 30
          ? new C({
              pk: "PK",
              sk: String(i).padStart(3, "0"),
              c: String(i)
            })
          : new D({
              pk: "PK",
              sk: String(i).padStart(3, "0"),
              d: String(i)
            })
      )

      await sandbox.seed(...items)

      const page = await Union.paginate(
        { first: 110 },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" }
        }
      )
      expect(page.edges.length).toBe(100)

      delete client.paginationOptions
    })
  })
})

describe("sandbox", () => {
  test("it seeds a lot of items", async () => {
    await sandbox.seed(
      ...Array.from({ length: 3000 }).map(
        (_, i) => new A({ pk: "PK", sk: String(i).padStart(3, "0"), a: i })
      )
    )
  })
})
