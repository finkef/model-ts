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
  BulkWriteTransactionError,
} from "../errors"

const client = new Client({ tableName: "table" })
const provider = getProvider(client)

const SIMPLE_CODEC = t.type({
  foo: t.string,
  bar: t.number,
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
        Snapshot Diff:
        - First value
        + Second value

        - Object {}
        + Object {
        +   "PK#hi__SK#42": Object {
        +     "PK": "PK#hi",
        +     "SK": "SK#42",
        +     "_docVersion": 0,
        +     "_tag": "Simple",
        +     "bar": 42,
        +     "foo": "hi",
        +   },
        + }
      `)
    })

    test("it inserts a model with single gsi", async () => {
      const before = await sandbox.snapshot()

      await new SingleGSI({ foo: "yes", bar: 42 }).put()

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
        Snapshot Diff:
        - First value
        + Second value

        - Object {}
        + Object {
        +   "PK#yes__SK#42": Object {
        +     "GSI2PK": "GSI2PK#yesyes",
        +     "GSI2SK": "GSI2SK#FIXED",
        +     "PK": "PK#yes",
        +     "SK": "SK#42",
        +     "_docVersion": 0,
        +     "_tag": "SingleGSI",
        +     "bar": 42,
        +     "foo": "yes",
        +   },
        + }
      `)
    })

    test("it inserts a model with multiple gsi", async () => {
      const before = await sandbox.snapshot()

      await new MultiGSI({ foo: "yes", bar: 42 }).put()

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
        Snapshot Diff:
        - First value
        + Second value

        - Object {}
        + Object {
        +   "PK#yes__SK#42": Object {
        +     "GSI2PK": "GSI2PK#yesyes",
        +     "GSI2SK": "GSI2SK#FIXED",
        +     "GSI3PK": "GSI3PK#FIXED",
        +     "GSI3SK": "GSI3SK#4242",
        +     "GSI4PK": "GSI4PK#FIXED",
        +     "GSI4SK": "GSI4SK#4242",
        +     "GSI5PK": "GSI5PK#FIXED",
        +     "GSI5SK": "GSI5SK#4242",
        +     "PK": "PK#yes",
        +     "SK": "SK#42",
        +     "_docVersion": 0,
        +     "_tag": "MultiGSI",
        +     "bar": 42,
        +     "foo": "yes",
        +   },
        + }
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
        Snapshot Diff:
        - First value
        + Second value

        - Object {}
        + Object {
        +   "PK#hi__SK#42": Object {
        +     "PK": "PK#hi",
        +     "SK": "SK#42",
        +     "_docVersion": 0,
        +     "_tag": "Simple",
        +     "bar": 42,
        +     "foo": "hi",
        +   },
        + }
      `)
    })

    test("it inserts a model with single gsi", async () => {
      const before = await sandbox.snapshot()

      await SingleGSI.put(new SingleGSI({ foo: "yes", bar: 42 }))

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
        Snapshot Diff:
        - First value
        + Second value

        - Object {}
        + Object {
        +   "PK#yes__SK#42": Object {
        +     "GSI2PK": "GSI2PK#yesyes",
        +     "GSI2SK": "GSI2SK#FIXED",
        +     "PK": "PK#yes",
        +     "SK": "SK#42",
        +     "_docVersion": 0,
        +     "_tag": "SingleGSI",
        +     "bar": 42,
        +     "foo": "yes",
        +   },
        + }
      `)
    })

    test("it inserts a model with multiple gsi", async () => {
      const before = await sandbox.snapshot()

      await MultiGSI.put(new MultiGSI({ foo: "yes", bar: 42 }))

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
        Snapshot Diff:
        - First value
        + Second value

        - Object {}
        + Object {
        +   "PK#yes__SK#42": Object {
        +     "GSI2PK": "GSI2PK#yesyes",
        +     "GSI2SK": "GSI2SK#FIXED",
        +     "GSI3PK": "GSI3PK#FIXED",
        +     "GSI3SK": "GSI3SK#4242",
        +     "GSI4PK": "GSI4PK#FIXED",
        +     "GSI4SK": "GSI4SK#4242",
        +     "GSI5PK": "GSI5PK#FIXED",
        +     "GSI5SK": "GSI5SK#4242",
        +     "PK": "PK#yes",
        +     "SK": "SK#42",
        +     "_docVersion": 0,
        +     "_tag": "MultiGSI",
        +     "bar": 42,
        +     "foo": "yes",
        +   },
        + }
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
          IgnoreExistence: true,
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
        SK: item.keys().SK,
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
          SK: item.keys().SK,
        },
      })

      expect(result).toBeNull()

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
        Snapshot Diff:
        - First value
        + Second value

        - Object {
        -   "PK#hi__SK#432": Object {
        -     "PK": "PK#hi",
        -     "SK": "SK#432",
        -     "_docVersion": 0,
        -     "_tag": "Simple",
        -     "bar": 432,
        -     "foo": "hi",
        -   },
        - }
        + Object {}
      `)
    })
  })

  describe("via model", () => {
    test("it deletes the item and returns null", async () => {
      const item = await new Simple({ foo: "hi", bar: 432 }).put()

      const before = await sandbox.snapshot()

      const result = await Simple.delete({
        PK: item.keys().PK,
        SK: item.keys().SK,
      })

      expect(result).toBeNull()

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
        Snapshot Diff:
        - First value
        + Second value

        - Object {
        -   "PK#hi__SK#432": Object {
        -     "PK": "PK#hi",
        -     "SK": "SK#432",
        -     "_docVersion": 0,
        -     "_tag": "Simple",
        -     "bar": 432,
        -     "foo": "hi",
        -   },
        - }
        + Object {}
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
        Snapshot Diff:
        - First value
        + Second value

        - Object {
        -   "PK#hi__SK#432": Object {
        -     "PK": "PK#hi",
        -     "SK": "SK#432",
        -     "_docVersion": 0,
        -     "_tag": "Simple",
        -     "bar": 432,
        -     "foo": "hi",
        -   },
        - }
        + Object {}
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
        Snapshot Diff:
        - First value
        + Second value

        @@ -1,25 +1,27 @@
          Object {
        -   "PK#hello__SK#42": Object {
        -     "GSI2PK": "GSI2PK#hellohello",
        -     "GSI2SK": "GSI2SK#FIXED",
        -     "GSI3PK": "GSI3PK#FIXED",
        -     "GSI3SK": "GSI3SK#4242",
        -     "GSI4PK": "GSI4PK#FIXED",
        -     "GSI4SK": "GSI4SK#4242",
        -     "GSI5PK": "GSI5PK#FIXED",
        -     "GSI5SK": "GSI5SK#4242",
        -     "PK": "PK#hello",
        -     "SK": "SK#42",
        +   "$$DELETED$$PK#hello__$$DELETED$$SK#42": Object {
        +     "GSI2PK": "$$DELETED$$GSI2PK#hellohello",
        +     "GSI2SK": "$$DELETED$$GSI2SK#FIXED",
        +     "GSI3PK": "$$DELETED$$GSI3PK#FIXED",
        +     "GSI3SK": "$$DELETED$$GSI3SK#4242",
        +     "GSI4PK": "$$DELETED$$GSI4PK#FIXED",
        +     "GSI4SK": "$$DELETED$$GSI4SK#4242",
        +     "GSI5PK": "$$DELETED$$GSI5PK#FIXED",
        +     "GSI5SK": "$$DELETED$$GSI5SK#4242",
        +     "PK": "$$DELETED$$PK#hello",
        +     "SK": "$$DELETED$$SK#42",
        +     "_deletedAt": "2021-05-01T08:00:00.000Z",
              "_docVersion": 0,
              "_tag": "MultiGSI",
              "bar": 42,
              "foo": "hello",
            },
        -   "PK#hi__SK#432": Object {
        -     "PK": "PK#hi",
        -     "SK": "SK#432",
        +   "$$DELETED$$PK#hi__$$DELETED$$SK#432": Object {
        +     "PK": "$$DELETED$$PK#hi",
        +     "SK": "$$DELETED$$SK#432",
        +     "_deletedAt": "2021-05-01T08:00:00.000Z",
              "_docVersion": 0,
              "_tag": "Simple",
              "bar": 432,
              "foo": "hi",
            },
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
        Snapshot Diff:
        - First value
        + Second value

        @@ -1,9 +1,10 @@
          Object {
        -   "PK#hi__SK#432": Object {
        -     "PK": "PK#hi",
        -     "SK": "SK#432",
        +   "$$DELETED$$PK#hi__$$DELETED$$SK#432": Object {
        +     "PK": "$$DELETED$$PK#hi",
        +     "SK": "$$DELETED$$SK#432",
        +     "_deletedAt": "2021-05-01T08:00:00.000Z",
              "_docVersion": 0,
              "_tag": "Simple",
              "bar": 432,
              "foo": "hi",
            },
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
        Snapshot Diff:
        - First value
        + Second value

        @@ -1,9 +1,10 @@
          Object {
        -   "PK#hi__SK#432": Object {
        -     "PK": "PK#hi",
        -     "SK": "SK#432",
        +   "$$DELETED$$PK#hi__$$DELETED$$SK#432": Object {
        +     "PK": "$$DELETED$$PK#hi",
        +     "SK": "$$DELETED$$SK#432",
        +     "_deletedAt": "2021-05-01T08:00:00.000Z",
              "_docVersion": 0,
              "_tag": "Simple",
              "bar": 432,
              "foo": "hi",
            },
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
        Snapshot Diff:
        - First value
        + Second value

          Object {
            "FIXEDPK__FIXEDSK": Object {
              "PK": "FIXEDPK",
              "SK": "FIXEDSK",
        -     "_docVersion": 0,
        +     "_docVersion": 1,
              "_tag": "InPlace",
              "bar": 1,
        -     "foo": "hello",
        +     "foo": "ciao",
            },
          }
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
      Snapshot Diff:
      - First value
      + Second value

        Object {
          "PK__SK": Object {
            "PK": "PK",
            "SK": "SK",
      -     "_docVersion": 0,
      +     "_docVersion": 1,
            "_tag": "A",
      -     "a": 1,
      +     "a": 2,
            "pk": "PK",
            "sk": "SK",
          },
        }
    `)
  })
})

describe("query", () => {
  test("it returns empty results", async () => {
    expect(
      await client.query(
        {
          KeyConditionExpression: `PK = :pk and begins_with(SK, :sk)`,
          ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT" },
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
          ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT#" },
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
        ExpressionAttributeValues: { ":pk": "abc", ":sk": "SORT#" },
      },
      { a: A, b: B, union: Union }
    )

    expect({
      meta: meta,
      _unknown: _unknown,
      a: a.map((item) => item.values()),
      b: b.map((item) => item.values()),
      union: union.map((item) => item.values()),
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
        Limit: 30,
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
        ExclusiveStartKey: firstPage.meta.lastEvaluatedKey,
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
        Limit: 10,
      },
      { a: A, b: B }
    )

    expect(a.length).toBe(20)
    expect(b.length).toBe(20)
    expect(_unknown.length).toBe(0)
    expect(meta.lastEvaluatedKey).toBeUndefined()
  })
})

describe("bulk", () => {
  describe("< 25 elements (true transaction)", () => {
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
          b: "bar",
        }).operation("update", { b: "baz" }),
        new B({
          pk: "PK#COND",
          sk: "SK#COND",
          b: "cond",
        }).operation("condition", {
          ConditionExpression: "b = :cond",
          ExpressionAttributeValues: { ":cond": "cond" },
        }),
      ])

      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
        Snapshot Diff:
        - First value
        + Second value

        @@ -1,32 +1,24 @@
          Object {
        +   "$$DELETED$$PK#3__$$DELETED$$SK#3": Object {
        +     "PK": "$$DELETED$$PK#3",
        +     "SK": "$$DELETED$$SK#3",
        +     "_deletedAt": "2021-05-01T08:00:00.000Z",
        +     "_docVersion": 0,
        +     "_tag": "B",
        +     "b": "bar",
        +     "pk": "PK#3",
        +     "sk": "SK#3",
        +   },
            "PK#1__SK#1": Object {
              "PK": "PK#1",
              "SK": "SK#1",
              "_docVersion": 0,
              "_tag": "A",
        -     "a": 1,
        +     "a": -1,
              "pk": "PK#1",
              "sk": "SK#1",
        -   },
        -   "PK#2__SK#2": Object {
        -     "PK": "PK#2",
        -     "SK": "SK#2",
        -     "_docVersion": 0,
        -     "_tag": "A",
        -     "a": 2,
        -     "pk": "PK#2",
        -     "sk": "SK#2",
        -   },
        -   "PK#3__SK#3": Object {
        -     "PK": "PK#3",
        -     "SK": "SK#3",
        -     "_docVersion": 0,
        -     "_tag": "B",
        -     "b": "bar",
        -     "pk": "PK#3",
        -     "sk": "SK#3",
            },
            "PK#COND__SK#COND": Object {
              "PK": "PK#COND",
              "SK": "SK#COND",
              "_docVersion": 0,
        @@ -36,12 +28,39 @@
              "sk": "SK#COND",
            },
            "PK#UPDATE__SK#UPDATE": Object {
              "PK": "PK#UPDATE",
              "SK": "SK#UPDATE",
        -     "_docVersion": 0,
        +     "_docVersion": 1,
              "_tag": "B",
        -     "b": "bar",
        +     "b": "baz",
              "pk": "PK#UPDATE",
              "sk": "SK#UPDATE",
        +   },
        +   "PK4__PK4": Object {
        +     "PK": "PK4",
        +     "SK": "PK4",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 4,
        +     "pk": "PK4",
        +     "sk": "PK4",
        +   },
        +   "PK5__PK5": Object {
        +     "PK": "PK5",
        +     "SK": "PK5",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 5,
        +     "pk": "PK5",
        +     "sk": "PK5",
        +   },
        +   "PK6__SK6": Object {
        +     "PK": "PK6",
        +     "SK": "SK6",
        +     "_docVersion": 0,
        +     "_tag": "B",
        +     "b": "baz",
        +     "pk": "PK6",
        +     "sk": "SK6",
            },
          }
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
          ),
        ])
      ).rejects.toBeInstanceOf(BulkWriteTransactionError)

      expect(await sandbox.snapshot()).toEqual(before)
    })
  })

  describe("> 25 items (pseudo transaction)", () => {
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
          b: "bar",
        }).operation("update", { b: "baz" }),
        ...Array.from({ length: 25 }).map((_, i) =>
          new A({ pk: `PK#A${i}`, sk: `SK#A${i}`, a: i }).operation("put")
        ),
      ])

      //#region snapshot
      expect(await sandbox.diff(before)).toMatchInlineSnapshot(`
        Snapshot Diff:
        - First value
        + Second value

        @@ -2,28 +2,271 @@
            "PK#1__SK#1": Object {
              "PK": "PK#1",
              "SK": "SK#1",
              "_docVersion": 0,
              "_tag": "A",
        -     "a": 1,
        +     "a": -1,
              "pk": "PK#1",
              "sk": "SK#1",
        +   },
        +   "PK#A0__SK#A0": Object {
        +     "PK": "PK#A0",
        +     "SK": "SK#A0",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 0,
        +     "pk": "PK#A0",
        +     "sk": "SK#A0",
        +   },
        +   "PK#A10__SK#A10": Object {
        +     "PK": "PK#A10",
        +     "SK": "SK#A10",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 10,
        +     "pk": "PK#A10",
        +     "sk": "SK#A10",
        +   },
        +   "PK#A11__SK#A11": Object {
        +     "PK": "PK#A11",
        +     "SK": "SK#A11",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 11,
        +     "pk": "PK#A11",
        +     "sk": "SK#A11",
        +   },
        +   "PK#A12__SK#A12": Object {
        +     "PK": "PK#A12",
        +     "SK": "SK#A12",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 12,
        +     "pk": "PK#A12",
        +     "sk": "SK#A12",
        +   },
        +   "PK#A13__SK#A13": Object {
        +     "PK": "PK#A13",
        +     "SK": "SK#A13",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 13,
        +     "pk": "PK#A13",
        +     "sk": "SK#A13",
        +   },
        +   "PK#A14__SK#A14": Object {
        +     "PK": "PK#A14",
        +     "SK": "SK#A14",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 14,
        +     "pk": "PK#A14",
        +     "sk": "SK#A14",
            },
        -   "PK#2__SK#2": Object {
        -     "PK": "PK#2",
        -     "SK": "SK#2",
        +   "PK#A15__SK#A15": Object {
        +     "PK": "PK#A15",
        +     "SK": "SK#A15",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 15,
        +     "pk": "PK#A15",
        +     "sk": "SK#A15",
        +   },
        +   "PK#A16__SK#A16": Object {
        +     "PK": "PK#A16",
        +     "SK": "SK#A16",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 16,
        +     "pk": "PK#A16",
        +     "sk": "SK#A16",
        +   },
        +   "PK#A17__SK#A17": Object {
        +     "PK": "PK#A17",
        +     "SK": "SK#A17",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 17,
        +     "pk": "PK#A17",
        +     "sk": "SK#A17",
        +   },
        +   "PK#A18__SK#A18": Object {
        +     "PK": "PK#A18",
        +     "SK": "SK#A18",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 18,
        +     "pk": "PK#A18",
        +     "sk": "SK#A18",
        +   },
        +   "PK#A19__SK#A19": Object {
        +     "PK": "PK#A19",
        +     "SK": "SK#A19",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 19,
        +     "pk": "PK#A19",
        +     "sk": "SK#A19",
        +   },
        +   "PK#A1__SK#A1": Object {
        +     "PK": "PK#A1",
        +     "SK": "SK#A1",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 1,
        +     "pk": "PK#A1",
        +     "sk": "SK#A1",
        +   },
        +   "PK#A20__SK#A20": Object {
        +     "PK": "PK#A20",
        +     "SK": "SK#A20",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 20,
        +     "pk": "PK#A20",
        +     "sk": "SK#A20",
        +   },
        +   "PK#A21__SK#A21": Object {
        +     "PK": "PK#A21",
        +     "SK": "SK#A21",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 21,
        +     "pk": "PK#A21",
        +     "sk": "SK#A21",
        +   },
        +   "PK#A22__SK#A22": Object {
        +     "PK": "PK#A22",
        +     "SK": "SK#A22",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 22,
        +     "pk": "PK#A22",
        +     "sk": "SK#A22",
        +   },
        +   "PK#A23__SK#A23": Object {
        +     "PK": "PK#A23",
        +     "SK": "SK#A23",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 23,
        +     "pk": "PK#A23",
        +     "sk": "SK#A23",
        +   },
        +   "PK#A24__SK#A24": Object {
        +     "PK": "PK#A24",
        +     "SK": "SK#A24",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 24,
        +     "pk": "PK#A24",
        +     "sk": "SK#A24",
        +   },
        +   "PK#A2__SK#A2": Object {
        +     "PK": "PK#A2",
        +     "SK": "SK#A2",
              "_docVersion": 0,
              "_tag": "A",
              "a": 2,
        -     "pk": "PK#2",
        -     "sk": "SK#2",
        +     "pk": "PK#A2",
        +     "sk": "SK#A2",
        +   },
        +   "PK#A3__SK#A3": Object {
        +     "PK": "PK#A3",
        +     "SK": "SK#A3",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 3,
        +     "pk": "PK#A3",
        +     "sk": "SK#A3",
        +   },
        +   "PK#A4__SK#A4": Object {
        +     "PK": "PK#A4",
        +     "SK": "SK#A4",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 4,
        +     "pk": "PK#A4",
        +     "sk": "SK#A4",
        +   },
        +   "PK#A5__SK#A5": Object {
        +     "PK": "PK#A5",
        +     "SK": "SK#A5",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 5,
        +     "pk": "PK#A5",
        +     "sk": "SK#A5",
        +   },
        +   "PK#A6__SK#A6": Object {
        +     "PK": "PK#A6",
        +     "SK": "SK#A6",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 6,
        +     "pk": "PK#A6",
        +     "sk": "SK#A6",
        +   },
        +   "PK#A7__SK#A7": Object {
        +     "PK": "PK#A7",
        +     "SK": "SK#A7",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 7,
        +     "pk": "PK#A7",
        +     "sk": "SK#A7",
        +   },
        +   "PK#A8__SK#A8": Object {
        +     "PK": "PK#A8",
        +     "SK": "SK#A8",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 8,
        +     "pk": "PK#A8",
        +     "sk": "SK#A8",
        +   },
        +   "PK#A9__SK#A9": Object {
        +     "PK": "PK#A9",
        +     "SK": "SK#A9",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 9,
        +     "pk": "PK#A9",
        +     "sk": "SK#A9",
            },
        -   "PK#3__SK#3": Object {
        -     "PK": "PK#3",
        -     "SK": "SK#3",
        +   "PK#UPDATE__SK#UPDATE": Object {
        +     "PK": "PK#UPDATE",
        +     "SK": "SK#UPDATE",
        +     "_docVersion": 1,
        +     "_tag": "B",
        +     "b": "baz",
        +     "pk": "PK#UPDATE",
        +     "sk": "SK#UPDATE",
        +   },
        +   "PK4__PK4": Object {
        +     "PK": "PK4",
        +     "SK": "PK4",
        +     "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 4,
        +     "pk": "PK4",
        +     "sk": "PK4",
        +   },
        +   "PK5__PK5": Object {
        +     "PK": "PK5",
        +     "SK": "PK5",
              "_docVersion": 0,
        +     "_tag": "A",
        +     "a": 5,
        +     "pk": "PK5",
        +     "sk": "PK5",
        +   },
        +   "PK6__SK6": Object {
        +     "PK": "PK6",
        +     "SK": "SK6",
        +     "_docVersion": 0,
              "_tag": "B",
        -     "b": "bar",
        -     "pk": "PK#3",
        -     "sk": "SK#3",
        +     "b": "baz",
        +     "pk": "PK6",
        +     "sk": "SK6",
            },
          }
      `)
      //#endregion
    })

    test("it fails and rolls back", async () => {
      const before = await sandbox.snapshot()

      await expect(
        client.bulk([
          // Succeeds
          ...Array.from({ length: 40 }).map((_, i) =>
            new A({ pk: `PK#${i}`, sk: `SK#${i}`, a: i }).operation("put")
          ),

          // Fails
          A.operation(
            "condition",
            { PK: "nicetry", SK: "nope" },
            { ConditionExpression: "attribute_exists(PK)" }
          ),
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
        duplicate: A.operation("get", { PK: "PK#1", SK: "SK#1" }),
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
        error2: A.operation("get", { PK: "PK#error2", SK: "SK#error2" }),
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
      duplicate: A.operation("get", { PK: "PK#1", SK: "SK#1" }),
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
          recover: true,
        }
      )

      expect(recovered).toBeInstanceOf(A)
      expect(recovered.isDeleted).toBe(true)
    })

    test("it throws if no item or soft deleted item exists", async () => {
      await expect(
        client.load(A.operation("get", { PK: "PK", SK: "SK" }), {
          recover: true,
        })
      ).rejects.toBeInstanceOf(ItemNotFoundError)
    })

    test("it returns null instead of throwing if no item or soft deleted item exists", async () => {
      await expect(
        client.load(A.operation("get", { PK: "PK", SK: "SK" }), {
          recover: true,
          null: true,
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
      expect(results.filter((item) => item instanceof C).length).toBe(123)
      expect(results.filter((item) => item instanceof D).length).toBe(111)
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
      expect(results.filter((item) => item instanceof C).length).toBe(123)
      expect(results.filter((item) => item instanceof D).length).toBe(111)
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
        default: 40,
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
          ExpressionAttributeValues: { ":pk": "PK" },
        }
      )
      expect(page.edges.length).toBe(40)

      delete client.paginationOptions
    })

    test("it respects custom pagination limit", async () => {
      client.paginationOptions = {
        limit: 100,
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
        default: 40,
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
          ExpressionAttributeValues: { ":pk": "PK" },
        }
      )
      expect(page.edges.length).toBe(40)

      delete client.paginationOptions
    })

    test("it respects custom pagination limit", async () => {
      client.paginationOptions = {
        limit: 100,
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
          ExpressionAttributeValues: { ":pk": "PK" },
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
        default: 40,
      }

      const items = Array.from({ length: 50 }).map((_, i) =>
        i > 30
          ? new C({
              pk: "PK",
              sk: String(i).padStart(3, "0"),
              c: String(i),
            })
          : new D({
              pk: "PK",
              sk: String(i).padStart(3, "0"),
              d: String(i),
            })
      )

      await sandbox.seed(...items)

      const page = await Union.paginate(
        {},
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" },
        }
      )
      expect(page.edges.length).toBe(40)

      delete client.paginationOptions
    })

    test("it respects custom pagination limit", async () => {
      client.paginationOptions = {
        limit: 100,
      }

      const items = Array.from({ length: 110 }).map((_, i) =>
        i > 30
          ? new C({
              pk: "PK",
              sk: String(i).padStart(3, "0"),
              c: String(i),
            })
          : new D({
              pk: "PK",
              sk: String(i).padStart(3, "0"),
              d: String(i),
            })
      )

      await sandbox.seed(...items)

      const page = await Union.paginate(
        { first: 110 },
        {
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: { ":pk": "PK" },
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
