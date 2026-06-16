import * as t from "io-ts"
import { model } from "../model"
import { RuntimeTypeValidationError } from "../runtime-type-validation-error"
import { Union, union } from "../union"

describe("without providers", () => {
  class A extends model("A", t.type({ a: t.string })) {}
  class B extends model("B", t.type({ b: t.number })) {}

  class Union extends union([A, B]) {}

  test("it throws if Union is constructed with `new`", () => {
    expect(() => new Union(null as never)).toThrowError()
  })

  describe("decode without `_tag`", () => {
    test("it decodes a value", () => {
      const decodedA = Union.from({ a: "a" })
      expect(decodedA).toBeInstanceOf(A)
      expect(decodedA).toMatchInlineSnapshot(`
        {
          "_tag": "A",
          "a": "a",
        }
      `)

      const decodedB = Union.from({ b: 42, c: "" })
      expect(decodedB).toBeInstanceOf(B)
      expect(decodedB).toMatchInlineSnapshot(`
        {
          "_tag": "B",
          "b": 42,
        }
      `)
    })

    test("it throws an error if it's unable to decode a value", () => {
      expect(() => Union.from("doesn't work")).toThrow(
        RuntimeTypeValidationError
      )
    })

    test("it uses the first matching model in case of ambiguity", () => {
      expect(Union.from({ a: "string", b: 42 })).toBeInstanceOf(A)
    })
  })

  describe("decode with `_tag`", () => {
    test("it decodes a value", () => {
      const decodedA = Union.from({ _tag: "A", a: "a" })
      expect(decodedA).toBeInstanceOf(A)
      expect(decodedA).toMatchInlineSnapshot(`
        {
          "_tag": "A",
          "a": "a",
        }
      `)

      const decodedB = Union.from({ _tag: "B", b: 42, c: "" })
      expect(decodedB).toBeInstanceOf(B)
      expect(decodedB).toMatchInlineSnapshot(`
        {
          "_tag": "B",
          "b": 42,
        }
      `)
    })

    test("it first tries a matching _tag", () => {
      expect(Union.from({ _tag: "B", a: "string", b: 42 })).toBeInstanceOf(B)
    })

    test("it uses the first matching model if the _tag is unknown", () => {
      expect(Union.from({ _tag: "x", a: "string", b: 42 })).toBeInstanceOf(A)
    })

    test("it throws an error if it's unable to decode a value", () => {
      expect(() => Union.from({ _tag: "C", c: "hello" })).toThrow(
        RuntimeTypeValidationError
      )
    })
  })
})

describe("with provider", () => {
  const PROVIDER = {
    unionProps: {
      printModels<T extends Union>(this: T) {
        return this._models.map(({ _tag }) => _tag).join(" | ")
      },
      constValue: 42,
      get doubleConstValue() {
        return this.constValue * 2
      }
    }
  }

  class A extends model("A", t.type({ a: t.string })) {}
  class B extends model("B", t.type({ b: t.number }), PROVIDER) {}
  class U extends union([A, B], PROVIDER) {}

  test("it injects class props", () => {
    expect(U.printModels()).toMatchInlineSnapshot(`"A | B"`)
    expect(U.constValue).toEqual(42)
    expect(U.doubleConstValue).toEqual(84)
  })
})

describe("as io-ts codec", () => {
  class A extends model("A", t.type({ a: t.string })) {}
  class B extends model("B", t.type({ b: t.number })) {}

  class Union extends union([A, B]) {}

  test("it decodes", () => {
    expect(t.type({ union: Union }).decode({ union: { b: 42 } }))
      .toMatchInlineSnapshot(`
      {
        "_tag": "Right",
        "right": {
          "union": {
            "_tag": "B",
            "b": 42,
          },
        },
      }
    `)
  })

  test("it fails", () => {
    expect(t.type({ union: Union }).decode({ union: "something else" }))
      .toMatchInlineSnapshot(`
      {
        "_tag": "Left",
        "left": [
          {
            "context": [
              {
                "actual": {
                  "union": "something else",
                },
                "key": "",
                "type": InterfaceType {
                  "_tag": "InterfaceType",
                  "decode": [Function],
                  "encode": [Function],
                  "is": [Function],
                  "name": "{ union: Union }",
                  "props": {
                    "union": [Function],
                  },
                  "validate": [Function],
                },
              },
              {
                "actual": "something else",
                "key": "union",
                "type": [Function],
              },
              {
                "actual": "something else",
                "key": "0",
                "type": [Function],
              },
            ],
            "message": undefined,
            "value": "something else",
          },
          {
            "context": [
              {
                "actual": {
                  "union": "something else",
                },
                "key": "",
                "type": InterfaceType {
                  "_tag": "InterfaceType",
                  "decode": [Function],
                  "encode": [Function],
                  "is": [Function],
                  "name": "{ union: Union }",
                  "props": {
                    "union": [Function],
                  },
                  "validate": [Function],
                },
              },
              {
                "actual": "something else",
                "key": "union",
                "type": [Function],
              },
              {
                "actual": "something else",
                "key": "1",
                "type": [Function],
              },
            ],
            "message": undefined,
            "value": "something else",
          },
        ],
      }
    `)
  })
})
