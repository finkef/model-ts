import * as t from "io-ts"
import { model } from "../model"
import { RuntimeTypeValidationError } from "../runtime-type-validation-error"
import { ModelConstructor } from "../utils"

const SIMPLE_CODEC = t.type({ foo: t.number })

test("it converts the codec to a `t.exact` type", () => {
  class MyModel extends model("MyModel", SIMPLE_CODEC) {}

  expect(MyModel._codec._tag).toEqual("ExactType")
  expect(MyModel._codec.props).toEqual((t.exact(SIMPLE_CODEC) as any).props)
})

describe("without providers", () => {
  class MyModel extends model("MyModel", SIMPLE_CODEC) {}

  describe("from", () => {
    test("it decodes a valid value", () => {
      const decoded = MyModel.from({ foo: 42, bar: "omitted" })
      expect(decoded).toBeInstanceOf(MyModel)
      expect(decoded).toMatchInlineSnapshot(`
        {
          "_tag": "MyModel",
          "foo": 42,
        }
      `)
    })

    test("it throws on invalid values", () => {
      expect(() => MyModel.from(null)).toThrow(RuntimeTypeValidationError)
      expect(() => MyModel.from(undefined)).toThrow(RuntimeTypeValidationError)
      expect(() => (MyModel.from as any)()).toThrow(RuntimeTypeValidationError)
      expect(() => MyModel.from({ foo: "not a number" })).toThrow(
        RuntimeTypeValidationError
      )
      expect(() => MyModel.from({})).toThrow(RuntimeTypeValidationError)
    })
  })

  describe("new", () => {
    test("it creates a new instance", () => {
      const decoded = new MyModel({ foo: 42 })
      expect(decoded).toBeInstanceOf(MyModel)
      expect(decoded).toMatchInlineSnapshot(`
        {
          "_tag": "MyModel",
          "foo": 42,
        }
      `)
    })

    test("it skips the runtime type check", () => {
      expect(() => new MyModel(null as any)).not.toThrow()
      expect(() => new MyModel(undefined as any)).not.toThrow()
      // @ts-ignore
      expect(() => new MyModel()).not.toThrow()
      expect(() => new MyModel({ foo: "not a number" } as any)).not.toThrow()
      expect(() => new MyModel({} as any)).not.toThrow()
    })
  })

  describe("encode", () => {
    test("it encodes a value", () => {
      expect(MyModel.from({ foo: 432 }).encode()).toEqual({
        foo: 432,
        _tag: MyModel._tag
      })
    })

    test("it omits extraneous fields", () => {
      expect(
        new MyModel({ foo: 432, bar: "omitted" } as any).encode()
      ).toEqual({ foo: 432, _tag: MyModel._tag })
    })
  })
})

describe("with provider", () => {
  const PROVIDER = {
    classProps: {
      getDoubleTag<C extends ModelConstructor>(this: C) {
        return this._tag.repeat(2)
      },
      constValue: 42,
      get doubleConstValue() {
        return this.constValue * 2
      }
    },
    instanceProps: {
      getFooString<C extends { foo: number }>(this: C) {
        return String(this.foo)
      },
      num: 33,
      get tripleNum() {
        return this.num * 3
      }
    }
  }

  class MyModel extends model("MyModel", SIMPLE_CODEC, PROVIDER) {}

  test("it injects class props", () => {
    expect(MyModel.getDoubleTag()).toEqual("MyModelMyModel")
    expect(MyModel.constValue).toEqual(42)
    expect(MyModel.doubleConstValue).toEqual(84)
  })

  describe("from", () => {
    test("it decodes a valid value and injects instance props", () => {
      const decoded = MyModel.from({ foo: 42, bar: "omitted" })
      expect(decoded).toBeInstanceOf(MyModel)
      expect(decoded).toMatchInlineSnapshot(`
        {
          "_tag": "MyModel",
          "foo": 42,
        }
      `)

      expect(decoded.getFooString()).toEqual("42")
      expect(decoded.num).toEqual(33)
      expect(decoded.tripleNum).toEqual(99)
    })

    test("it throws on invalid values", () => {
      expect(() => MyModel.from(null)).toThrow(RuntimeTypeValidationError)
      expect(() => MyModel.from(undefined)).toThrow(RuntimeTypeValidationError)
      expect(() => (MyModel.from as any)()).toThrow(RuntimeTypeValidationError)
      expect(() => MyModel.from({ foo: "not a number" })).toThrow(
        RuntimeTypeValidationError
      )
      expect(() => MyModel.from({})).toThrow(RuntimeTypeValidationError)
    })
  })

  describe("new", () => {
    test("it creates a new instance and injects instance props", () => {
      const decoded = new MyModel({ foo: 42 })
      expect(decoded).toBeInstanceOf(MyModel)

      expect(decoded._tag).toEqual(MyModel._tag)

      expect(decoded).toMatchInlineSnapshot(`
        {
          "_tag": "MyModel",
          "foo": 42,
        }
      `)

      expect(decoded.getFooString()).toEqual("42")
      expect(decoded.num).toEqual(33)
      expect(decoded.tripleNum).toEqual(99)
    })

    test("it skips the runtime type check", () => {
      expect(() => new MyModel(null as any)).not.toThrow()
      expect(() => new MyModel(undefined as any)).not.toThrow()
      // @ts-ignore
      expect(() => new MyModel()).not.toThrow()
      expect(() => new MyModel({ foo: "not a number" } as any)).not.toThrow()
      expect(() => new MyModel({} as any)).not.toThrow()
    })
  })

  describe("encode", () => {
    test("it encodes a value", () => {
      expect(MyModel.from({ foo: 432 }).encode()).toEqual({
        foo: 432,
        _tag: MyModel._tag
      })
    })

    test("it omits extraneous fields", () => {
      expect(
        new MyModel({ foo: 432, bar: "omitted" } as any).encode()
      ).toEqual({ foo: 432, _tag: MyModel._tag })
    })
  })

  test("it returns values", () => {
    expect(new MyModel({ foo: 432, bar: "omitted" } as any).values())
      .toMatchInlineSnapshot(`
      {
        "foo": 432,
      }
    `)
  })
})

describe("as io-ts codec", () => {
  class MyModel extends model("MyModel", SIMPLE_CODEC) {}

  test("it decodes", () => {
    expect(t.type({ model: MyModel }).decode({ model: { foo: 42 } }))
      .toMatchInlineSnapshot(`
      {
        "_tag": "Right",
        "right": {
          "model": {
            "_tag": "MyModel",
            "foo": 42,
          },
        },
      }
    `)
  })

  test("it fails", () => {
    expect(t.type({ model: MyModel }).decode({ model: "something else" }))
      .toMatchInlineSnapshot(`
      {
        "_tag": "Left",
        "left": [
          {
            "context": [
              {
                "actual": {
                  "model": "something else",
                },
                "key": "",
                "type": InterfaceType {
                  "_tag": "InterfaceType",
                  "decode": [Function],
                  "encode": [Function],
                  "is": [Function],
                  "name": "{ model: MyModel }",
                  "props": {
                    "model": [Function],
                  },
                  "validate": [Function],
                },
              },
              {
                "actual": "something else",
                "key": "model",
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
