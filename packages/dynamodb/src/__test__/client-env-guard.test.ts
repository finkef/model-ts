import { Client } from "../client"

describe("client env guard", () => {
  const originalNodeEnv = process.env.NODE_ENV
  const originalInMemory = process.env.EXPERIMENTAL_DYNAMODB_IN_MEMORY

  afterEach(() => {
    if (typeof originalNodeEnv === "undefined") delete process.env.NODE_ENV
    else process.env.NODE_ENV = originalNodeEnv

    if (typeof originalInMemory === "undefined")
      delete process.env.EXPERIMENTAL_DYNAMODB_IN_MEMORY
    else process.env.EXPERIMENTAL_DYNAMODB_IN_MEMORY = originalInMemory
  })

  test("allows in-memory mode in test env", () => {
    process.env.NODE_ENV = "test"
    process.env.EXPERIMENTAL_DYNAMODB_IN_MEMORY = "1"

    expect(() => new Client({ tableName: "table" })).not.toThrow()
  })

  test("throws when in-memory mode is enabled outside test env", () => {
    process.env.NODE_ENV = "development"
    process.env.EXPERIMENTAL_DYNAMODB_IN_MEMORY = "1"

    expect(() => new Client({ tableName: "table" })).toThrow(
      'EXPERIMENTAL_DYNAMODB_IN_MEMORY=1 is only allowed when NODE_ENV is "test"'
    )
  })
})
