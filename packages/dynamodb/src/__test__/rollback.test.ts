import * as t from "io-ts"
import { model } from "@model-ts/core"
import { Sandbox, createSandbox } from "../sandbox"
import { Client } from "../client"
import { getProvider } from "../provider"

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

describe("rollback", () => {
  let sandbox: Sandbox

  beforeAll(async () => {
    sandbox = await createSandbox(client)
    await sandbox.seed(
      new Simple({ foo: "seeded1", bar: 1 }),
      new Simple({ foo: "seeded2", bar: 2 })
    )
  })

  afterAll(async () => {
    await sandbox.destroy()
  })

  test("rolls back newly added items", async () => {
    sandbox.startTracking()

    await new Simple({ foo: "new-item", bar: 99 }).put()
    expect(await sandbox.get("PK#new-item", "SK#99")).not.toBeNull()

    await sandbox.rollback()

    expect(await sandbox.get("PK#new-item", "SK#99")).toBeNull()
    expect(await sandbox.get("PK#seeded1", "SK#1")).not.toBeNull()
    expect(await sandbox.get("PK#seeded2", "SK#2")).not.toBeNull()
  })

  test("rolls back updated items", async () => {
    sandbox.startTracking()

    await Simple.updateRaw(
      { PK: "PK#seeded1", SK: "SK#1" },
      { foo: "modified" }
    )

    const modified = await sandbox.get("PK#seeded1", "SK#1")
    expect(modified.foo).toBe("modified")

    await sandbox.rollback()

    const restored = await sandbox.get("PK#seeded1", "SK#1")
    expect(restored.foo).toBe("seeded1")
  })

  test("rolls back deleted items", async () => {
    sandbox.startTracking()

    await Simple.delete({ PK: "PK#seeded1", SK: "SK#1" })
    expect(await sandbox.get("PK#seeded1", "SK#1")).toBeNull()

    await sandbox.rollback()

    const restored = await sandbox.get("PK#seeded1", "SK#1")
    expect(restored).not.toBeNull()
    expect(restored.foo).toBe("seeded1")
  })

  test("rolls back soft-deleted items", async () => {
    sandbox.startTracking()

    const item = new Simple({ foo: "seeded1", bar: 1 })
    // @ts-ignore - _docVersion needed for softDelete encoding
    item._docVersion = 0
    await client.softDelete(item)

    // Original should be gone, deleted version should exist
    expect(await sandbox.get("PK#seeded1", "SK#1")).toBeNull()
    expect(
      await sandbox.get("$$DELETED$$PK#seeded1", "$$DELETED$$SK#1")
    ).not.toBeNull()

    await sandbox.rollback()

    // Original should be restored, deleted version should be gone
    expect(await sandbox.get("PK#seeded1", "SK#1")).not.toBeNull()
    expect(
      await sandbox.get("$$DELETED$$PK#seeded1", "$$DELETED$$SK#1")
    ).toBeNull()
  })

  test("rolls back mixed operations", async () => {
    sandbox.startTracking()

    // Add a new item
    await new Simple({ foo: "brand-new", bar: 50 }).put()

    // Update an existing item
    await Simple.updateRaw(
      { PK: "PK#seeded1", SK: "SK#1" },
      { foo: "changed" }
    )

    // Delete an existing item
    await Simple.delete({ PK: "PK#seeded2", SK: "SK#2" })

    // Verify all changes took effect
    expect(await sandbox.get("PK#brand-new", "SK#50")).not.toBeNull()
    expect((await sandbox.get("PK#seeded1", "SK#1")).foo).toBe("changed")
    expect(await sandbox.get("PK#seeded2", "SK#2")).toBeNull()

    await sandbox.rollback()

    // New item should be gone
    expect(await sandbox.get("PK#brand-new", "SK#50")).toBeNull()
    // Updated item should be restored
    expect((await sandbox.get("PK#seeded1", "SK#1")).foo).toBe("seeded1")
    // Deleted item should be back
    expect(await sandbox.get("PK#seeded2", "SK#2")).not.toBeNull()
  })

  test("supports multiple tracking/rollback cycles", async () => {
    // Cycle 1
    sandbox.startTracking()
    await new Simple({ foo: "cycle1", bar: 10 }).put()
    await sandbox.rollback()
    expect(await sandbox.get("PK#cycle1", "SK#10")).toBeNull()

    // Cycle 2
    sandbox.startTracking()
    await new Simple({ foo: "cycle2", bar: 20 }).put()
    await sandbox.rollback()
    expect(await sandbox.get("PK#cycle2", "SK#20")).toBeNull()

    // Cycle 3 - modify seeded data
    sandbox.startTracking()
    await Simple.updateRaw(
      { PK: "PK#seeded1", SK: "SK#1" },
      { foo: "cycle3-mod" }
    )
    await sandbox.rollback()
    expect((await sandbox.get("PK#seeded1", "SK#1")).foo).toBe("seeded1")

    // Seeded data is intact throughout
    expect(await sandbox.get("PK#seeded1", "SK#1")).not.toBeNull()
    expect(await sandbox.get("PK#seeded2", "SK#2")).not.toBeNull()
  })

  test("does not track writes before startTracking", async () => {
    // Write without tracking
    await new Simple({ foo: "untracked", bar: 77 }).put()

    // Now start tracking and immediately rollback
    sandbox.startTracking()
    await sandbox.rollback()

    // The untracked write should still be there
    expect(await sandbox.get("PK#untracked", "SK#77")).not.toBeNull()

    // Clean up manually for subsequent tests
    sandbox.startTracking()
    await Simple.delete({ PK: "PK#untracked", SK: "SK#77" })
    await sandbox.rollback()
  })

  test("tracks the same key modified multiple times", async () => {
    sandbox.startTracking()

    // Update the same item twice
    await Simple.updateRaw(
      { PK: "PK#seeded1", SK: "SK#1" },
      { foo: "first-update" }
    )
    await Simple.updateRaw(
      { PK: "PK#seeded1", SK: "SK#1" },
      { foo: "second-update" }
    )

    expect((await sandbox.get("PK#seeded1", "SK#1")).foo).toBe("second-update")

    await sandbox.rollback()

    // Should restore to original, not to the intermediate state
    expect((await sandbox.get("PK#seeded1", "SK#1")).foo).toBe("seeded1")
  })
})

describe("rollback with beforeEach/afterEach pattern", () => {
  let sandbox: Sandbox

  beforeAll(async () => {
    sandbox = await createSandbox(client)
    await sandbox.seed(
      new Simple({ foo: "a", bar: 1 }),
      new Simple({ foo: "b", bar: 2 })
    )
  })

  beforeEach(() => {
    sandbox.startTracking()
  })

  afterEach(async () => {
    await sandbox.rollback()
  })

  afterAll(async () => {
    await sandbox.destroy()
  })

  test("test A: add an item", async () => {
    await new Simple({ foo: "from-test-a", bar: 100 }).put()
    expect(await sandbox.get("PK#from-test-a", "SK#100")).not.toBeNull()
  })

  test("test B: item from test A does not exist", async () => {
    expect(await sandbox.get("PK#from-test-a", "SK#100")).toBeNull()
  })

  test("test C: modify seeded data", async () => {
    await Simple.updateRaw({ PK: "PK#a", SK: "SK#1" }, { foo: "modified-a" })
    expect((await sandbox.get("PK#a", "SK#1")).foo).toBe("modified-a")
  })

  test("test D: seeded data is back to original", async () => {
    expect((await sandbox.get("PK#a", "SK#1")).foo).toBe("a")
  })

  test("test E: delete seeded data", async () => {
    await Simple.delete({ PK: "PK#b", SK: "SK#2" })
    expect(await sandbox.get("PK#b", "SK#2")).toBeNull()
  })

  test("test F: deleted seeded data is back", async () => {
    expect(await sandbox.get("PK#b", "SK#2")).not.toBeNull()
    expect((await sandbox.get("PK#b", "SK#2")).foo).toBe("b")
  })
})

describe("existing sandbox semantics still work", () => {
  let sandbox: Sandbox

  beforeEach(async () => {
    sandbox = await createSandbox(client)
  })

  afterEach(async () => {
    await sandbox.destroy()
  })

  test("create and destroy per test still works", async () => {
    await sandbox.seed(new Simple({ foo: "per-test", bar: 1 }))

    const item = await sandbox.get("PK#per-test", "SK#1")
    expect(item).not.toBeNull()
    expect(item.foo).toBe("per-test")
  })

  test("snapshot and diff still work", async () => {
    const before = await sandbox.snapshot()
    await new Simple({ foo: "diffed", bar: 1 }).put()
    const diffResult = await sandbox.diff(before)
    expect(diffResult).toContain("PK#diffed")
  })
})
