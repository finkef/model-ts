import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import { mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { join, resolve } from "node:path"
import { fileURLToPath } from "node:url"

const root = fileURLToPath(new URL("..", import.meta.url))
const directory = mkdtempSync(join(root, ".scale-"))
const count = 200
try {
  const models = Array.from(
    { length: count },
    (_, i) =>
      `class Item${i} extends Model.Class<Item${i}>()("Item${i}", {id: Schema.String}) {}`,
  )
  const registrations = Array.from(
    { length: count },
    (_, i) => `Register<typeof Item${i}>`,
  ).join(", ")
  const definitions = Array.from(
    { length: count },
    (_, i) =>
      `item${i}: G.object(Item${i}, {id: G.id(), next: G.nullable("Item${(i + 1) % count}", () => null)})`,
  )
  writeFileSync(
    join(directory, "scale.ts"),
    `import { Model } from "@model-ts/core"\nimport { Schema } from "effect"\nimport { createBuilder, type Register } from "../src/index.ts"\n${models.join("\n")}\ninterface Types extends ${registrations} {}\nconst G = createBuilder<Types>()\nconst definitions = {${definitions.join(",\n")}, query: G.query({root: G.field("Item0", () => new Item0({id:"root"}))})}\nexport const schema = G.schema(definitions)\n`,
  )
  writeFileSync(
    join(directory, "tsconfig.json"),
    JSON.stringify({
      compilerOptions: {
        strict: true,
        target: "ES2022",
        module: "NodeNext",
        moduleResolution: "NodeNext",
        skipLibCheck: true,
        noEmit: true,
        allowImportingTsExtensions: true,
        types: ["node"],
      },
      files: ["scale.ts"],
    }),
  )
  const start = performance.now()
  const result = spawnSync(
    resolve(root, "../../node_modules/.bin/tsc"),
    ["--project", directory, "--pretty", "false"],
    { cwd: root, encoding: "utf8" },
  )
  assert.ifError(result.error)
  assert.equal(result.status, 0, result.stdout + result.stderr)
  console.log(
    `Native TS7: ${count} real Model.Class object types with cyclic references passed in ${((performance.now() - start) / 1000).toFixed(3)}s.`,
  )
} finally {
  rmSync(directory, { recursive: true, force: true })
}
