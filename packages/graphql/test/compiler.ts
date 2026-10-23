import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import {
  cpSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs"
import { join, resolve } from "node:path"
import { fileURLToPath } from "node:url"
import { negativeCases, registryCases, definitionCases } from "./cases.ts"

const root = fileURLToPath(new URL("..", import.meta.url))
const cli = resolve(root, "../../node_modules/.bin/tsc")
const temporary = mkdtempSync(join(root, ".compiler-"))
const update = process.argv.includes("--update")
const baselineFile = join(root, "test/compiler-diagnostics.json")
interface Diagnostic {
  file: string
  line: number
  column: number
  code: number
  message: string
}
interface Expected {
  name: string
  diagnostics: readonly Omit<Diagnostic, "message">[]
}
const expected: readonly Expected[] = update
  ? []
  : JSON.parse(readFileSync(baselineFile, "utf8"))
const recorded: Expected[] = []

// Match source tokens instead of whitespace so formatting cannot invalidate edits.
// These are source overlays, not a parser or a dependency on the old compiler API.
function tokens(source: string) {
  return [
    ...source.matchAll(
      /"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|`(?:\\.|[^`\\])*`|[A-Za-z_$][\w$]*|\d+|[^\s;,]/g,
    ),
  ].map((match) => ({
    text: match[0].startsWith("'") ? `"${match[0].slice(1, -1)}"` : match[0],
    start: match.index,
    end: match.index + match[0].length,
  }))
}
function locate(source: string, snippet: string) {
  const haystack = tokens(source),
    needle = tokens(snippet)
  const matches = haystack.flatMap((token, i) =>
    needle.every((part, j) => part.text === haystack[i + j]?.text)
      ? [{ start: token.start, end: haystack[i + needle.length - 1]!.end }]
      : [],
  )
  assert.equal(
    matches.length,
    1,
    `Expected one source anchor: ${snippet}; found ${matches.length}`,
  )
  const found = matches[0]!
  if (snippet.trimEnd().endsWith(",")) {
    const trailing = /^\s*,/.exec(source.slice(found.end))
    if (trailing) found.end += trailing[0].length
  }
  return found
}
function overlay(source: string, before: string, after: string) {
  const span = locate(source, before)
  return source.slice(0, span.start) + after + source.slice(span.end)
}
const pathFor = (file: string) =>
  file
    .replace(/^src\/(api|store)\//, "test/app/$1/")
    .replace(/^tests\//, "test/")
function adapt(code: string) {
  // The fixture uses real model-ts keys, query parameters and Effect cursors.
  let result = code
    .replace(/\b([A-Za-z_$][\w$]*)\s*=>/g, "($1) =>")
    .replace(
      /(List|Workspace)\.load\(\{\s*id:\s*([^}]+)\}\)/g,
      "$1.load($1.key($2))",
    )
    .replace(/(List|Workspace)\.load\(\{\s*id\s*\}\)/g, "$1.load($1.key(id))")
    .replace(
      "List.paginate(args, { workspaceId: workspace.id })",
      'List.paginate(args, { IndexName: "GSI1", KeyConditionExpression: "GSI1PK = :pk", ExpressionAttributeValues: { ":pk": `WORKSPACE#${workspace.id}` } })',
    )
    .replace(
      "list.update(input)",
      'list.update({ ...input, updatedAt: new Date("2026-10-22T08:00:00Z") })',
    )
    .replace("cursor: updated.cursor()", "cursor: yield* updated.cursor()")
  result = result.replace(
    /(["'])(\.{1,2}\/[^"']+)(["'])/g,
    (_, quote: string, path: string) => {
      if (path === "../builder") path = "../../../src/GraphQL"
      return `${quote}${path.endsWith(".ts") ? path : path + ".ts"}${quote}`
    },
  )
  return result
}
function compile(): Diagnostic[] {
  const process = spawnSync(
    cli,
    ["--project", temporary, "--pretty", "false"],
    { cwd: temporary, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 },
  )
  assert.ifError(process.error)
  assert.ok(
    process.status === 0 || process.status === 1 || process.status === 2,
    `Compiler crashed: ${process.status}\n${process.stderr}\n${process.stdout}`,
  )
  const diagnostics: Diagnostic[] = []
  for (const match of process.stdout.matchAll(
    /^(.+?)\((\d+),(\d+)\): error TS(\d+): ([\s\S]*?)(?=^.+?\(\d+,\d+\): error TS\d+:|$(?![\s\S]))/gm,
  )) {
    diagnostics.push({
      file: match[1]!.replaceAll("\\", "/"),
      line: Number(match[2]),
      column: Number(match[3]),
      code: Number(match[4]),
      message: match[5]!.trim().replace(/\s+/g, " "),
    })
  }
  assert.ok(
    process.status === 0 || diagnostics.length,
    `Unparsed diagnostics:\n${process.stdout}`,
  )
  return diagnostics
}

try {
  cpSync(join(root, "src"), join(temporary, "src"), { recursive: true })
  cpSync(join(root, "test/app"), join(temporary, "test/app"), {
    recursive: true,
  })
  cpSync(join(root, "test/fixtures"), join(temporary, "test/fixtures"), {
    recursive: true,
  })
  writeFileSync(join(temporary, "package.json"), '{"type":"module"}\n')
  writeFileSync(
    join(temporary, "tsconfig.json"),
    JSON.stringify(
      {
        compilerOptions: {
          strict: true,
          target: "ES2022",
          module: "NodeNext",
          moduleResolution: "NodeNext",
          noEmit: true,
          skipLibCheck: true,
          types: ["node"],
          allowImportingTsExtensions: true,
        },
        include: ["src", "test"],
      },
      null,
      2,
    ),
  )
  assert.deepEqual(compile(), [], "Unmodified compiler fixture must pass")
  const cases = [...negativeCases, ...registryCases, ...definitionCases]
  const failures: string[] = []
  for (const test of cases) {
    const file = pathFor(test.file)
    const changes = [
      { file: test.file, before: test.before, after: test.after },
      ...(test.additional ?? []),
    ]
    const originals = new Map<string, string>()
    try {
      for (const change of changes) {
        const target = pathFor(change.file),
          absolute = join(temporary, target)
        const original = readFileSync(absolute, "utf8")
        originals.set(absolute, original)
        let modified = overlay(
          original,
          adapt(change.before),
          adapt(change.after),
        )
        if (target === file) modified = adapt(test.prefix ?? "") + modified
        writeFileSync(absolute, modified)
      }
      const diagnostics = compile()
      assert.ok(
        !diagnostics.some((d) => [2589, 7022, 7023, 7056].includes(d.code)),
        `${test.name}: inference collapsed\n${JSON.stringify(diagnostics)}`,
      )
      const expectedFile = test.root
        ? test.name === "missing layer" ||
          test.name === "missing request context"
          ? file
          : "test/app/api/schema.ts"
        : pathFor(test.expectedFile ?? test.file)
      const matching = diagnostics.filter(
        (d) => d.file === expectedFile && test.message.test(d.message),
      )
      assert.ok(
        matching.length,
        `${test.name}: no local diagnostic matching ${test.message} in ${expectedFile}\n${JSON.stringify(diagnostics, null, 2)}`,
      )
      if (test.lineAnchor) {
        const source = readFileSync(join(temporary, expectedFile), "utf8")
        const span = locate(source, adapt(test.lineAnchor))
        const first = source.slice(0, span.start).split("\n").length
        // A formatted call may put its model argument after the reference line.
        let end = span.end
        let depth =
          (source.slice(span.start, end).match(/\(/g) ?? []).length -
          (source.slice(span.start, end).match(/\)/g) ?? []).length
        while (depth > 0 && end < source.length) {
          if (source[end] === "(") depth++
          if (source[end] === ")") depth--
          end++
        }
        const last = source.slice(0, end).split("\n").length
        assert.ok(
          matching.some((d) => d.line >= first && d.line <= last),
          `${test.name}: diagnostic is not on its definition/reference line`,
        )
      }
      const locations = matching.map(
        ({ message: _message, ...location }) => location,
      )
      if (!update)
        assert.deepEqual(
          locations,
          expected.find((e) => e.name === test.name)?.diagnostics,
          `${test.name}: diagnostic location/code changed`,
        )
      recorded.push({ name: test.name, diagnostics: locations })
      console.log(`PASS ${test.name}`)
    } catch (error) {
      failures.push(
        `${test.name}: ${error instanceof Error ? error.message : String(error)}`,
      )
    } finally {
      for (const [file, source] of originals) writeFileSync(file, source)
    }
  }
  assert.deepEqual(failures, [], failures.join("\n\n"))
  if (update)
    writeFileSync(baselineFile, JSON.stringify(recorded, null, 2) + "\n")
  else
    assert.equal(
      expected.length,
      recorded.length,
      "Every diagnostic baseline must have a case",
    )
  console.log(
    `Native TS7: ${cases.length} negative/registry cases passed (file:line:column, code, message).`,
  )
} finally {
  // Remove only the unique directory created by this invocation.
  rmSync(temporary, { recursive: true, force: true })
}
