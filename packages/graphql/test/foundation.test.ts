import { expect, it } from "@effect/vitest"
import { Model } from "@model-ts/core"
import { Effect, Schema } from "effect"
import { createBuilder, type Register } from "../src/index.ts"

class Library extends Model.Class<Library>()("Library", {
  id: Schema.String,
}) {}
class LibraryConnection extends Model.Class<LibraryConnection>()(
  "LibraryConnection",
  {
    id: Schema.String,
  },
) {}
class LibraryEdge extends Model.Class<LibraryEdge>()("LibraryEdge", {
  id: Schema.String,
}) {}

type Types = Register<typeof Library> & Register<typeof LibraryConnection>
const G = createBuilder<Types>()
const objects = {
  library: G.object(Library, { id: G.fields.id() }),
  connection: G.object(LibraryConnection, { id: G.fields.id() }),
}

it.effect("real LibraryConnection and renamed derived connection coexist", () =>
  Effect.gen(function* () {
    const query = G.query({
      saved: G.fields.field(
        "LibraryConnection",
        () => new LibraryConnection({ id: "saved" }),
      ),
      directory: G.fields.connection("Library", {
        name: "LibraryDirectoryConnection",
        nodes: () => [new Library({ id: "directory" })],
      }),
    })
    const schema = G.schema({ ...objects, query })
    expect(
      yield* G.execute(schema, {
        source: "{saved{id} directory{edges{node{id}}}}",
      }),
    ).toEqual({
      data: {
        saved: { id: "saved" },
        directory: { edges: [{ node: { id: "directory" } }] },
      },
    })
  }),
)

it("rejects duplicate real names at assembly", () => {
  const query = G.query({ hello: G.fields.field("String", () => "hello") })
  expect(() =>
    G.schema({ ...objects, duplicate: objects.library, query }),
  ).toThrow("Duplicate GraphQL object: Library")
})

it("claims derived connection names only when used", () => {
  const query = G.query({
    directory: G.fields.connection("Library", { nodes: () => [] }),
  })
  expect(() => G.schema({ ...objects, query })).toThrow(
    "Duplicate GraphQL name: LibraryConnection",
  )
})

it("rejects a real edge name when a connection needs that edge", () => {
  const local = createBuilder<
    Register<typeof Library> & Register<typeof LibraryEdge>
  >()
  const library = local.object(Library, { id: local.fields.id() })
  const edge = local.object(LibraryEdge, { id: local.fields.id() })
  const query = local.query({
    directory: local.fields.connection("Library", { nodes: () => [] }),
  })
  expect(() => local.schema({ library, edge, query })).toThrow(
    "Duplicate GraphQL name: LibraryEdge",
  )
})
