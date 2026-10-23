import { expect, it } from "@effect/vitest"
import { Model } from "@model-ts/core"
import { Effect, Schema, Stream } from "effect"
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
  library: G.object(Library, { id: G.id() }),
  connection: G.object(LibraryConnection, { id: G.id() }),
}

it.effect("real LibraryConnection and renamed derived connection coexist", () =>
  Effect.gen(function* () {
    const query = {
      savedField: G.queryField(
        "saved",
        G.field(
          "LibraryConnection",
          () => new LibraryConnection({ id: "saved" }),
        ),
      ),
      directoryField: G.queryField(
        "directory",
        G.connection("Library", {
          name: "LibraryDirectoryConnection",
          nodes: () => [new Library({ id: "directory" })],
        }),
      ),
    }
    const schema = G.schema({ ...objects, ...query })
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
  const query = G.queryField(
    "hello",
    G.field("String", () => "hello"),
  )
  expect(() =>
    G.schema({ ...objects, duplicate: objects.library, query }),
  ).toThrow("Duplicate GraphQL object: Library")
})

it("claims derived connection names only when used", () => {
  const query = G.queryField(
    "directory",
    G.connection("Library", { nodes: () => [] }),
  )
  expect(() => G.schema({ ...objects, query })).toThrow(
    "Duplicate GraphQL name: LibraryConnection",
  )
})

it("rejects a real edge name when a connection needs that edge", () => {
  const local = createBuilder<
    Register<typeof Library> & Register<typeof LibraryEdge>
  >()
  const library = local.object(Library, { id: local.id() })
  const edge = local.object(LibraryEdge, { id: local.id() })
  const query = local.queryField(
    "directory",
    local.connection("Library", { nodes: () => [] }),
  )
  expect(() => local.schema({ library, edge, query })).toThrow(
    "Duplicate GraphQL name: LibraryEdge",
  )
})

for (const root of ["Query", "Mutation", "Subscription"] as const) {
  it(`rejects duplicate ${root} field names at assembly`, () => {
    const GraphQL = createBuilder<{}>()
    const field = GraphQL.field("String", () => "value")
    const event = GraphQL.field("String", {
      subscribe: () => Stream.succeed("value"),
    })
    const define = () =>
      root === "Query"
        ? GraphQL.queryField("duplicate", field)
        : root === "Mutation"
          ? GraphQL.mutationField("duplicate", field)
          : GraphQL.subscriptionField("duplicate", event)
    expect(() =>
      GraphQL.schema({
        ping: GraphQL.queryField("ping", field),
        first: define(),
        second: define(),
      }),
    ).toThrow(`Duplicate field: ${root}.duplicate`)
  })
}

it.effect(
  "merges individual root fields and permits the same name on different roots",
  () =>
    Effect.gen(function* () {
      const GraphQL = createBuilder<{}>()
      const field = GraphQL.field("String", () => "value")
      const schema = GraphQL.schema({
        first: GraphQL.queryField("first", field),
        second: GraphQL.queryField("second", field),
        mutation: GraphQL.mutationField("first", field),
        subscription: GraphQL.subscriptionField(
          "first",
          GraphQL.field("String", {
            subscribe: () => Stream.succeed("event"),
          }),
        ),
      })
      expect(
        yield* GraphQL.execute(schema, { source: "{ first second }" }),
      ).toEqual({
        data: { first: "value", second: "value" },
      })
      expect(
        yield* GraphQL.execute(schema, { source: "mutation { first }" }),
      ).toEqual({
        data: { first: "value" },
      })
      expect(
        yield* Stream.runCollect(
          GraphQL.subscribe(schema, { source: "subscription { first }" }),
        ),
      ).toEqual([{ data: { first: "event" } }])
      for (const name of ["query", "mutation", "subscription", "events"]) {
        expect(GraphQL).not.toHaveProperty(name)
      }
    }),
)
