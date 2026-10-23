import { readFileSync } from "node:fs"
import { expect, test, it } from "@effect/vitest"
import { Sandbox } from "@model-ts/dynamodb/Sandbox"
import { Context, Effect, Schema } from "effect"
import { printType, buildSchema, lexicographicSortSchema } from "graphql"
import { encodeCursor, slice } from "../src/Connections.ts"
import { createBuilder, type Register } from "../src/GraphQL.ts"
import {
  DynamoDBError,
  ItemDecodeError,
  PaginationError,
} from "./app/store/errors.ts"
import { ListId, OrganizationId, WorkspaceId } from "./app/store/ids.ts"
import { List } from "./app/store/models.ts"

class Library extends Schema.TaggedClass<Library>()("Library", {
  id: Schema.String,
}) {}
class LibraryConnection extends Schema.TaggedClass<LibraryConnection>()(
  "LibraryConnection",
  { token: Schema.String },
) {}
const Node = Schema.Struct({ id: Schema.String })
const SearchResult = Schema.Union([Library, LibraryConnection])
interface Types
  extends
    Register<typeof Library>,
    Register<typeof LibraryConnection>,
    Register<typeof Node, "Node", "interface">,
    Register<typeof SearchResult, "SearchResult"> {}
const G = createBuilder<Types>({ defaultPageSize: 2, maxPageSize: 5 })
const definitions = {
  library: G.object(Library, { implements: ["Node"] }, { id: G.id() }),
  connection: G.object(LibraryConnection, { token: G.string() }),
  node: G.interface("Node", Node, { id: G.id() }),
  result: G.union("SearchResult", SearchResult),
}
const nodes = Array.from(
  { length: 5 },
  (_, i) => new Library({ id: String(i) }),
)

test("abstract nodes, typed edges and renamed directory coexist with a real LibraryConnection", async () => {
  const query = {
    directoryField: G.queryField("directory", G.connection("Library"), {
      name: "LibraryDirectoryConnection",
      edgeName: "LibraryDirectoryEdge",
      nodes: () => nodes,
    }),
    connectedField: G.queryField(
      "connected",
      "LibraryConnection",
      () => new LibraryConnection({ token: "secret" }),
    ),
    holdingsField: G.queryField("holdings", G.connection("Node"), {
      nodes: () => nodes,
    }),
    searchField: G.queryField("search", G.connection("SearchResult"), {
      args: { terms: Schema.Array(Schema.String) },
      nodes: (_, { terms }) =>
        terms.length
          ? [nodes[0]!, new LibraryConnection({ token: "hit" })]
          : [],
    }),
    edgeField: G.queryField("edge", "SearchResultEdge", () => ({
      cursor: "opaque",
      node: nodes[0]!,
    })),
  }
  const built = G.schema({ ...definitions, ...query })
  expect(
    await Effect.runPromise(
      G.execute(built, {
        source:
          '{directory{edges{node{id}}} connected{token} holdings{edges{node{id __typename}}} search(terms:["hit"]){edges{node{__typename ...on LibraryConnection{token}}}} edge{cursor node{__typename}}}',
      }),
    ),
  ).toEqual({
    data: {
      directory: { edges: [{ node: { id: "0" } }, { node: { id: "1" } }] },
      connected: { token: "secret" },
      holdings: {
        edges: [
          { node: { id: "0", __typename: "Library" } },
          { node: { id: "1", __typename: "Library" } },
        ],
      },
      search: {
        edges: [
          { node: { __typename: "Library" } },
          { node: { __typename: "LibraryConnection", token: "hit" } },
        ],
      },
      edge: { cursor: "opaque", node: { __typename: "Library" } },
    },
  })
  expect(
    printType(built.schema.getType("LibraryDirectoryConnection")!),
  ).toContain("edges: [LibraryDirectoryEdge!]!")
  expect(printType(built.schema.getType("LibraryDirectoryEdge")!)).toContain(
    "node: Library!",
  )
  expect(printType(built.schema.getType("NodeEdge")!)).toContain("node: Node!")
  expect(printType(built.schema.getType("SearchResultEdge")!)).toContain(
    "node: SearchResult!",
  )
  expect(built.schema.getType("LibraryEdge")).toBeUndefined()
})

test("before-only defaults backward; zero, null and explicit sizes retain their meaning", async () => {
  const query = G.queryField("page", G.connection("Library"), {
    name: "Catalog",
    nodes: () => nodes,
    totalCount: () => Effect.succeed(nodes.length),
  })
  const built = G.schema({ ...definitions, query })
  for (const [args, ids] of [
    [`before:"${encodeCursor(4)}"`, ["2", "3"]],
    ["first:null,last:null", ["0", "1"]],
    ["last:1", ["4"]],
    ["first:0", []],
    ["first:5", ["0", "1", "2", "3", "4"]],
  ] as const) {
    expect(
      await Effect.runPromise(
        G.execute(built, {
          source: `{page(${args}){edges{node{id}} totalCount}}`,
        }),
      ),
    ).toEqual({
      data: {
        page: { edges: ids.map((id) => ({ node: { id } })), totalCount: 5 },
      },
    })
  }
  expect(
    (
      await Effect.runPromise(
        G.execute(built, { source: "{page(first:6){totalCount}}" }),
      )
    ).errors?.[0]?.extensions.code,
  ).toBe("BAD_USER_INPUT")
})

test("totalCount runs only when selected and retains decoded args, context and services", async () => {
  class Count extends Context.Service<Count, { readonly value: number }>()(
    "Count",
  ) {}
  const local = createBuilder<
    Register<typeof Library>,
    { readonly tenant: string }
  >()
  let calls = 0
  const query = local.queryField("page", local.connection("Library"), {
    args: { filter: Schema.NumberFromString },
    nodes: () => nodes,
    totalCount: (_, { filter }, { tenant }) => {
      calls++
      return Count.useSync(({ value }) => value + filter + tenant.length)
    },
  })
  const built = local.schema({
    library: local.object(Library, { id: local.id() }),
    query,
  })
  const run = (source: string) =>
    Effect.runPromise(
      local
        .execute(built, { source }, { tenant: "acme" })
        .pipe(Effect.provideService(Count, { value: 10 })),
    )
  expect(await run('{page(filter:"2",first:0){edges{cursor}}}')).toEqual({
    data: { page: { edges: [] } },
  })
  expect(calls).toBe(0)
  expect(
    await run(
      '{page(filter:"2",first:0){edges{cursor} totalCount @skip(if:true)}}',
    ),
  ).toEqual({ data: { page: { edges: [] } } })
  expect(calls).toBe(0)
  expect(
    await run(
      'query {page(filter:"2"){...Count}} fragment Count on LibraryCountedConnection {count:totalCount}',
    ),
  ).toEqual({ data: { page: { count: 16 } } })
  expect(calls).toBe(1)
})

test("lazy totalCount failures use the field error policy", async () => {
  const query = G.queryField("page", G.connection("Library"), {
    name: "Catalog",
    resolve: () => Effect.succeed(slice(nodes, { first: 1 })),
    totalCount: () => Effect.die(new Error("private count failure")),
  })
  const built = G.schema({ ...definitions, query })
  expect(
    await Effect.runPromise(
      G.execute(built, { source: "{page{edges{node{id}}}}" }),
    ),
  ).toEqual({
    data: { page: { edges: [{ node: { id: "0" } }] } },
  })
  const result = await Effect.runPromise(
    G.execute(built, { source: "{page{totalCount}}" }),
  )
  expect(result.errors?.[0]).toMatchObject({
    message: "Internal server error",
    path: ["page", "totalCount"],
    extensions: { code: "INTERNAL_SERVER_ERROR" },
  })
})

test("paged resolvers receive opaque storage cursors and return their page unchanged", async () => {
  const opaque = "encrypted-storage-cursor"
  const page = {
    edges: [{ cursor: opaque, node: nodes[0]! }],
    pageInfo: {
      hasNextPage: false,
      hasPreviousPage: true,
      startCursor: opaque,
      endCursor: opaque,
    },
  }
  const query = G.queryField("page", G.connection("Library"), {
    name: "Catalog",
    args: { filter: Schema.String },
    resolve: (_, args) => {
      expect(args).toEqual({ filter: "active", before: opaque })
      return page
    },
  })
  expect(
    await Effect.runPromise(
      G.execute(G.schema({ ...definitions, query }), {
        source: `{page(before:"${opaque}",filter:"active"){edges{cursor} pageInfo{startCursor endCursor hasPreviousPage}}}`,
      }),
    ),
  ).toEqual({
    data: {
      page: {
        edges: [{ cursor: opaque }],
        pageInfo: {
          startCursor: opaque,
          endCursor: opaque,
          hasPreviousPage: true,
        },
      },
    },
  })
})

test("only names actually derived are claimed, and incompatible reused shapes fail assembly", () => {
  const build = (
    fields: Readonly<Record<string, ReturnType<typeof G.queryField>>>,
  ) => G.schema({ ...definitions, ...fields })
  expect(() =>
    build({
      bad: G.queryField("bad", G.connection("Library"), { nodes: () => nodes }),
    }),
  ).toThrow("Duplicate GraphQL name: LibraryConnection")
  expect(() =>
    build({
      a: G.queryField("a", G.connection("Node"), {
        name: "Shared",
        nodes: () => nodes,
      }),
      b: G.queryField("b", G.connection("SearchResult"), {
        name: "Shared",
        nodes: () => nodes,
      }),
    }),
  ).toThrow("Duplicate GraphQL name: Shared")
  expect(() =>
    build({
      a: G.queryField("a", G.connection("Node"), {
        edgeName: "SharedEdge",
        nodes: () => nodes,
      }),
      b: G.queryField("b", G.connection("SearchResult"), {
        edgeName: "SharedEdge",
        nodes: () => nodes,
      }),
    }),
  ).toThrow("Incompatible edge SharedEdge")
  expect(() =>
    build({
      a: G.queryField("a", G.connection("Node"), { nodes: () => nodes }),
      b: G.queryField("b", G.connection("Node"), {
        name: "NodeConnection",
        totalCount: () => 5,
        nodes: () => nodes,
      }),
    }),
  ).toThrow("Duplicate GraphQL name: NodeConnection")
})

test("out-of-range nodes cursors produce empty pages with no invented cursors", () => {
  expect(slice(nodes, { after: encodeCursor(999) })).toEqual({
    edges: [],
    pageInfo: {
      startCursor: null,
      endCursor: null,
      hasNextPage: false,
      hasPreviousPage: true,
    },
  })
})

// The expected SDL is written by hand, independent of the builder's output.
test("generated connections print Relay descriptions, explicit names and nullability", () => {
  const expected = lexicographicSortSchema(
    buildSchema(
      readFileSync(
        new URL("./fixtures/connection-schema.graphql", import.meta.url),
        "utf8",
      ),
    ),
  )
  const query = {
    searchField: G.queryField("search", G.connection("SearchResult"), {
      nodes: () => nodes,
    }),
    directoryField: G.queryField("directory", G.connection("Library"), {
      name: "LibraryDirectoryConnection",
      nodes: () => nodes,
    }),
  }
  const actual = lexicographicSortSchema(
    G.schema({ ...definitions, ...query }).schema,
  )
  for (const name of [
    "LibraryDirectoryConnection",
    "LibraryEdge",
    "PageInfo",
    "SearchResultConnection",
    "SearchResultEdge",
  ])
    expect(printType(actual.getType(name)!)).toBe(
      printType(expected.getType(name)!),
    )
})

test("PageInfo cannot be reused as an edge name", () => {
  const query = G.queryField("page", G.connection("Node"), {
    edgeName: "PageInfo",
    nodes: () => nodes,
  })
  expect(() => G.schema({ ...definitions, query })).toThrow(
    "Incompatible derived type PageInfo",
  )
})

it.effect(
  "DynamoDB pages resume through GraphQL without changing cursors or storage defaults",
  () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      const stored = [1, 2, 3, 4].map(
        (n) =>
          new List({
            id: ListId.make(`l${n}`),
            organizationId: OrganizationId.make("o1"),
            workspaceId: WorkspaceId.make("w1"),
            createdBy: "u1",
            name: `List ${n}`,
            status: "ACTIVE",
            createdAt: new Date("2026-10-22T00:00:00Z"),
          }),
      )
      yield* sandbox.seed(...stored)
      const local = createBuilder<
        Register<typeof List> & Register<typeof Node, "Node", "interface">
      >().withErrorPolicy({
        internal: [DynamoDBError, ItemDecodeError, PaginationError],
      })
      const list = local.object(
        List,
        { implements: ["Node"] },
        { id: local.id() },
      )
      const node = local.interface("Node", Node, { id: local.id() })
      const query = local.queryField("page", local.connection("Node"), {
        authorize: () => Effect.succeed(true),
        resolve: (_, args) =>
          List.paginate(args, {
            IndexName: "GSI1",
            KeyConditionExpression: "GSI1PK = :pk",
            ExpressionAttributeValues: { ":pk": "WORKSPACE#w1" },
          }),
      })
      const built = local.schema({ list, node, query })
      // Validation errors survive Effect authorization before DynamoDB.paginate runs.
      for (const args of ["first:101", "first:-1", "first:1,last:1"]) {
        const invalid = yield* local.execute(built, {
          source: `{page(${args}){edges{node{id}}}}`,
        })
        expect(invalid.errors?.[0]?.extensions.code).toBe("BAD_USER_INPUT")
      }
      // Item cursors are the same opaque encrypted values returned by paginate().
      const before = yield* stored[3]!.cursor()
      const after = yield* stored[0]!.cursor()
      for (const [args, ids] of [
        [{ before }, ["l3"]],
        [{ after }, ["l2"]],
        [{}, ["l1"]],
      ] as const) {
        const response = yield* local.execute(built, {
          source:
            "query($before:String,$after:String){page(before:$before,after:$after){edges{node{id}}}}",
          variableValues: args,
        })
        expect(response).toEqual({
          data: { page: { edges: ids.map((id) => ({ node: { id } })) } },
        })
      }
    }).pipe(
      Effect.provide(
        Sandbox.layerMemory({ pagination: { default: 1, limit: 5 } }),
      ),
    ),
)
