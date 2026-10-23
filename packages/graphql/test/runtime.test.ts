import { readFileSync } from "node:fs"
import { describe, expect, test } from "@effect/vitest"
import { Effect, Layer, Schema, Stream } from "effect"
import {
  buildSchema,
  isObjectType,
  lexicographicSortSchema,
  printSchema,
  printType,
} from "graphql"
import type { Request } from "../src/GraphQL.ts"
import { createBuilder, type Register } from "../src/GraphQL.ts"
import { annotations } from "../src/SchemaTypes.ts"
import {
  identityFor,
  type AppContext,
  type Identity,
} from "./app/api/context.ts"
import { GraphQL } from "./app/api/graphql.ts"
import * as definitions from "./app/api/graphql/index.ts"
import { execute } from "./app/api/handler.ts"
import { schema } from "./app/api/schema.ts"
import {
  UserInputError,
  DynamoDBError,
  ItemNotFoundError,
  ItemDecodeError,
} from "./app/store/errors.ts"
import { ListId, OrganizationId, WorkspaceId } from "./app/store/ids.ts"
import {
  DynamoDBMemory,
  List,
  ListStatus,
  Workspace,
} from "./app/store/models.ts"
import { Guard, Fetch } from "./fixtures/definitions/index.ts"
import { GraphQL as FixtureGraphQL } from "./fixtures/graphql.ts"
import { schema as fixtureSchema } from "./fixtures/schema.ts"

// A fresh table per call; the identity is provided per request.
const ctx = (identity: Identity): AppContext => ({
  identity,
  requestId: "test",
})
const member = ctx(
  identityFor(OrganizationId.make("o1"), ["workspace:read", "workspace:write"]),
)
const handler = (request: Request) =>
  execute(request, member).pipe(
    Effect.provide(DynamoDBMemory),
    Effect.runPromise,
  )

const mutation = `mutation($id:ID!,$input:ListUpdateInput!){listUpdate(id:$id,input:$input){__typename ...on ListEdge{cursor node{name starred status updatedAt workspace{name}}} ...on UserInputError{message field} ...on NotFoundError{message}}}`
const appLayer = DynamoDBMemory
const update = (id: string, input: { name?: string; starred?: boolean }) =>
  handler({ source: mutation, variableValues: { id, input } })

describe("example", () => {
  test("success edge", async () => {
    expect(await update("l1", { name: "Launch v2", starred: true })).toEqual({
      data: {
        listUpdate: {
          __typename: "ListEdge",
          cursor: expect.any(String),
          node: {
            name: "Launch v2",
            starred: true,
            status: "ACTIVE",
            updatedAt: "2026-10-22T08:00:00.000Z",
            workspace: { name: "Product" },
          },
        },
      },
    })
  })
  for (const name of ["", " ", "\n\t"]) {
    test(`Schema decode rejects blank ${JSON.stringify(name)} as a union`, async () => {
      const result = await update("l1", { name })
      expect(result.errors).toBeUndefined()
      expect(result.data?.listUpdate).toMatchObject({
        __typename: "UserInputError",
        field: "name",
      })
    })
  }
  test("optional-only null reports a fix in the declared input error result", async () => {
    const result = await handler({
      source: mutation,
      variableValues: { id: "l1", input: { name: null } },
    })
    expect(result.errors).toBeUndefined()
    expect(result.data?.listUpdate).toMatchObject({
      __typename: "UserInputError",
      field: "name",
      message: expect.stringContaining("Schema.optionalKey(Schema.NullOr(S))"),
    })
  })
  test("unknown ID", async () => {
    expect(await update("absent", {})).toEqual({
      data: {
        listUpdate: { __typename: "NotFoundError", message: "List not found" },
      },
    })
  })
  test("workspace connection", async () => {
    expect(
      await handler({
        source:
          '{workspace(id:"w1"){name lists(first:10){edges{node{id name status}}}}}',
      }),
    ).toEqual({
      data: {
        workspace: {
          name: "Product",
          lists: {
            edges: [{ node: { id: "l1", name: "Launch", status: "ACTIVE" } }],
          },
        },
      },
    })
  })
  test("absent optional fields become null", async () => {
    expect(await update("l1", {})).toMatchObject({
      data: { listUpdate: { node: { starred: null } } },
    })
  })
  test("authorization false blocks resolver", async () => {
    const result = await GraphQL.execute(
      schema,
      { source: mutation, variableValues: { id: "l1", input: {} } },
      ctx(identityFor(null, [])),
    ).pipe(Effect.provide(DynamoDBMemory), Effect.runPromise)
    expect(result.errors?.[0]?.extensions.code).toBe("FORBIDDEN")
  })
  test("scope denial remains NotFoundError", async () => {
    expect(
      await GraphQL.execute(
        schema,
        { source: mutation, variableValues: { id: "l1", input: {} } },
        ctx(identityFor(OrganizationId.make("o1"), [])),
      ).pipe(Effect.provide(DynamoDBMemory), Effect.runPromise),
    ).toEqual({
      data: {
        listUpdate: { __typename: "NotFoundError", message: "List not found" },
      },
    })
  })
  test("v7 SDL preserved, plus new types and query fields", () => {
    const baseline = buildSchema(
      readFileSync(new URL("./fixtures/v7.graphql", import.meta.url), "utf8"),
    )
    // These types gained descriptions or a DateTime representation since v7;
    // only their field types must stay the same.
    const describedTypes = [
      "Query",
      "DateTime",
      "PageInfo",
      "ListEdge",
      "ListConnection",
      "WorkspaceConnection",
      "Workspace",
    ]
    const types = Object.values(baseline.getTypeMap()).filter(
      (type) => !type.name.startsWith("__"),
    )
    const current = lexicographicSortSchema(schema.schema)
    for (const old of types) expect(current.getType(old.name)).toBeDefined()
    for (const old of types.filter(
      (type) => !describedTypes.includes(type.name),
    ))
      expect(printType(current.getType(old.name)!)).toBe(printType(old))
    for (const old of types
      .filter(isObjectType)
      .filter((type) => describedTypes.includes(type.name))) {
      const actual = current.getType(old.name)
      if (!isObjectType(actual)) throw new Error(`Expected object ${old.name}`)
      for (const [name, field] of Object.entries(old.getFields()))
        expect(String(actual.getFields()[name]?.type)).toBe(String(field.type))
    }
    const expected = buildSchema(
      readFileSync(
        new URL("./fixtures/schema.graphql", import.meta.url),
        "utf8",
      ),
    )
    expect(printSchema(lexicographicSortSchema(schema.schema))).toBe(
      printSchema(lexicographicSortSchema(expected)),
    )
  })
})

test("store is free of GraphQL metadata; id wraps a copy", () => {
  for (const s of [
    ListId,
    WorkspaceId,
    OrganizationId,
    ListStatus,
    List.fields.name,
    Workspace.fields.name,
  ]) {
    expect(annotations(s.ast).graphqlType).toBeUndefined()
    expect(annotations(s.ast).description).toBeUndefined()
  }
  const wrapped = GraphQL.id(ListId)
  expect(wrapped).not.toBe(ListId)
  expect(wrapped.ast).not.toBe(ListId.ast)
  expect(annotations(wrapped.ast).graphqlType).toBe("ID")
  expect(Schema.decodeUnknownSync(wrapped)("l1")).toBe("l1")
  expect(annotations(ListId.ast).graphqlType).toBeUndefined()
})

test("declared fields need no model AST inspection", async () => {
  const model = Schema.Struct({ value: Schema.String })
  // A trap on the supplied schema's fields would have broken the old expose path.
  Object.defineProperty(model, "fields", {
    get() {
      throw new Error("Model fields must not be read")
    },
  })
  Object.defineProperty(model, "ast", {
    get() {
      throw new Error("Model AST must not be read")
    },
  })
  const local = createBuilder<Register<typeof model, "Explicit">>()
  const object = local.object("Explicit", model, {}, { value: local.string() })
  // Use a scalar query to include the object without requiring a new registry ref.
  const query = local.query({ hello: local.field("String", () => "ok") })
  const built = local.schema({ object, query })
  expect(
    await local
      .execute(built, { source: "{hello}" })
      .pipe(Effect.provide(appLayer), Effect.runPromise),
  ).toEqual({ data: { hello: "ok" } })
  expect(built.schema.getType("Explicit")).toBeDefined()
})

describe("defaults, named refs and wrappers", () => {
  const layers = Layer.mergeAll(
    Layer.succeed(Guard, { allowed: true }),
    Layer.succeed(Fetch, { value: "fetched" }),
  )
  const execute = (source: string) =>
    FixtureGraphQL.execute(fixtureSchema, { source }).pipe(
      Effect.provide(layers),
      Effect.runPromise,
    )
  test("all scalars, enum, custom scalar, model, edge and PageInfo", async () => {
    expect(
      await execute(
        "{fixture{id text integer float boolean date json custom status workspace{id title} edge{cursor node{name}} pageInfo{hasNextPage} computed guarded}}",
      ),
    ).toEqual({
      data: {
        fixture: {
          id: "l1",
          text: "hello",
          integer: 1,
          float: 1.5,
          boolean: true,
          date: "2026-10-22T00:00:00.000Z",
          json: { x: 1 },
          custom: "token",
          status: "ACTIVE",
          workspace: { id: "w1", title: "Workspace" },
          edge: { cursor: "1", node: { name: "Example" } },
          pageInfo: { hasNextPage: false },
          computed: "hello",
          guarded: "fetched",
        },
      },
    })
  })
  test("null, undefined, Option and nullable elements", async () => {
    expect(
      await execute(
        "{fixture{optional nullish option maybeNodes{id} maybeStatus maybeWorkspace{id} nullableElements plainNullable effectNullable}}",
      ),
    ).toEqual({
      data: {
        fixture: {
          optional: null,
          nullish: null,
          option: null,
          maybeNodes: null,
          maybeStatus: null,
          maybeWorkspace: null,
          nullableElements: ["yes", null],
          plainNullable: null,
          effectNullable: null,
        },
      },
    })
  })
  test("default list and connection plus resolved nodes", async () => {
    expect(
      await execute(
        "{fixture{nodes{id} page{edges{node{id}}} resolvedList{id} resolvedConnection{edges{node{id}} totalCount}}}",
      ),
    ).toEqual({
      data: {
        fixture: {
          nodes: [{ id: "l1" }],
          page: { edges: [{ node: { id: "l1" } }] },
          resolvedList: [{ id: "l1" }],
          resolvedConnection: {
            edges: [{ node: { id: "l1" } }],
            totalCount: 1,
          },
        },
      },
    })
  })
  test("field metadata is at the boundary", () => {
    const sdl = printSchema(fixtureSchema.schema)
    expect(sdl).toContain(
      'text: String! @deprecated(reason: "Prefer computed")',
    )
    expect(sdl).toContain('"""Text"""')
  })
})

class NestedInput extends Schema.Class<NestedInput>("NestedInput")({
  label: Schema.NonEmptyString,
}) {}
class OuterInput extends Schema.Class<OuterInput>("OuterInput")({
  nested: NestedInput,
}) {}
const NestedQuery = GraphQL.query({
  nested: GraphQL.field("String", {
    args: { input: OuterInput },
    resolve: (_, { input }) =>
      input instanceof OuterInput && input.nested instanceof NestedInput
        ? input.nested.label
        : "wrong",
  }),
})
test("nested input names and decoded classes come from identifiers", async () => {
  const built = GraphQL.schema({ ...definitions, NestedQuery })
  expect(printSchema(built.schema)).toContain("input NestedInput {")
  expect(printSchema(built.schema)).toContain("input OuterInput {")
  expect(
    await GraphQL.execute(
      built,
      { source: '{nested(input:{nested:{label:"value"}})}' },
      member,
    ).pipe(Effect.provide(appLayer), Effect.runPromise),
  ).toEqual({ data: { nested: "value" } })
})
test("decode failure without declared UserInputError remains BAD_USER_INPUT", async () => {
  const built = GraphQL.schema({ ...definitions, NestedQuery })
  const result = await GraphQL.execute(
    built,
    { source: '{nested(input:{nested:{label:""}})}' },
    member,
  ).pipe(Effect.provide(appLayer), Effect.runPromise)
  expect(result.errors?.[0]?.extensions.code).toBe("BAD_USER_INPUT")
})
test("nested decode path in UserInputError", async () => {
  const query = GraphQL.query({
    nestedEdge: GraphQL.field("ListEdge", {
      args: { input: OuterInput },
      errors: [UserInputError],
      resolve: () =>
        List.load(List.key(ListId.make("l1"))).pipe(
          Effect.flatMap((node) =>
            Effect.map(node.cursor(), (cursor) => ({ cursor, node })),
          ),
        ),
    }),
  })
  const built = GraphQL.schema({ ...definitions, query })
  const result = await GraphQL.execute(
    built,
    {
      source:
        '{nestedEdge(input:{nested:{label:""}}){__typename ...on UserInputError{field}}}',
    },
    member,
  ).pipe(Effect.provide(appLayer), Effect.runPromise)
  expect(result).toEqual({
    data: {
      nestedEdge: { __typename: "UserInputError", field: "nested.label" },
    },
  })
})

test("errors preserve public/internal/defect policy", async () => {
  const query = GraphQL.query({
    internal: GraphQL.nullable("String", () =>
      Effect.fail(new DynamoDBError({ operation: "secret", cause: "secret" })),
    ),
    defect: GraphQL.nullable("String", () => Effect.die(new Error("secret"))),
    thrown: GraphQL.nullable("String", () => {
      throw new Error("secret")
    }),
  })
  const built = GraphQL.schema({ ...definitions, query })
  const result = await GraphQL.execute(
    built,
    { source: "{internal defect thrown}" },
    member,
  ).pipe(Effect.provide(appLayer), Effect.runPromise)
  expect(result.errors).toHaveLength(3)
  for (const error of result.errors ?? []) {
    expect(error.message).toBe("Internal server error")
    expect(error.extensions.code).toBe("INTERNAL_SERVER_ERROR")
  }
})
test("internal errors cannot be exposed as result members", () => {
  expect(() => {
    const query = GraphQL.query({
      bad: GraphQL.field("List", {
        errors: [DynamoDBError],
        resolve: () =>
          Effect.fail(new DynamoDBError({ operation: "x", cause: "x" })),
      }),
    })
    GraphQL.schema({ ...definitions, query })
  }).toThrow("Internal errors cannot be exposed")
})

test("union and interface resolve by model tag", async () => {
  expect(
    await handler({
      source:
        "{favorites{__typename ...on List{name} ...on Workspace{name}} node{__typename id}}",
    }),
  ).toEqual({
    data: {
      favorites: [
        { __typename: "List", name: "Launch" },
        { __typename: "Workspace", name: "Product" },
      ],
      node: { __typename: "List", id: "l1" },
    },
  })
})
test("custom scalar, DateTime, JSON and deprecation", async () => {
  expect(
    await handler({
      source:
        '{slug(value:"product-team") serverTime metadata(value:{enabled:true}) legacyName}',
    }),
  ).toEqual({
    data: {
      slug: "product-team",
      serverTime: "2026-10-22T08:00:00.000Z",
      metadata: { enabled: true },
      legacyName: "Legacy workspace",
    },
  })
  expect(
    (await handler({ source: '{slug(value:"INVALID SLUG")}' })).errors,
  ).toHaveLength(1)
  expect(
    schema.schema.getQueryType()?.getFields().legacyName?.deprecationReason,
  ).toBe("Use workspace.name")
})
test("subscription decodes ID and emits a List", async () => {
  const result = await GraphQL.subscribe(
    schema,
    { source: 'subscription{listChanged(id:"l1"){id name}}' },
    member,
  ).pipe(Stream.runCollect, Effect.provide(appLayer), Effect.runPromise)
  expect(Array.from(result)).toEqual([
    { data: { listChanged: { id: "l1", name: "Launch" } } },
  ])
})
test("connection slicing, totalCount and maximum", async () => {
  expect(
    await handler({
      source:
        "{listsCounted(first:0){edges{cursor} totalCount pageInfo{hasNextPage}}}",
    }),
  ).toEqual({
    data: {
      listsCounted: {
        edges: [],
        totalCount: 1,
        pageInfo: { hasNextPage: true },
      },
    },
  })
  expect(
    await handler({
      source: "{listsCounted(last:1){edges{node{id}} totalCount}}",
    }),
  ).toEqual({
    data: { listsCounted: { edges: [{ node: { id: "l1" } }], totalCount: 1 } },
  })
  for (const args of [
    "first:101",
    "last:101",
    "first:-1",
    "first:1,last:1",
    'after:"bad"',
    'first:1,before:"x"',
    'last:1,after:"x"',
    'after:"x",before:"y"',
  ]) {
    const result = await handler({
      source: `{listsCounted(${args}){totalCount}}`,
    })
    expect(result.errors?.[0]?.extensions.code).toBe("BAD_USER_INPUT")
  }
})
test("default page size is 20, independently of the maximum of 100", async () => {
  const local = createBuilder<Register<typeof List>>().withErrorPolicy({
    public: [ItemNotFoundError],
    internal: [DynamoDBError, ItemDecodeError],
  })
  const object = local.object(List, { id: local.id() })
  const query = local.query({
    page: local.connection("List", {
      nodes: () =>
        List.load(List.key(ListId.make("l1"))).pipe(
          Effect.map((list) => Array.from({ length: 120 }, () => list)),
        ),
      totalCount: () => 120,
    }),
  })
  const built = local.schema({ object, query })
  const result = await local
    .execute(built, {
      source: "{page{edges{node{id}} totalCount pageInfo{hasNextPage}}}",
    })
    .pipe(Effect.provide(DynamoDBMemory), Effect.runPromise)
  expect(result.data?.page).toMatchObject({
    totalCount: 120,
    pageInfo: { hasNextPage: true },
  })
  expect(Reflect.get(Object(result.data?.page), "edges")).toHaveLength(20)
})
test("tag, not overridden class identifier, supplies object name", () => {
  class Tagged extends Schema.TaggedClass<Tagged>("DifferentIdentifier")(
    "ActualTag",
    { id: Schema.String },
  ) {}
  const local = createBuilder<Register<typeof Tagged>>()
  const object = local.object(Tagged, { id: local.id() })
  expect(object["~name"]).toBe("ActualTag")
})
