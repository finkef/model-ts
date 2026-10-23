import { expect, it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { printSchema, GraphQLEnumType, GraphQLObjectType } from "graphql"
import {
  G,
  Labels,
  schema,
  definitions,
  Scope,
  Success,
  Item,
  Identity,
  Alternative,
  Missing,
} from "./fixtures/type-contracts.ts"

const execute = (
  source: string,
  allowed = true,
  variables?: Record<string, unknown>,
) =>
  G.execute(schema, { source, variableValues: variables }, { allowed }).pipe(
    Effect.provideService(Labels, { prefix: "label:" }),
  )

it.effect(
  "mapped enums round-trip literals, variables and nested input lists",
  () =>
    Effect.gen(function* () {
      expect(
        yield* execute(
          `query($scope: Scope!, $input: ScopeInput!) {
      echo(scope: $scope) input(input: $input) status
    }`,
          true,
          {
            scope: "USER_READ",
            input: { scopes: ["WORKSPACE_WRITE", "USER_READ"] },
          },
        ),
      ).toEqual({
        data: {
          echo: "USER_READ",
          input: ["WORKSPACE_WRITE", "USER_READ"],
          status: "ACTIVE",
        },
      })
      expect(
        yield* execute(
          "{ echo(scope: WORKSPACE_WRITE) input(input: {scopes: [USER_READ]}) }",
        ),
      ).toEqual({
        data: { echo: "WORKSPACE_WRITE", input: ["USER_READ"] },
      })
      const type = schema.schema.getType("Scope")
      expect(type).toBeInstanceOf(GraphQLEnumType)
      if (!(type instanceof GraphQLEnumType)) throw new Error("Missing enum")
      expect(type.parseValue("USER_READ")).toBe("user:read")
      expect(type.serialize("workspace:write")).toBe("WORKSPACE_WRITE")
      expect(type.getValue("USER_READ")?.description).toBe("Read the user.")
      expect(type.getValue("WORKSPACE_WRITE")?.deprecationReason).toBe(
        "Use a scoped key.",
      )
      const rejected = yield* execute(
        "query($scope: Scope!) { echo(scope: $scope) }",
        true,
        { scope: "user:read" },
      )
      expect(rejected.errors).toHaveLength(1)
    }),
)

it.effect(
  "interfaces inherit resolvers and authorization; object fields override",
  () =>
    Effect.gen(function* () {
      expect(
        yield* execute(
          "{ identity { __typename id label } item { id label } }",
        ),
      ).toEqual({
        data: {
          identity: {
            __typename: "Alternative",
            id: "alt-1",
            label: "label:alt-1",
          },
          item: { id: "item-1", label: "one" },
        },
      })
      // The override replaces the inherited authorization as well as its resolver.
      expect(yield* execute("{ item { id label } }", false)).toEqual({
        data: { item: { id: "item-1", label: "one" } },
      })
      const denied = yield* execute("{ identity { label } }", false)
      expect(denied.errors).toHaveLength(1)
      expect(denied.errors?.[0]?.path).toEqual(["identity", "label"])
    }),
)

it.effect(
  "nullable multi-success results reuse registered Error implementors",
  () =>
    Effect.gen(function* () {
      const source = `mutation($fail: Boolean!, $alternative: Boolean!) {
      change(fail: $fail, alternative: $alternative) {
        __typename ... on Item { id name } ... on Alternative { id }
        ... on Error { message }
      }
    }`
      expect(
        yield* execute(source, true, { fail: false, alternative: false }),
      ).toEqual({
        data: { change: { __typename: "Item", id: "item-1", name: "one" } },
      })
      expect(
        yield* execute(source, true, { fail: false, alternative: true }),
      ).toEqual({
        data: { change: { __typename: "Alternative", id: "alt-1" } },
      })
      expect(
        yield* execute(source, true, { fail: true, alternative: false }),
      ).toEqual({
        data: { change: { __typename: "Missing", message: "Not found" } },
      })
      const error = schema.schema.getType("Missing")
      expect(error).toBeInstanceOf(GraphQLObjectType)
      if (!(error instanceof GraphQLObjectType))
        throw new Error("Missing error")
      expect(error.getInterfaces().map((type) => type.name)).toEqual(["Error"])
      expect(Object.keys(error.getFields())).toEqual(["message"])
    }),
)

it.effect(
  "nullable results contain authorization and defect failures without bubbling siblings",
  () =>
    Effect.gen(function* () {
      const result = yield* execute(
        "mutation { denied { __typename } broken { __typename } empty { __typename } markAllRead { __typename ... on Item { id } } }",
      )
      expect(result.data).toEqual({
        denied: null,
        broken: null,
        empty: null,
        markAllRead: { __typename: "Item", id: "item-1" },
      })
      expect(result.errors).toHaveLength(2)
      expect(
        result.errors?.find((error) => error.path?.[0] === "broken")?.extensions
          ?.code,
      ).toBe("INTERNAL_SERVER_ERROR")
      expect(JSON.stringify(result)).not.toContain("private")
    }),
)

it("preserves descriptions, deprecations, explicit result names and nullable SDL", () => {
  const sdl = printSchema(schema.schema)
  expect(sdl).toContain('"""An identifiable value."""\ninterface Identity')
  expect(sdl).toContain(
    '"""A successful operation."""\nunion Success = Item | Alternative',
  )
  expect(sdl).toContain(
    '"""The result of changing an item."""\nunion ChangeResult = Item | Alternative | Missing',
  )
  expect(sdl).toContain(
    'name: String! @deprecated(reason: "Use label instead.")',
  )
  expect(sdl).toContain(
    'WORKSPACE_WRITE @deprecated(reason: "Use a scoped key.")',
  )
  expect(sdl).toContain(
    "change(fail: Boolean!, alternative: Boolean!): ChangeResult\n",
  )
  expect(sdl).toContain("union MarkAllReadResult = Item")
  expect(sdl).toContain("union ItemChangeResult = Item | Missing")
  expect(sdl).toContain("union AlternativeChangeResult = Item | Missing")
  expect(sdl).toContain("union QueryChangeResult = Item | Missing")
})

it("checks enum mappings and rejects ambiguous internal aliases", () => {
  const scope = G.enum("Scope", Scope, {
    values: {
      USER_READ: "user:read",
      READ_USER: "user:read",
      WORKSPACE_WRITE: "workspace:write",
    },
  })
  expect(() => G.schema({ ...definitions, scope })).toThrow(
    "enum values must be unique",
  )
})

it("rejects conflicting explicit result names", () => {
  const query = G.queryField("one", "Item", {
    errors: [Missing],
    result: { name: "ChangeResult" },
    resolve: () =>
      Schema.decodeUnknownSync(Item)({
        id: "item-1",
        kind: "item",
        name: "one",
      }),
  })
  expect(() => G.schema({ ...definitions, query })).toThrow(
    "Duplicate GraphQL name: ChangeResult",
  )
})

it("prints boundary types, result unions and mapped enums as expected SDL", async () => {
  const { readFileSync } = await import("node:fs")
  const { lexicographicSortSchema, printType } = await import("graphql")
  const { schema: bookstore, scopeValues } =
    await import("./fixtures/bookstore.ts")
  const sorted = lexicographicSortSchema(bookstore.schema)
  const actual = Object.fromEntries([
    ...[
      "Scope",
      "Error",
      "NotFoundError",
      "BookCreateResult",
      "SearchResult",
      "OrderPlaceResult",
      "WishlistClearResult",
      "AdminRefundResult",
    ].map((name) => {
      const type = sorted.getType(name)
      if (!type) throw new Error(`Missing ${name}`)
      return [name, printType(type)]
    }),
    ...["bookCreate", "orderPlace", "adminRefundIssue", "wishlistClear"].map(
      (name) => [
        `Mutation.${name}`,
        String(sorted.getMutationType()?.getFields()[name]?.type),
      ],
    ),
  ])
  const author = sorted.getType("Author")
  if (!(author instanceof GraphQLObjectType)) throw new Error("Missing Author")
  const field = author.getFields().portrait!
  actual["Author.portrait"] = {
    type: String(field.type),
    description: field.description,
    deprecationReason: field.deprecationReason,
  }
  const expected: unknown = JSON.parse(
    readFileSync(
      new URL("./fixtures/bookstore-sdl.json", import.meta.url),
      "utf8",
    ),
  )
  expect(actual).toEqual(expected)
  const scope = sorted.getType("Scope")
  if (!(scope instanceof GraphQLEnumType)) throw new Error("Missing Scope")
  for (const [name, value] of Object.entries(scopeValues)) {
    expect(scope.parseValue(name)).toBe(value)
    expect(scope.serialize(value)).toBe(name)
  }
})

it.effect(
  "untagged union members resolve by boundary __typename without a custom resolver",
  () =>
    Effect.gen(function* () {
      const success = G.union("Success", Success, {
        members: ["Item", "Alternative"],
      })
      const query = G.queryField("value", "Success", () => ({
        ...Schema.decodeUnknownSync(Item)({
          id: "item-1",
          kind: "item",
          name: "one",
        }),
        __typename: "Item",
      }))
      const built = G.schema({ ...definitions, success, query })
      const result = yield* G.execute(
        built,
        {
          source: "{ value { __typename ... on Item { id name } } }",
        },
        { allowed: true },
      ).pipe(Effect.provideService(Labels, { prefix: "label:" }))
      expect(result).toEqual({
        data: { value: { __typename: "Item", id: "item-1", name: "one" } },
      })
    }),
)

it.effect(
  "inherited typed-result fields share the interface's union identity",
  () =>
    Effect.gen(function* () {
      const identity = G.interface("Identity", Identity, {
        id: G.id(),
        change: G.nullable("Item", {
          errors: [Missing],
          resolve: () => Effect.fail(new Missing({ message: "Not found" })),
        }),
      })
      const item = G.object(
        "Item",
        Item,
        { implements: ["Identity"] },
        { name: G.string() },
      )
      const alternative = G.object(
        "Alternative",
        Alternative,
        { implements: ["Identity"] },
        {},
      )
      const built = G.schema({ ...definitions, identity, item, alternative })
      expect(
        yield* G.execute(
          built,
          {
            source:
              "{ item { change { __typename ... on Error { message } } } }",
          },
          { allowed: true },
        ),
      ).toEqual({
        data: {
          item: { change: { __typename: "Missing", message: "Not found" } },
        },
      })
    }),
)
