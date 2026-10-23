import { expect, it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { createBuilder, type Register } from "../src/GraphQL.ts"
import {
  G,
  definitions,
  Identity,
  Scope,
  Labels,
} from "./fixtures/type-contracts.ts"

it.effect(
  "abstract connections preserve explicit union members and inherited interface fields",
  () =>
    Effect.gen(function* () {
      const value = { id: "alt-1", kind: "alternative" as const }
      const identity = G.interface(
        "Identity",
        Identity,
        {
          description: "An identifiable value.",
          resolveType: () => "Alternative",
        },
        {
          ...definitions.identity.fields,
          related: G.connection("Identity", { nodes: () => [value] }),
        },
      )
      const query = G.query({
        identities: G.connection("Identity", { nodes: () => [value] }),
        search: G.connection("Success", {
          nodes: () => [
            value,
            { id: "item-1", kind: "item" as const, name: "one" },
          ],
        }),
        edge: G.field("SuccessEdge", () => ({ cursor: "opaque", node: value })),
      })
      const built = G.schema({ ...definitions, identity, query })
      const result = yield* G.execute(
        built,
        {
          source: `{
      identities { edges { node { __typename id label related { edges { node { id } } } } } }
      search { edges { node { __typename ... on Item { id name } ... on Alternative { id label } } } }
      edge { cursor node { __typename ... on Alternative { id } } }
    }`,
        },
        { allowed: true },
      ).pipe(Effect.provideService(Labels, { prefix: "label:" }))
      expect(result).toEqual({
        data: {
          identities: {
            edges: [
              {
                node: {
                  __typename: "Alternative",
                  id: "alt-1",
                  label: "label:alt-1",
                  related: { edges: [{ node: { id: "alt-1" } }] },
                },
              },
            ],
          },
          search: {
            edges: [
              {
                node: {
                  __typename: "Alternative",
                  id: "alt-1",
                  label: "label:alt-1",
                },
              },
              { node: { __typename: "Item", id: "item-1", name: "one" } },
            ],
          },
          edge: {
            cursor: "opaque",
            node: { __typename: "Alternative", id: "alt-1" },
          },
        },
      })
    }),
)

it.effect(
  "plain model enum schemas survive optional, nullable, annotated and nested list inputs",
  () =>
    Effect.gen(function* () {
      class ModelInput extends Schema.Class<ModelInput>("ModelInput")({
        scopes: Schema.optionalKey(Schema.NullOr(Schema.Array(Scope))),
      }) {}
      const query = G.query({
        echo: G.list("Scope", {
          args: {
            scope: Schema.optionalKey(
              Scope.annotate({ description: "Optional permission." }),
            ),
            input: ModelInput,
            fallback: G.default(Scope, "user:read"),
          },
          resolve: (_, { scope, input, fallback }) => {
            expect(input).toBeInstanceOf(ModelInput)
            return [
              ...(scope ? [scope] : []),
              ...(input.scopes ?? []),
              fallback,
            ]
          },
        }),
      })
      const built = G.schema({ ...definitions, query })
      for (const [source, variableValues, expected] of [
        [
          "{ echo(scope: WORKSPACE_WRITE, input: {scopes: [USER_READ]}) }",
          undefined,
          ["WORKSPACE_WRITE", "USER_READ", "USER_READ"],
        ],
        [
          "query($input: ModelInput!) { echo(input: $input) }",
          { input: { scopes: null } },
          ["USER_READ"],
        ],
        ["{ echo(input: {}) }", undefined, ["USER_READ"]],
      ] as const) {
        expect(
          yield* G.execute(
            built,
            { source, variableValues },
            { allowed: true },
          ).pipe(Effect.provideService(Labels, { prefix: "label:" })),
        ).toEqual({ data: { echo: expected } })
      }
    }),
)

it("unregistered literal schemas report the input path and registration fix", () => {
  // Equal values are not evidence that two domain schemas represent the same enum.
  const Unregistered = Schema.Literals(["user:read", "workspace:write"])
  const Input = G.input(
    "UnregisteredInput",
    Schema.Struct({ scopes: Schema.Array(Unregistered) }),
  )
  const query = G.query({
    value: G.field("Boolean", { args: { input: Input }, resolve: () => true }),
  })
  expect(() => G.schema({ ...definitions, query })).toThrow(
    "Unregistered literal or union schema at UnregisteredInput.scopes; register this exact model schema with GraphQL.enum(name, schema) and export the definition in the schema barrel",
  )
})

it.effect("singleton enum identity is local to the assembled schema", () =>
  Effect.gen(function* () {
    const Single = Schema.Literals(["only:value"])
    const local = createBuilder<Register<typeof Single, "Single">>()
    const single = local.enum("Single", Single, {
      values: { ONLY: "only:value" },
    })
    const query = local.query({
      value: local.field("Single", {
        args: { value: Schema.optionalKey(Single) },
        resolve: (_, args) => args.value ?? "only:value",
      }),
    })
    expect(
      yield* local.execute(local.schema({ single, query }), {
        source: "{ value(value: ONLY) }",
      }),
    ).toEqual({ data: { value: "ONLY" } })
    const unregistered = createBuilder<{}>()
    const missing = unregistered.query({
      value: unregistered.field("Boolean", {
        args: { value: Single },
        resolve: () => true,
      }),
    })
    expect(() => unregistered.schema({ missing })).toThrow(
      "register this exact model schema",
    )
  }),
)

it("rejects two enum names for one model schema instead of choosing by barrel order", () => {
  const local = createBuilder<
    Register<typeof Scope, "One"> & Register<typeof Scope, "Two">
  >()
  const values = { READ: "user:read", WRITE: "workspace:write" } as const
  const one = local.enum("One", Scope, { values })
  const two = local.enum("Two", Scope.annotate({ description: "Alias" }), {
    values,
  })
  const query = local.query({
    value: local.field("One", () => "user:read"),
  })
  expect(() => local.schema({ one, two, query })).toThrow(
    "Enum schema is registered as both One and Two; use a separate Schema.Literals instance for each enum",
  )
})
