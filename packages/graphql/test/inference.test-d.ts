import { Sandbox } from "@model-ts/dynamodb/Sandbox"
import type { Stream } from "effect"
import { Effect, Schema } from "effect"
import type {
  DocumentNode,
  GraphQLSchema,
  GraphQLFormattedError,
} from "graphql"
import { describe, expectTypeOf, it } from "vitest"
import type { ExecutionResult, Edge, Connection } from "../src/GraphQL.ts"
import {
  createBuilder,
  executor,
  execute,
  subscribe,
  type BuiltSchema,
  type Register,
  type Values,
} from "../src/index.ts"
import * as Testing from "../src/Testing.ts"
import type { AppContext, Identity } from "./app/api/context.ts"
import { identityFor } from "./app/api/context.ts"
import { GraphQL } from "./app/api/graphql.ts"
import { ListUpdateInput } from "./app/api/graphql/mutation/list/list-update.ts"
import { schema } from "./app/api/schema.ts"
import { ListId } from "./app/store/ids.ts"
import type { DynamoDB } from "./app/store/models.ts"
import { List, Workspace } from "./app/store/models.ts"
import type * as executionFixture from "./fixtures/execution.ts"
import type { Labels } from "./fixtures/type-contracts.ts"
import { schema as inheritedSchema } from "./fixtures/type-contracts.ts"

describe("inference across registered real model classes", () => {
  it("preserves parent, decoded args, context and resolver requirements", () => {
    GraphQL.object(List, {
      workspace: GraphQL.field("Workspace", {
        args: { id: GraphQL.id(ListId), input: ListUpdateInput },
        resolve: (parent, args, context) => {
          expectTypeOf(parent).toEqualTypeOf<List>()
          expectTypeOf(args).toEqualTypeOf<{
            readonly id: ListId
            readonly input: ListUpdateInput
          }>()
          expectTypeOf(context).toEqualTypeOf<AppContext>()
          return Workspace.load(Workspace.key(parent.workspaceId))
        },
      }),
    })
    expectTypeOf(schema).toEqualTypeOf<BuiltSchema<DynamoDB, AppContext>>()
    expectTypeOf<
      Effect.Services<ReturnType<typeof GraphQL.execute<DynamoDB>>>
    >().toEqualTypeOf<DynamoDB>()
  })
})

it("preserves explicit connection-shaped names", () => {
  class Library extends Schema.TaggedClass<Library>()("Library", {
    id: Schema.String,
  }) {}
  class LibraryConnection extends Schema.TaggedClass<LibraryConnection>()(
    "LibraryConnection",
    { id: Schema.String },
  ) {}
  type Types = Register<typeof Library> & Register<typeof LibraryConnection>
  expectTypeOf<
    Values<Types>["LibraryConnection"]
  >().toEqualTypeOf<LibraryConnection>()
})

it("GraphQL.id is an output field without a schema and an ID argument with one", () => {
  const argument = GraphQL.id(ListId)
  expectTypeOf(argument).toEqualTypeOf<(typeof ListId)["Rebuild"]>()
  expectTypeOf<Schema.Schema.Type<typeof argument>>().toEqualTypeOf<ListId>()
  GraphQL.object(List, {
    id: GraphQL.id(),
    workspaceId: GraphQL.id({
      description: "The owning workspace.",
      authorize: (list) => list.workspaceId.length > 0,
    }),
    workspace: GraphQL.nullable("Workspace", {
      args: { id: GraphQL.id(ListId) },
      resolve: () => null,
    }),
  })
  GraphQL.object(List, {
    // @ts-expect-error an ID argument schema is not an output field
    id: GraphQL.id(ListId),
  })
  GraphQL.nullable("Workspace", {
    // @ts-expect-error the ID output field is not an argument schema
    args: { id: GraphQL.id() },
    resolve: () => null,
  })
  // @ts-expect-error ID arguments must decode from strings
  GraphQL.id(Schema.Number)
  // @ts-expect-error ID output fields take no resolver
  GraphQL.id({ resolve: () => "id" })
})

it("rejects incompatible contexts at each composition boundary", () => {
  interface Tenant {
    readonly tenant: { readonly id: string }
  }
  interface User {
    readonly user: { readonly name: string }
  }
  const tenant = createBuilder<{}, Tenant>()
  const user = createBuilder<{}, User>()
  const anonymous = createBuilder<{}>()
  const query = tenant.query({
    id: tenant.field("String", (_, __, context) => context.tenant.id),
  })
  const built = tenant.schema({ query })
  expectTypeOf(built).toEqualTypeOf<BuiltSchema<never, Tenant>>()
  user.query({
    // @ts-expect-error fields retain the context of their builder
    id: tenant.field("String", (_, __, context) => context.tenant.id),
  })
  // @ts-expect-error definitions retain context when exported through a barrel
  user.schema({ query })
  // @ts-expect-error a context-free builder cannot erase a definition's context
  anonymous.schema({ query })
  // @ts-expect-error executor builder and schema context must agree
  user.execute(built, { source: "{id}" }, { user: { name: "user" } })
  // @ts-expect-error context-free execution cannot erase the schema's context
  anonymous.execute(built, { source: "{id}" })
  // @ts-expect-error standalone execution requires the schema's request context
  execute(built, { source: "{id}" })
  // @ts-expect-error wrong context cannot widen BuiltSchema's invariant context
  execute(built, { source: "{id}" }, { user: { name: "user" } })
  // @ts-expect-error standalone subscription enforces the same context contract
  subscribe(built, { source: "subscription {id}" })
  // @ts-expect-error subscription cannot infer context from the wrong argument
  subscribe(built, { source: "subscription {id}" }, { user: { name: "user" } })
  expectTypeOf(
    execute(built, { source: "{id}" }, { tenant: { id: "tenant" } }),
  ).toEqualTypeOf<Effect.Effect<ExecutionResult>>()
})

it("retains inherited resolver services with empty implementing field maps", () => {
  expectTypeOf(inheritedSchema).toEqualTypeOf<
    BuiltSchema<Labels, { readonly allowed: boolean }>
  >()
})

it("abstract connections retain model types, decoded filters and services", () => {
  const node = Schema.Struct({ id: Schema.String })
  const union = Schema.Union([List, Workspace])
  type Types = Register<typeof List> &
    Register<typeof Workspace> &
    Register<typeof node, "Node", "interface"> &
    Register<typeof union, "SearchResult">
  expectTypeOf<Values<Types>["NodeEdge"]>().toEqualTypeOf<
    Edge<typeof node.Type>
  >()
  expectTypeOf<Values<Types>["SearchResultConnection"]>().toEqualTypeOf<
    Connection<List | Workspace>
  >()
  GraphQL.query({
    searchPage: GraphQL.connection("FavoriteTarget", {
      args: { ids: Schema.Array(GraphQL.id(ListId)) },
      nodes: (_, { ids }, context) => {
        expectTypeOf(ids).toEqualTypeOf<readonly ListId[]>()
        expectTypeOf(context).toEqualTypeOf<AppContext>()
        return Effect.forEach(ids, (id) => List.load(List.key(id)))
      },
    }),
  })
})

it("satisfies the transport's independent structural contract without erasing context or services", () => {
  type Json =
    | null
    | boolean
    | number
    | string
    | Json[]
    | { [key: string]: Json }
  interface Execution<C> {
    readonly document: DocumentNode
    readonly request: {
      query: string
      variables?: Record<string, Json> | null
      operationName?: string | null
      extensions?: Record<string, Json> | null
    }
    readonly context: C
  }
  interface FormattedResult {
    data?: Record<string, unknown> | null
    errors?: ReadonlyArray<GraphQLFormattedError>
    extensions?: Record<string, unknown>
  }
  interface Executor<C, E = never, R = never> {
    readonly schema: GraphQLSchema
    readonly execute: (
      input: Execution<C>,
    ) => Effect.Effect<FormattedResult, E, R>
    readonly subscribe: (
      input: Execution<C>,
    ) => Stream.Stream<FormattedResult, E, R>
  }
  const context: AppContext = {
    identity: { organizationId: null, hasWorkspaceScope: () => false },
    requestId: "types",
  }
  const adapter = executor(schema)
  expectTypeOf(adapter.validation).toEqualTypeOf<"executor">()
  expectTypeOf(adapter).toExtend<Executor<AppContext, never, DynamoDB>>()
  expectTypeOf<
    Parameters<typeof adapter.execute>[0]["context"]
  >().toEqualTypeOf<AppContext>()
  expectTypeOf<
    Effect.Services<ReturnType<typeof adapter.execute>>
  >().toEqualTypeOf<DynamoDB>()
  expectTypeOf<
    Stream.Services<ReturnType<typeof adapter.subscribe>>
  >().toEqualTypeOf<DynamoDB>()
  expectTypeOf(
    Testing.execute(schema, { source: "{__typename}" }, context).pipe(
      Effect.provide(Sandbox.layerLocal()),
    ),
  ).toExtend<Effect.Effect<Testing.Result, unknown>>()
})

it("subscription authorization contributes requirements to executors and testing operations", () => {
  expectTypeOf<
    Effect.Services<typeof executionFixture.runQuery>
  >().toEqualTypeOf<executionFixture.Service>()
  expectTypeOf<
    Effect.Services<typeof executionFixture.snapshotQuery>
  >().toEqualTypeOf<executionFixture.Service>()
  expectTypeOf<
    Stream.Services<typeof executionFixture.adapterEvents>
  >().toEqualTypeOf<executionFixture.Service>()
})

it("Testing.client fills request context and keeps factory services", () => {
  const anonymous = identityFor(null, [])
  const app = Testing.client(schema, {
    context: (options: { identity?: Identity }) => ({
      requestId: "test",
      identity: options.identity ?? anonymous,
    }),
  })
  expectTypeOf(app.request("{__typename}")).toEqualTypeOf<
    Effect.Effect<Testing.Result, never, DynamoDB>
  >()
  app.request("query($id: ID!) { list(id: $id) { id } }", {
    identity: anonymous,
    variables: { id: "l1" },
    operationName: "List",
  })
  // @ts-expect-error request options are the factory's options plus variables
  app.request("{__typename}", { user: "user" })
  // @ts-expect-error a schema with request context needs a context factory
  Testing.client(schema)
  // @ts-expect-error the factory must build the schema's full context
  Testing.client(schema, { context: () => ({ requestId: "test" }) })
  const required = Testing.client(schema, {
    context: (options: { identity: Identity }) => ({
      requestId: "test",
      identity: options.identity,
    }),
  })
  // @ts-expect-error required factory options must be passed to each request
  required.request("{__typename}")
  // @ts-expect-error subscriptions take the same request options
  required.subscribe('subscription { listChanged(id: "l1") { id } }')
  required.request("{__typename}", { identity: anonymous })
  const effectful = Testing.client(schema, {
    context: () =>
      Effect.gen(function* () {
        yield* Sandbox
        return { requestId: "test", identity: anonymous }
      }),
  })
  expectTypeOf<
    Effect.Services<ReturnType<typeof effectful.request>>
  >().toEqualTypeOf<DynamoDB | Sandbox>()
  expectTypeOf<
    Stream.Services<ReturnType<typeof effectful.subscribe>>
  >().toEqualTypeOf<DynamoDB | Sandbox>()
  const plain = createBuilder<{}>()
  const plainSchema = plain.schema({
    query: plain.query({ ping: plain.field("String", () => "pong") }),
  })
  expectTypeOf(Testing.client(plainSchema).request("{ping}")).toEqualTypeOf<
    Effect.Effect<Testing.Result>
  >()
})
