import { Context, Effect, Schema } from "effect"
import { createBuilder } from "../src/GraphQL.ts"
import type {
  Register,
  BuiltSchema,
  Connection,
  Edge,
  PageInfo,
  Requirements,
  Values,
} from "../src/GraphQL.ts"
import { identityFor, type AppContext } from "./app/api/context.ts"
import { GraphQL, type Types } from "./app/api/graphql.ts"
import type * as definitions from "./app/api/graphql/index.ts"
import { ListUpdateInput } from "./app/api/graphql/mutation/list/list-update.ts"
import { schema } from "./app/api/schema.ts"
import { ListId } from "./app/store/ids.ts"
import type { DynamoDB } from "./app/store/models.ts"
import { DynamoDBMemory, List, Workspace } from "./app/store/models.ts"
import type { Guard, Fetch } from "./fixtures/definitions/index.ts"
import type { schema as fixtureSchema } from "./fixtures/schema.ts"
type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? true
    : false
type Assert<T extends true> = T
export type ListValue = Assert<Equal<Values<Types>["List"], List>>
export type EdgeValue = Assert<Equal<Values<Types>["ListEdge"], Edge<List>>>
export type ConnectionValue = Assert<
  Equal<Values<Types>["WorkspaceConnection"], Connection<Workspace>>
>
export type PageInfoValue = Assert<Equal<Values<Types>["PageInfo"], PageInfo>>
export type EnumKind = Assert<Equal<Types["ListStatus"]["kind"], "enum">>
export type NoEnumEdge = Assert<
  Equal<Extract<keyof Values<Types>, "ListStatusEdge">, never>
>
export type RootsExcluded = Assert<
  Equal<Extract<keyof Types, "Query" | "Mutation" | "ListUpdateInput">, never>
>
export type ExactRequirements = Assert<
  Equal<Requirements<typeof definitions>, DynamoDB>
>
export type ExactSchema = Assert<
  Equal<typeof schema, BuiltSchema<DynamoDB, AppContext>>
>
export type ExactFixtureSchema = Assert<
  Equal<typeof fixtureSchema, BuiltSchema<Guard | Fetch>>
>
export type AuthorizationNeedsNoService = Assert<
  Equal<Requirements<{ ListType: typeof definitions.ListType }>, DynamoDB>
>
export const execution = GraphQL.execute(
  schema,
  { source: '{workspace(id:"w1"){id}}' },
  { identity: identityFor(null, []), requestId: "t" },
)
// @ts-expect-error the request context is required by the app builder
GraphQL.execute(schema, { source: "{__typename}" })
// @ts-expect-error the request context must be an AppContext
GraphQL.execute(schema, { source: "{__typename}" }, { requestId: "t" })
export type ExactExecution = Assert<
  Equal<Effect.Services<typeof execution>, DynamoDB>
>
export type NoTypedErrors = Assert<Equal<Effect.Error<typeof execution>, never>>
export const complete = execution.pipe(Effect.provide(DynamoDBMemory))
export type Complete = Assert<Equal<Effect.Services<typeof complete>, never>>
const id = GraphQL.id(ListId)
export type IdBrandPreserved = Assert<Equal<typeof id.Type, ListId>>
const input = Schema.Struct({ id: GraphQL.id(ListId), input: ListUpdateInput })
export type InputClassPreserved = Assert<
  Equal<
    typeof input.Type,
    { readonly id: ListId; readonly input: ListUpdateInput }
  >
>
export class Extra extends Context.Service<Extra, { readonly n: number }>()(
  "Extra",
) {}
const defaults = GraphQL.object(Workspace, {
  id: GraphQL.id({ authorize: () => Extra.useSync((s) => s.n > 0) }),
})
export type DefaultAuthRequirements = Assert<
  Equal<Requirements<{ defaults: typeof defaults }>, Extra>
>
const counted = GraphQL.queryField(
  "page",
  GraphQL.connection("List", {
    resolve: () =>
      List.paginate(
        { first: 1 },
        {
          IndexName: "GSI1",
          KeyConditionExpression: "GSI1PK = :pk",
          ExpressionAttributeValues: { ":pk": "WORKSPACE#w1" },
        },
      ),
    totalCount: () => Extra.useSync((s) => s.n),
    authorize: (_, __, { identity }) => identity.organizationId !== null,
  }),
)
export type IndependentRequirements = Assert<
  Equal<Requirements<{ counted: typeof counted }>, DynamoDB | Extra>
>

// Explicit names remain available without touching the model.
const aliasBuilder = createBuilder<Register<typeof List, "ListAlias">>()
const alias = aliasBuilder.object(
  "ListAlias",
  List,
  {},
  { id: aliasBuilder.id() },
)
export type AliasName = Assert<Equal<(typeof alias)["~name"], "ListAlias">>
export type UnionKind = Assert<Equal<Types["FavoriteTarget"]["kind"], "union">>
export type UnionValue = Assert<
  Equal<Values<Types>["FavoriteTarget"], List | Workspace>
>
export type SubscriptionRequirements = Assert<
  Equal<
    Requirements<{ subscription: typeof definitions.ListChangedSubscription }>,
    DynamoDB
  >
>
export type MutationRequirements = Assert<
  Equal<
    Requirements<{ mutation: typeof definitions.ListUpdateMutation }>,
    DynamoDB
  >
>
