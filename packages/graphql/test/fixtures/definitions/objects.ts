import { Effect, Context, Schema } from "effect"
import { GraphQL, type Register } from "../graphql.ts"
import { Fixture, List, Workspace, Status, Token, fixture } from "../models.ts"
export class Guard extends Context.Service<
  Guard,
  { readonly allowed: boolean }
>()("Guard") {}
export class Fetch extends Context.Service<Fetch, { readonly value: string }>()(
  "Fetch",
) {}
const OtherStatus = Schema.Literals(["DRAFT"])
declare module "../graphql.ts" {
  interface Types
    extends
      Register<typeof List, "List">,
      Register<typeof Workspace, "Workspace">,
      Register<typeof Fixture, "Fixture">,
      Register<typeof Status, "Status">,
      Register<typeof OtherStatus, "OtherStatus">,
      Register<typeof Token, "Token"> {}
}
export const OtherStatusType = GraphQL.enum("OtherStatus", OtherStatus)
export const StatusType = GraphQL.enum("Status", Status)
export const TokenType = GraphQL.scalar("Token", Token)
export const ListType = GraphQL.object(
  "List",
  List,
  {},
  { id: GraphQL.id(), name: GraphQL.string() },
)
export const WorkspaceType = GraphQL.object(
  "Workspace",
  Workspace,
  {},
  { id: GraphQL.id(), title: GraphQL.string() },
)
export const FixtureType = GraphQL.object(
  "Fixture",
  Fixture,
  {},
  {
    id: GraphQL.id(),
    text: GraphQL.string({
      description: "Text",
      deprecationReason: "Prefer computed",
      authorize: () => true,
    }),
    integer: GraphQL.int(),
    float: GraphQL.float(),
    boolean: GraphQL.boolean(),
    date: GraphQL.dateTime(),
    json: GraphQL.json(),
    custom: GraphQL.field("Token"),
    status: GraphQL.field("Status"),
    workspace: GraphQL.field("Workspace"),
    nodes: GraphQL.list("List"),
    edge: GraphQL.field("ListEdge"),
    page: GraphQL.connection("List"),
    pageInfo: GraphQL.field("PageInfo"),
    optional: GraphQL.nullable.string(),
    nullish: GraphQL.nullable.string(),
    option: GraphQL.nullable.string(),
    maybeNodes: GraphQL.nullable(GraphQL.list("List")),
    maybeWorkspace: GraphQL.nullable("Workspace", {
      description: "Optional workspace",
      authorize: () => true,
    }),
    maybeStatus: GraphQL.nullable("Status"),
    nullableElements: GraphQL.list(GraphQL.nullable("String")),
    computed: GraphQL.field("String", (source) => source.text),
    resolvedList: GraphQL.list("List", () => fixture.nodes),
    resolvedConnection: GraphQL.connection("List", {
      nodes: () => fixture.nodes,
      totalCount: () => fixture.nodes.length,
    }),
    guarded: GraphQL.field("String", {
      authorize: () => Guard.useSync((s) => s.allowed),
      resolve: () => Fetch.useSync((s) => s.value),
    }),
    plainNullable: GraphQL.nullable("String", () => undefined),
    effectNullable: GraphQL.nullable("String", () => Effect.succeed(undefined)),
  },
)
export const Query = GraphQL.query({
  fixture: GraphQL.field("Fixture", () => fixture),
})
