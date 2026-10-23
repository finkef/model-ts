import { Effect, Context, Schema } from "effect"
import { GraphQL, t, type Register } from "../graphql.ts"
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
  { id: t.id(), name: t.string() },
)
export const WorkspaceType = GraphQL.object(
  "Workspace",
  Workspace,
  {},
  { id: t.id(), title: t.string() },
)
export const FixtureType = GraphQL.object(
  "Fixture",
  Fixture,
  {},
  {
    id: t.id(),
    text: t.string({
      description: "Text",
      deprecationReason: "Prefer computed",
      authorize: () => true,
    }),
    integer: t.int(),
    float: t.float(),
    boolean: t.boolean(),
    date: t.dateTime(),
    json: t.json(),
    custom: t.field("Token"),
    status: t.field("Status"),
    workspace: t.field("Workspace"),
    nodes: t.list("List"),
    edge: t.field("ListEdge"),
    page: t.connection("List"),
    pageInfo: t.field("PageInfo"),
    optional: t.nullable.string(),
    nullish: t.nullable.string(),
    option: t.nullable.string(),
    maybeNodes: t.nullable(t.list("List")),
    maybeWorkspace: t.nullable("Workspace", {
      description: "Optional workspace",
      authorize: () => true,
    }),
    maybeStatus: t.nullable("Status"),
    nullableElements: t.list(t.nullable("String")),
    computed: t.field("String", (source) => source.text),
    resolvedList: t.list("List", () => fixture.nodes),
    resolvedConnection: t.connection("List", {
      nodes: () => fixture.nodes,
      totalCount: () => fixture.nodes.length,
    }),
    guarded: t.field("String", {
      authorize: () => Guard.useSync((s) => s.allowed),
      resolve: () => Fetch.useSync((s) => s.value),
    }),
    plainNullable: t.nullable("String", () => undefined),
    effectNullable: t.nullable("String", () => Effect.succeed(undefined)),
  },
)
export const Query = GraphQL.query({
  fixture: t.field("Fixture", () => fixture),
})
