import { Effect, Schema } from "effect"
import { ListId, WorkspaceId } from "../../../store/ids.ts"
import { List, Workspace } from "../../../store/models.ts"
import { GraphQL } from "../../graphql.ts"
import { SlugType } from "../slug.ts"

export const ExamplesQuery = GraphQL.query({
  favorites: GraphQL.list("FavoriteTarget", () =>
    Effect.all([
      List.load(List.key(ListId.make("l1"))),
      Workspace.load(Workspace.key(WorkspaceId.make("w1"))),
    ]),
  ),
  node: GraphQL.field("Node", () => List.load(List.key(ListId.make("l1")))),
  slug: GraphQL.field("Slug", {
    args: { value: SlugType.schema },
    authorize: () => true,
    resolve: (_, { value }) => value,
    description: "Validate a workspace URL slug.",
  }),
  serverTime: GraphQL.field("DateTime", () => new Date("2026-10-22T08:00:00Z")),
  metadata: GraphQL.field("JSON", {
    args: { value: Schema.Json.annotate({ graphqlType: "JSON" }) },
    resolve: (_, { value }) => value,
  }),
  legacyName: GraphQL.field("String", {
    resolve: () => "Legacy workspace",
    deprecationReason: "Use workspace.name",
  }),
  listsCounted: GraphQL.connection("List", {
    nodes: () =>
      List.load(List.key(ListId.make("l1"))).pipe(Effect.map((list) => [list])),
    totalCount: () => 1,
  }),
})
