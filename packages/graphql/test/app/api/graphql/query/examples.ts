import { Effect, Schema } from "effect"
import { ListId, WorkspaceId } from "../../../store/ids.ts"
import { List, Workspace } from "../../../store/models.ts"
import { GraphQL } from "../../graphql.ts"
import { SlugType } from "../slug.ts"

export const FavoritesQuery = GraphQL.queryField(
  "favorites",
  GraphQL.list("FavoriteTarget"),
  () =>
    Effect.all([
      List.load(List.key(ListId.make("l1"))),
      Workspace.load(Workspace.key(WorkspaceId.make("w1"))),
    ]),
)
export const NodeQuery = GraphQL.queryField("node", "Node", () =>
  List.load(List.key(ListId.make("l1"))),
)
export const SlugQuery = GraphQL.queryField("slug", "Slug", {
  args: { value: SlugType.schema },
  authorize: () => true,
  resolve: (_, { value }) => value,
  description: "Validate a workspace URL slug.",
})
export const ServerTimeQuery = GraphQL.queryField(
  "serverTime",
  "DateTime",
  () => new Date("2026-10-22T08:00:00Z"),
)
export const MetadataQuery = GraphQL.queryField("metadata", "JSON", {
  args: { value: Schema.Json.annotate({ graphqlType: "JSON" }) },
  resolve: (_, { value }) => value,
})
export const LegacyNameQuery = GraphQL.queryField("legacyName", "String", {
  resolve: () => "Legacy workspace",
  deprecationReason: "Use workspace.name",
})
export const ListsCountedQuery = GraphQL.queryField(
  "listsCounted",
  GraphQL.connection("List"),
  {
    nodes: () =>
      List.load(List.key(ListId.make("l1"))).pipe(Effect.map((list) => [list])),
    totalCount: () => 1,
  },
)
