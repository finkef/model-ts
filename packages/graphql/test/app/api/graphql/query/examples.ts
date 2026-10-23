import { Effect, Schema } from "effect"
import { ListId, WorkspaceId } from "../../../store/ids.ts"
import { List, Workspace } from "../../../store/models.ts"
import { GraphQL, t } from "../../graphql.ts"
import { SlugType } from "../slug.ts"

export const ExamplesQuery = GraphQL.query({
  favorites: t.list("FavoriteTarget", () =>
    Effect.all([
      List.load(List.key(ListId.make("l1"))),
      Workspace.load(Workspace.key(WorkspaceId.make("w1"))),
    ]),
  ),
  node: t.field("Node", () => List.load(List.key(ListId.make("l1")))),
  slug: t.field("Slug", {
    args: { value: SlugType.schema },
    authorize: () => true,
    resolve: (_, { value }) => value,
    description: "Validate a workspace URL slug.",
  }),
  serverTime: t.field("DateTime", () => new Date("2026-10-22T08:00:00Z")),
  metadata: t.field("JSON", {
    args: { value: Schema.Json.annotate({ graphqlType: "JSON" }) },
    resolve: (_, { value }) => value,
  }),
  legacyName: t.field("String", {
    resolve: () => "Legacy workspace",
    deprecationReason: "Use workspace.name",
  }),
  listsCounted: t.connection("List", {
    nodes: () =>
      List.load(List.key(ListId.make("l1"))).pipe(Effect.map((list) => [list])),
    totalCount: () => 1,
  }),
})
