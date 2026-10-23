import { Effect, Schema } from "effect"
import { ListId, WorkspaceId } from "../../../store/ids.ts"
import { List, Workspace } from "../../../store/models.ts"
import { GraphQL } from "../../graphql.ts"

/** Search pages can contain different registered object models. */
export const SearchQuery = GraphQL.query({
  search: GraphQL.connection("FavoriteTarget", {
    args: { terms: Schema.Array(Schema.String) },
    nodes: (_, { terms }) =>
      terms.length === 0
        ? Effect.succeed([])
        : Effect.all([
            List.load(List.key(ListId.make("l1"))),
            Workspace.load(Workspace.key(WorkspaceId.make("w1"))),
          ]),
  }),
  nodePage: GraphQL.connection("Node", {
    nodes: () =>
      List.load(List.key(ListId.make("l1"))).pipe(Effect.map((node) => [node])),
  }),
  searchEdge: GraphQL.field("FavoriteTargetEdge", () =>
    List.load(List.key(ListId.make("l1"))).pipe(
      Effect.flatMap((node) =>
        Effect.map(node.cursor(), (cursor) => ({ node, cursor })),
      ),
    ),
  ),
})
