import { Stream } from "effect"
import { ListId } from "../../../store/ids.ts"
import { List } from "../../../store/models.ts"
import { GraphQL, t } from "../../graphql.ts"

export const ListChangedSubscription = GraphQL.subscription({
  listChanged: t.events("List", {
    args: { id: GraphQL.id(ListId) },
    authorize: (list, _, { identity }) =>
      identity.hasWorkspaceScope(list.workspaceId, "workspace:read"),
    subscribe: (_, { id }) => Stream.fromEffect(List.load(List.key(id))),
  }),
})
