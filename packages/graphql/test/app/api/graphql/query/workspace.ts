import { WorkspaceId } from "../../../store/ids.ts"
import { Workspace } from "../../../store/models.ts"
import { GraphQL } from "../../graphql.ts"

export const WorkspaceQuery = GraphQL.queryField(
  "workspace",
  GraphQL.field("Workspace", {
    args: { id: GraphQL.id(WorkspaceId) },
    authorize: (_, { id }, { identity }) =>
      identity.hasWorkspaceScope(id, "workspace:read"),
    resolve: (_, { id }) => Workspace.load(Workspace.key(id)),
  }),
)
