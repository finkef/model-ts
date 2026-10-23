import { Effect } from "effect"
import { List, Workspace } from "../../store/models.ts"
import { GraphQL, type Register } from "../graphql.ts"

declare module "../graphql.ts" {
  interface Types extends Register<typeof List> {}
}

export const ListType = GraphQL.object(
  List,
  { implements: ["Node"] },
  {
    organizationId: GraphQL.id(),
    workspaceId: GraphQL.id(),
    createdBy: GraphQL.string(),
    name: GraphQL.string({
      description: "Shown in the sidebar and in mentions.",
    }),
    starred: GraphQL.nullable.boolean(),
    status: GraphQL.field("ListStatus", {
      description: "Whether the list shows up in the sidebar.",
    }),
    createdAt: GraphQL.dateTime(),
    updatedAt: GraphQL.nullable.dateTime(),
    workspace: GraphQL.nullable("Workspace", {
      authorize: (list, _, { identity }) =>
        identity.hasWorkspaceScope(list.workspaceId, "workspace:read"),
      resolve: (list) =>
        Workspace.load(Workspace.key(list.workspaceId)).pipe(
          Effect.catchTag("ItemNotFoundError", () => Effect.succeed(null)),
        ),
    }),
  },
)
