import { Effect } from "effect"
import { List, Workspace } from "../../store/models.ts"
import { GraphQL, t, type Register } from "../graphql.ts"

declare module "../graphql.ts" {
  interface Types extends Register<typeof List> {}
}

export const ListType = GraphQL.object(
  List,
  { implements: ["Node"] },
  {
    organizationId: t.id(),
    workspaceId: t.id(),
    createdBy: t.string(),
    name: t.string({ description: "Shown in the sidebar and in mentions." }),
    starred: t.nullable.boolean(),
    status: t.field("ListStatus", {
      description: "Whether the list shows up in the sidebar.",
    }),
    createdAt: t.dateTime(),
    updatedAt: t.nullable.dateTime(),
    workspace: t.nullable("Workspace", {
      authorize: (list, _, { identity }) =>
        identity.hasWorkspaceScope(list.workspaceId, "workspace:read"),
      resolve: (list) =>
        Workspace.load(Workspace.key(list.workspaceId)).pipe(
          Effect.catchTag("ItemNotFoundError", () => Effect.succeed(null)),
        ),
    }),
  },
)
