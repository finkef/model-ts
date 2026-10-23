import { List, Workspace } from "../../store/models.ts"
import { GraphQL, t, type Register } from "../graphql.ts"

declare module "../graphql.ts" {
  interface Types extends Register<typeof Workspace> {}
}

export const WorkspaceType = GraphQL.object(
  Workspace,
  { implements: ["Node"] },
  {
    id: t.id(),
    name: t.string({ description: "Shown in the sidebar and in mentions." }),
    createdAt: t.dateTime(),
    lists: t.connection("List", (workspace, args) =>
      List.paginate(args, {
        IndexName: "GSI1",
        KeyConditionExpression: "GSI1PK = :pk",
        ExpressionAttributeValues: { ":pk": `WORKSPACE#${workspace.id}` },
      }),
    ),
  },
)
