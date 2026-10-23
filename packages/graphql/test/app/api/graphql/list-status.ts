import { ListStatus } from "../../store/models.ts"
import { GraphQL, type Register } from "../graphql.ts"

declare module "../graphql.ts" {
  interface Types extends Register<typeof ListStatus, "ListStatus"> {}
}

export const ListStatusType = GraphQL.enum(
  "ListStatus",
  ListStatus.annotate({
    description: "Whether the list shows up in the sidebar.",
  }),
)
