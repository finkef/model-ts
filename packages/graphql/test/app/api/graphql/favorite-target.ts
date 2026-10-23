import { Schema } from "effect"
import { List, Workspace } from "../../store/models.ts"
import { GraphQL, type Register } from "../graphql.ts"

const FavoriteTarget = Schema.Union([List, Workspace])
declare module "../graphql.ts" {
  interface Types extends Register<typeof FavoriteTarget, "FavoriteTarget"> {}
}
export const FavoriteTargetType = GraphQL.union(
  "FavoriteTarget",
  FavoriteTarget,
)
