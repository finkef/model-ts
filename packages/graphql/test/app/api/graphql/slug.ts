import { Slug } from "../../store/slug.ts"
import { GraphQL, type Register } from "../graphql.ts"

declare module "../graphql.ts" {
  interface Types extends Register<typeof Slug, "Slug"> {}
}
export const SlugType = GraphQL.scalar("Slug", Slug)
