import { GraphQL } from "./graphql.ts"
import * as definitions from "./graphql/index.ts"

export const schema = GraphQL.schema(definitions)
