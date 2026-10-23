import * as definitions from "./definitions/index.ts"
import { GraphQL } from "./graphql.ts"
export const schema = GraphQL.schema(definitions)
