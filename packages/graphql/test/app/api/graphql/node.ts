import { Schema } from "effect"
import { ID } from "../../../../src/GraphQL.ts"
import { GraphQL, type Register } from "../graphql.ts"

const Node = Schema.Struct({ id: ID })
declare module "../graphql.ts" {
  interface Types extends Register<typeof Node, "Node", "interface"> {}
}

export const NodeType = GraphQL.interface("Node", Node, { id: GraphQL.id() })
