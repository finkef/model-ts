import type { SchemaAST } from "effect"
import { Schema } from "effect"
import {
  type GraphQLOutputType,
  type GraphQLNullableType,
  type GraphQLInputType,
  isNonNullType,
} from "graphql"

declare module "effect/Schema" {
  namespace Annotations {
    interface Annotations {
      readonly specifiedByURL?: string
      readonly graphqlType?: string
      readonly graphqlDefault?: unknown
      readonly deprecationReason?: string
    }
  }
}

export const ID = Schema.String.annotate({ graphqlType: "ID" })

export function id<S extends Schema.Top & Schema.Schema<string>>(
  schema: S,
): S["Rebuild"] {
  return schema.annotate({ graphqlType: "ID" })
}

export const JSONValue = Schema.Unknown.annotate({ graphqlType: "JSON" })

/** DateTime inputs decode to normalized ISO strings. */
export const DateTime = Schema.String.annotate({ graphqlType: "DateTime" })

export function input<S extends Schema.Top>(
  name: string,
  schema: S,
): S["Rebuild"] {
  // Schema annotations preserve the codec and its fields (including Schema classes).
  return schema.annotate({ graphqlType: name })
}

export function defaulted<
  S extends Schema.Top & Schema.Codec<unknown, unknown>,
>(schema: S, value: NoInfer<S["Type"]>): S["Rebuild"] {
  Schema.decodeUnknownSync(Schema.toType(schema))(value)
  return schema.annotate({ graphqlDefault: value })
}

export const invalid = (message: string): never => {
  throw new Error(message)
}

export const annotations = (ast: SchemaAST.AST) => ({
  ...Schema.resolveAnnotations(Schema.make(ast)),
  ...ast.context?.annotations,
})

export function descriptionOf(ast: SchemaAST.AST): string | undefined {
  const own = annotations(ast).description
  return typeof own === "string"
    ? own
    : ast._tag === "Union"
      ? ast.types.map(descriptionOf).find((v) => v !== undefined)
      : undefined
}

export function metadata(ast: SchemaAST.AST) {
  const description = descriptionOf(ast)
  const deprecationReason = annotations(ast).deprecationReason
  return {
    ...(description === undefined ? {} : { description }),
    ...(deprecationReason === undefined ? {} : { deprecationReason }),
  }
}

export function representation(node: {
  readonly annotations?: Schema.Annotations.Annotations | undefined
}): unknown {
  return Reflect.get(Object(node.annotations?.representation), "id")
}

export function unwrap(
  type: GraphQLOutputType,
): GraphQLOutputType & GraphQLNullableType
export function unwrap(
  type: GraphQLInputType,
): GraphQLInputType & GraphQLNullableType
export function unwrap(type: GraphQLOutputType | GraphQLInputType) {
  return isNonNullType(type) ? type.ofType : type
}
