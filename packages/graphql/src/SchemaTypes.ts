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

/** The annotations of the type itself, without those of the key it is used under. */
const typeAnnotations = (ast: SchemaAST.AST): Schema.Annotations.Annotations =>
  Schema.resolveAnnotations(Schema.make(ast)) ?? {}

export function descriptionOf(
  ast: SchemaAST.AST,
  annotationsOf: (
    ast: SchemaAST.AST,
  ) => Schema.Annotations.Annotations = annotations,
): string | undefined {
  const own = annotationsOf(ast).description
  return typeof own === "string"
    ? own
    : ast._tag === "Union"
      ? ast.types
          .map((type) => descriptionOf(type, annotationsOf))
          .find((v) => v !== undefined)
      : undefined
}

export function metadata(
  ast: SchemaAST.AST,
  annotationsOf: (
    ast: SchemaAST.AST,
  ) => Schema.Annotations.Annotations = annotations,
) {
  const description = descriptionOf(ast, annotationsOf)
  const deprecationReason = annotationsOf(ast).deprecationReason
  return {
    ...(description === undefined ? {} : { description }),
    ...(deprecationReason === undefined ? {} : { deprecationReason }),
  }
}

/**
 * Metadata of a named type. Key annotations (`annotateKey`) describe the
 * argument or field that uses the type, never the type.
 */
export const typeMetadata = (ast: SchemaAST.AST) =>
  metadata(ast, typeAnnotations)

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
