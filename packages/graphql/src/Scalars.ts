import { Schema } from "effect"
import {
  GraphQLScalarType,
  GraphQLEnumType,
  valueFromASTUntyped,
  Kind,
  type ValueNode,
} from "graphql"
import { fieldError } from "./Errors.ts"
import { guardScalar } from "./ScalarBoundary.ts"
import { invalid, metadata, annotations } from "./SchemaTypes.ts"

export const date = (value: unknown) => {
  if (typeof value !== "string")
    return invalid("DateTime expects an ISO string")
  const result = new Date(value)
  return Number.isFinite(result.getTime())
    ? result
    : invalid("Invalid DateTime")
}

const invalidInput = (message: string): never => {
  throw fieldError(message, { code: "BAD_USER_INPUT" })
}

const dateInput = (value: unknown): string => {
  try {
    return date(value).toISOString()
  } catch {
    return invalidInput("DateTime expects a valid ISO string")
  }
}

export const dateTime = () =>
  guardScalar(
    new GraphQLScalarType({
      name: "DateTime",
      description: "A valid DateTime value",
      serialize: (value) =>
        value instanceof Date && Number.isFinite(value.getTime())
          ? value.toISOString()
          : date(value).toISOString(),
      parseValue: dateInput,
      parseLiteral: (node) =>
        node.kind === Kind.STRING
          ? dateInput(node.value)
          : invalidInput("DateTime expects a string literal"),
    }),
  )

export function scalar<A, I>(
  name: string,
  codec: Schema.Codec<A, I>,
): GraphQLScalarType {
  const decode = Schema.decodeUnknownSync(codec)
  const encode = Schema.encodeUnknownSync(codec)
  const decodeInput = (value: unknown) => {
    try {
      return decode(value)
    } catch (error) {
      if (Schema.isSchemaError(error))
        return invalidInput(`Invalid value for ${name}`)
      throw error
    }
  }
  return guardScalar(
    new GraphQLScalarType({
      name,
      ...metadata(codec.ast),
      specifiedByURL: annotations(codec.ast).specifiedByURL,
      serialize: encode,
      // Keep the encoded representation until the argument Struct converts it for the resolver.
      parseValue: (value) => {
        decodeInput(value)
        return value
      },
      parseLiteral: (node, variables) => {
        if (node.kind === Kind.ENUM)
          return invalidInput(
            `${name} cannot represent an enum literal; quote the value as a string`,
          )
        const value: unknown = valueFromASTUntyped(node, variables)
        decodeInput(value)
        return value
      },
    }),
  )
}

function jsonLiteral(
  node: ValueNode,
  variables?: Readonly<Record<string, unknown>> | null,
): unknown {
  switch (node.kind) {
    case Kind.ENUM:
      return invalidInput(
        "JSON cannot represent an enum literal; quote the value as a string",
      )
    case Kind.OBJECT:
      return Object.fromEntries(
        node.fields.map((field) => [
          field.name.value,
          jsonLiteral(field.value, variables),
        ]),
      )
    case Kind.LIST:
      return node.values.map((value) => jsonLiteral(value, variables))
    default:
      return valueFromASTUntyped(node, variables)
  }
}

/** Like graphql-type-json, variables and outputs are deliberately unrestricted JS values. */
export const jsonScalar = () =>
  guardScalar(
    new GraphQLScalarType({
      name: "JSON",
      description:
        "The `JSON` scalar type represents JSON values as specified by [ECMA-404](http://www.ecma-international.org/publications/files/ECMA-ST/ECMA-404.pdf).",
      serialize: (value) => value,
      parseValue: (value) => value,
      parseLiteral: jsonLiteral,
    }),
  )

export function enumeration(
  name: string,
  schema: Schema.Schema<string>,
): GraphQLEnumType {
  const ast = schema.ast
  const values = ast._tag === "Union" ? ast.types : [ast]
  return new GraphQLEnumType({
    name,
    ...metadata(ast),
    values: Object.fromEntries(
      values.map((value) => {
        if (value._tag !== "Literal" || typeof value.literal !== "string")
          return invalid(`${name}: enums require string Schema.Literals`)
        return [value.literal, { value: value.literal, ...metadata(value) }]
      }),
    ),
  })
}
