import {
  GraphQLDirective,
  GraphQLInputObjectType,
  GraphQLInterfaceType,
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
  GraphQLScalarType,
  GraphQLSchema,
  GraphQLUnionType,
  isInputObjectType,
  isInputType,
  isInterfaceType,
  isListType,
  isNonNullType,
  isObjectType,
  isOutputType,
  isScalarType,
  isUnionType,
  specifiedScalarTypes,
  type GraphQLInputType,
  type GraphQLNamedType,
  type GraphQLOutputType,
  type GraphQLType,
} from "graphql"
import { InputError } from "./InputError.ts"

const schemas = new WeakMap<GraphQLSchema, GraphQLSchema>()

/**
 * graphql-js sometimes reports a scalar's GraphQLError verbatim. Wrapping custom
 * input parsers preserves provenance even in that case. Clone the type graph:
 * mutating application scalars would affect other executors and concurrent requests.
 */
export const inputSafeSchema = (schema: GraphQLSchema): GraphQLSchema => {
  const cached = schemas.get(schema)
  if (cached) return cached
  const mapped = new Map<GraphQLNamedType, GraphQLNamedType>()
  const input = (type: GraphQLInputType): GraphQLInputType => {
    const result = replace(type)
    if (!isInputType(result))
      throw new Error("Input type mapping changed category")
    return result
  }
  const output = (type: GraphQLOutputType): GraphQLOutputType => {
    const result = replace(type)
    if (!isOutputType(result))
      throw new Error("Output type mapping changed category")
    return result
  }
  const object = (type: GraphQLObjectType): GraphQLObjectType => {
    const result = named(type)
    if (!isObjectType(result))
      throw new Error("Object mapping changed category")
    return result
  }
  const iface = (type: GraphQLInterfaceType): GraphQLInterfaceType => {
    const result = named(type)
    if (!isInterfaceType(result))
      throw new Error("Interface mapping changed category")
    return result
  }
  function replace(type: GraphQLType): GraphQLType {
    if (isListType(type)) return new GraphQLList(replace(type.ofType))
    if (isNonNullType(type)) {
      const inner = replace(type.ofType)
      if (isNonNullType(inner)) throw new Error("Nested non-null type")
      return new GraphQLNonNull(inner)
    }
    return named(type)
  }
  function named(type: GraphQLNamedType): GraphQLNamedType {
    const previous = mapped.get(type)
    if (previous) return previous
    let result: GraphQLNamedType = type
    if (isScalarType(type) && !specifiedScalarTypes.includes(type)) {
      result = new GraphQLScalarType({
        ...type.toConfig(),
        parseValue(value) {
          try {
            return type.parseValue(value)
          } catch (cause) {
            throw new InputError(cause)
          }
        },
        parseLiteral(node, variables) {
          try {
            return type.parseLiteral(node, variables)
          } catch (cause) {
            throw new InputError(cause)
          }
        },
      })
    } else if (isObjectType(type) || isInterfaceType(type)) {
      const config = type.toConfig()
      const fields = () =>
        Object.fromEntries(
          Object.entries(config.fields).map(([name, field]) => [
            name,
            {
              ...field,
              type: output(field.type),
              args: Object.fromEntries(
                Object.entries(field.args ?? {}).map(([name, argument]) => [
                  name,
                  { ...argument, type: input(argument.type) },
                ]),
              ),
            },
          ]),
        )
      const interfaces = () => config.interfaces.map(iface)
      result = isObjectType(type)
        ? new GraphQLObjectType({ ...type.toConfig(), fields, interfaces })
        : new GraphQLInterfaceType({ ...type.toConfig(), fields, interfaces })
    } else if (isInputObjectType(type)) {
      const config = type.toConfig()
      result = new GraphQLInputObjectType({
        ...config,
        fields: () =>
          Object.fromEntries(
            Object.entries(config.fields).map(([name, field]) => [
              name,
              { ...field, type: input(field.type) },
            ]),
          ),
      })
    } else if (isUnionType(type)) {
      const config = type.toConfig()
      result = new GraphQLUnionType({
        ...config,
        types: () => config.types.map(object),
      })
    }
    mapped.set(type, result)
    return result
  }
  const config = schema.toConfig()
  const result = new GraphQLSchema({
    ...config,
    query: config.query && object(config.query),
    mutation: config.mutation && object(config.mutation),
    subscription: config.subscription && object(config.subscription),
    // Introspection types belong to graphql-js and must keep their canonical identity.
    types: config.types
      .filter((type) => !type.name.startsWith("__"))
      .map(named),
    directives: config.directives.map((directive) => {
      const config = directive.toConfig()
      return new GraphQLDirective({
        ...config,
        args: Object.fromEntries(
          Object.entries(config.args).map(([name, argument]) => [
            name,
            { ...argument, type: input(argument.type) },
          ]),
        ),
      })
    }),
  })
  schemas.set(schema, result)
  schemas.set(result, result)
  return result
}
