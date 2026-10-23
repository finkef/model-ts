import type { Schema } from "effect"
import {
  GraphQLObjectType,
  GraphQLNonNull,
  GraphQLUnionType,
  isOutputType,
  type GraphQLNamedType,
  type GraphQLAbstractType,
} from "graphql"
import type { BuildContext } from "./FieldTypes.ts"
import type { InputTypes } from "./InputTypes.ts"
import { metadata, invalid } from "./SchemaTypes.ts"

export interface ErrorSchema extends Schema.ConstraintDecoder<unknown, never> {
  readonly Type: Error & { readonly _tag: string }
  readonly fields: Schema.Struct.Fields
}

export type ErrorsOf<E extends readonly ErrorSchema[]> = E[number]["Type"]

export interface ResultOptions {
  /** Override the generated result name to preserve an existing schema contract. */
  readonly name?: string
  readonly description?: string
}

export const errorMember = Symbol("GraphQL error member")

export function errorName(error: ErrorSchema): string {
  const tag = error.fields._tag?.ast
  if (tag?._tag !== "Literal" || typeof tag.literal !== "string")
    return invalid("Expected a Schema.TaggedError with a literal string _tag")
  return tag.literal
}

export function makeResultTypes(
  inputs: InputTypes,
  add: <T extends GraphQLNamedType>(type: T) => T,
  objectType: (name: string, coordinate: string) => GraphQLObjectType,
  named: ReadonlyMap<string, GraphQLNamedType>,
): BuildContext["result"] {
  const errorTypes = new Map<ErrorSchema, GraphQLObjectType>()
  const errorType = (error: ErrorSchema) => {
    const existing = errorTypes.get(error)
    if (existing) return existing
    const name = errorName(error)
    const registered = named.get(name)
    if (registered) {
      if (!(registered instanceof GraphQLObjectType))
        return invalid(
          `Result error ${name} must be an object; register its error boundary as an object`,
        )
      errorTypes.set(error, registered)
      return registered
    }
    const type = add(
      new GraphQLObjectType({
        name,
        fields: Object.fromEntries(
          Object.entries(error.fields)
            .filter(([key]) => key !== "_tag")
            .map(([key, field]) => [
              key,
              (() => {
                const type = inputs.type(field.ast, `${name}.${key}`)
                if (!isOutputType(type))
                  return invalid(
                    `Error fields need an output scalar at ${name}.${key}`,
                  )
                return { type, ...metadata(field.ast) }
              })(),
            ]),
        ),
      }),
    )
    errorTypes.set(error, type)
    return type
  }
  return (ref, errors, coordinate, options = {}, nullable = false) => {
    const success = named.get(ref)
    const successMembers =
      success instanceof GraphQLUnionType
        ? success.getTypes()
        : [objectType(ref, coordinate)]
    const [parent, field = parent!] = coordinate.split(".")
    // Mutation results keep the conventional <field>Result name; other parents
    // are prefixed to avoid collisions across roots.
    const prefix = parent === "Mutation" ? "" : parent
    const name =
      options.name ??
      `${prefix}${field[0]!.toUpperCase()}${field.slice(1)}Result`
    const members = [...successMembers, ...errors.map(errorType)]
    if (new Set(members).size !== members.length)
      invalid(`Duplicate union member: ${name}`)
    const type = add(
      new GraphQLUnionType({
        name,
        ...(options.description === undefined
          ? {}
          : { description: options.description }),
        types: members,
        resolveType: (
          value,
          context,
          info,
          abstractType: GraphQLAbstractType,
        ) => {
          if (
            typeof value === "object" &&
            value !== null &&
            errorMember in value
          )
            return Reflect.get(value, errorMember)
          if (success instanceof GraphQLUnionType) {
            if (success.resolveType)
              return success.resolveType(value, context, info, abstractType)
            const tag: unknown =
              Reflect.get(Object(value), "__typename") ??
              Reflect.get(Object(value), "_tag")
            return typeof tag === "string" ? tag : undefined
          }
          return ref
        },
      }),
    )
    return nullable ? type : new GraphQLNonNull(type)
  }
}
