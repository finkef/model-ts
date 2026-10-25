import { Schema, SchemaAST } from "effect"
import {
  GraphQLBoolean,
  GraphQLFloat,
  GraphQLInt,
  GraphQLList,
  GraphQLNonNull,
  GraphQLScalarType,
  GraphQLString,
  type GraphQLNamedType,
  type GraphQLInputType,
  GraphQLInputObjectType,
  GraphQLEnumType,
  getNamedType,
  isInputType,
} from "graphql"
import { dateTime, jsonScalar } from "./Scalars.ts"
import {
  invalid,
  annotations,
  metadata,
  representation,
  typeMetadata,
  unwrap,
} from "./SchemaTypes.ts"

export function structure(ast: SchemaAST.AST): SchemaAST.Objects | undefined {
  if (ast._tag === "Objects") return ast
  if (ast._tag === "Declaration" && ast.typeParameters[0]?._tag === "Objects")
    return ast.typeParameters[0]
  return undefined
}

/**
 * Metadata of an argument or input field. A named input object's description
 * belongs to its `input` definition, so an argument or field of that type is
 * only described by its own key annotations (`GraphQL.arg`). Scalar, enum and
 * list arguments keep their schema's metadata.
 */
function describeUse(ast: SchemaAST.AST, type: GraphQLInputType) {
  const own = metadata(ast)
  if (!(getNamedType(type) instanceof GraphQLInputObjectType)) return own
  const { description: _inherited, ...rest } = own
  const key = ast.context?.annotations?.description
  return typeof key === "string" ? { ...rest, description: key } : rest
}

/** One mapper per assembled schema: names and AST identity never leak between schemas. */
export class InputTypes {
  private readonly registered = new Map<SchemaAST.AST, GraphQLInputType>()
  // optionalKey() and annotate() copy Union ASTs but retain their member array.
  // Its identity links model enum schemas without matching unrelated literals by value.
  private readonly enums = new Map<readonly SchemaAST.AST[], GraphQLEnumType>()
  private readonly owners = new Map<string, SchemaAST.Objects>()
  constructor(
    private readonly named: Map<string, GraphQLNamedType>,
    private readonly add: <T extends GraphQLNamedType>(type: T) => T,
  ) {}
  register(ast: SchemaAST.AST, type: GraphQLInputType) {
    if (ast._tag === "Union" && type instanceof GraphQLEnumType) {
      const existing = this.enums.get(ast.types)
      if (existing && existing !== type)
        invalid(
          `Enum schema is registered as both ${existing.name} and ${type.name}; use a separate Schema.Literals instance for each enum`,
        )
      this.enums.set(ast.types, type)
    }
    this.registered.set(ast, type)
  }
  argument(
    schema: Schema.ConstraintDecoder<unknown, never>,
    coordinate: string,
  ) {
    const defaultValue: unknown = annotations(schema.ast).graphqlDefault
    const type = this.type(schema.ast, coordinate)
    return {
      type,
      ...describeUse(schema.ast, type),
      ...(defaultValue === undefined
        ? {}
        : {
            // defaulted() accepts a codec; the argument map retains its decoder view.
            defaultValue: Schema.encodeUnknownSync(
              schema as Schema.Codec<unknown, unknown>,
            )(defaultValue),
          }),
    }
  }
  type(ast: SchemaAST.AST, coordinate: string): GraphQLInputType {
    const nonnull = !SchemaAST.isOptional(ast)
    const wrap = (type: GraphQLInputType) =>
      nonnull ? new GraphQLNonNull(unwrap(type)) : unwrap(type)
    const registered = this.registered.get(ast)
    if (registered) return wrap(registered)
    const ref =
      annotations(ast).graphqlType ??
      (structure(ast) ? annotations(ast).identifier : undefined)
    if (ref) {
      const existing = this.named.get(ref)
      if (existing) {
        if (!isInputType(existing))
          return invalid(`${coordinate}: ${ref} is not an input type`)
        const owner = this.owners.get(ref)
        if (
          owner &&
          owner.propertySignatures !== structure(ast)?.propertySignatures
        )
          return invalid(`Duplicate input name: ${ref}`)
        if (structure(ast) && !(existing instanceof GraphQLInputObjectType))
          return invalid(`Duplicate input name: ${ref}`)
        // An input object has one description. Another one at a later use
        // would make the result depend on which use was assembled first.
        const description = typeMetadata(ast).description
        if (owner && description !== existing.description)
          return invalid(
            `${coordinate}: input ${ref} is described differently here than where it was first used. Keep its description on the schema that defines it, and describe this argument or field with GraphQL.arg(schema, { description })`,
          )
        return wrap(existing)
      }
      if (ref === "DateTime") return wrap(this.add(dateTime()))
      if (ref === "JSON") return wrap(this.add(jsonScalar()))
      const encoded = structure(ast) ?? SchemaAST.toEncoded(ast)
      if (encoded._tag !== "Objects")
        return invalid(`${coordinate}: unregistered scalar or enum ${ref}`)
      if (encoded.indexSignatures.length)
        return invalid(
          `${coordinate}: input dictionaries require a JSON scalar`,
        )
      this.owners.set(ref, encoded)
      const type = this.add(
        new GraphQLInputObjectType({
          name: ref,
          ...typeMetadata(ast),
          fields: () =>
            Object.fromEntries(
              encoded.propertySignatures.map((p) => {
                if (typeof p.name !== "string")
                  return invalid(`${coordinate}: input keys must be strings`)
                return [
                  p.name,
                  this.argument(Schema.make(p.type), `${ref}.${p.name}`),
                ]
              }),
            ),
        }),
      )
      return wrap(type)
    }
    if (ast._tag === "Union") {
      const enumeration = this.enums.get(ast.types)
      if (enumeration) return wrap(enumeration)
      const members = ast.types.filter(
        (t) => t._tag !== "Null" && t._tag !== "Undefined",
      )
      if (members.length !== 1 || members.length === ast.types.length)
        return invalid(
          `Unregistered literal or union schema at ${coordinate}; register this exact model schema with GraphQL.enum(name, schema) and export the definition in the schema barrel`,
        )
      return unwrap(this.type(members[0]!, coordinate))
    }
    if (
      ast._tag === "Declaration" &&
      representation(ast) === "effect/schema/Date"
    ) {
      const existing = this.named.get("DateTime")
      return wrap(
        existing instanceof GraphQLScalarType ? existing : this.add(dateTime()),
      )
    }
    if (ast.encoding) return this.type(SchemaAST.toEncoded(ast), coordinate)
    switch (ast._tag) {
      case "String":
        return wrap(GraphQLString)
      case "Number":
        return wrap(
          ast.checks?.some((c) => representation(c) === "effect/schema/isInt")
            ? GraphQLInt
            : GraphQLFloat,
        )
      case "Boolean":
        return wrap(GraphQLBoolean)
      case "Literal":
        return wrap(
          typeof ast.literal === "string"
            ? GraphQLString
            : typeof ast.literal === "boolean"
              ? GraphQLBoolean
              : GraphQLFloat,
        )
      case "Arrays":
        if (ast.elements.length || ast.rest.length !== 1)
          return invalid(`Unsupported tuple at ${coordinate}`)
        return wrap(new GraphQLList(this.type(ast.rest[0]!, coordinate)))
      default:
        return invalid(
          `Unsupported Schema ${ast._tag} at ${coordinate}; use Schema.Class or name input objects with GraphQL.input`,
        )
    }
  }
}

/** Report the schema change needed to accept an explicit clear, without echoing input data. */
export function optionalNullMessage(
  args: Readonly<Record<string, Schema.ConstraintDecoder<unknown, never>>>,
  raw: Readonly<Record<string, unknown>>,
): string | undefined {
  const visit = (
    ast: SchemaAST.AST,
    value: unknown,
    path: string,
  ): string | undefined => {
    if (value === null) {
      const nullable = Schema.is(Schema.toEncoded(Schema.make(ast)))(null)
      return SchemaAST.isOptional(ast) && !nullable
        ? `${path}: null is not accepted by Schema.optionalKey(S); omit the field or use Schema.optionalKey(Schema.NullOr(S)) to allow clearing it`
        : undefined
    }
    if (ast._tag === "Union") {
      for (const member of ast.types) {
        const message = visit(member, value, path)
        if (message) return message
      }
    }
    const object = structure(ast)
    if (object && typeof value === "object" && value !== null) {
      for (const field of object.propertySignatures) {
        const message = visit(
          field.type,
          Reflect.get(value, field.name),
          `${path}.${String(field.name)}`,
        )
        if (message) return message
      }
    }
    if (ast._tag === "Arrays" && ast.rest[0] && Array.isArray(value)) {
      for (const [index, item] of value.entries()) {
        const message = visit(ast.rest[0], item, `${path}[${index}]`)
        if (message) return message
      }
    }
    return undefined
  }
  for (const [key, schema] of Object.entries(args)) {
    const message = visit(schema.ast, raw[key], key)
    if (message) return message
  }
  return undefined
}

/** DateTime parses to ISO strings; native Schema.Date inputs still decode to Dates. */
export function argumentDecoder(
  args: Readonly<Record<string, Schema.ConstraintDecoder<unknown, never>>>,
) {
  const schema = Schema.Struct(args)
  const decode = Schema.decodeUnknownSync(schema)
  // Walk the wire shape: a Schema.Class itself has an encoding to its object fields.
  const encoded = SchemaAST.toEncoded(schema.ast)
  const prepare = (ast: SchemaAST.AST, value: unknown): unknown => {
    if (value == null) return value
    if (
      ast._tag === "Declaration" &&
      representation(ast) === "effect/schema/Date" &&
      typeof value === "string"
    )
      return new Date(value)
    if (ast._tag === "Union") {
      const member = ast.types.find(
        (type) => type._tag !== "Null" && type._tag !== "Undefined",
      )
      return member ? prepare(member, value) : value
    }
    const object = structure(ast)
    if (object && typeof value === "object") {
      const fields = new Map(
        object.propertySignatures.map((field) => [field.name, field.type]),
      )
      return Object.fromEntries(
        Object.entries(value).map(([key, item]) => {
          const field = fields.get(key)
          return [key, field ? prepare(field, item) : item]
        }),
      )
    }
    if (ast._tag === "Arrays" && ast.rest[0] && Array.isArray(value))
      return value.map((item) => prepare(ast.rest[0]!, item))
    return value
  }
  return (raw: Readonly<Record<string, unknown>>) =>
    decode(prepare(encoded, raw))
}
