import type { Schema } from "effect"
import {
  GraphQLBoolean,
  GraphQLFloat,
  GraphQLID,
  GraphQLInt,
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
  GraphQLScalarType,
  GraphQLEnumType,
  GraphQLInterfaceType,
  GraphQLSchema,
  GraphQLString,
  GraphQLUnionType,
  validateSchema,
  type GraphQLNamedType,
  type GraphQLFieldConfigMap,
} from "graphql"
import { makeConnectionTypes } from "./Connections.ts"
import {
  mappedEnumeration,
  type EnumOptions,
  type NamedDefinition,
  type AbstractOptions,
} from "./Definitions.ts"
import { describe } from "./FieldRuntime.ts"
import {
  type BuildContext,
  type RuntimeField,
  type RequestContext,
} from "./FieldTypes.ts"
import { InputTypes } from "./InputTypes.ts"
import { makeResultTypes } from "./Results.ts"
import { guardScalar } from "./ScalarBoundary.ts"
import { dateTime, scalar, jsonScalar, enumeration } from "./Scalars.ts"
import { invalid, unwrap } from "./SchemaTypes.ts"

export interface Collected {
  readonly name: string
  readonly description?: string
  readonly fields: Record<string, RuntimeField>
  readonly interfaces?: readonly string[]
  readonly kind?: "object" | "interface"
  readonly resolveType?: (value: unknown) => string | null
}

export interface AnyDefinition {
  readonly "~kind":
    | "object"
    | "interface"
    | "query"
    | "mutation"
    | "subscription"
  readonly "~name"?: string
  readonly "~model"?: Schema.Top
  readonly "~implements"?: readonly string[]
  readonly "~resolveType"?: (value: unknown) => string | null
  readonly "~description"?: string
  readonly fields: Readonly<Record<string, RuntimeField>>
}

export function isDefinition(value: unknown): value is AnyDefinition {
  return (
    typeof value === "object" &&
    value !== null &&
    "~kind" in value &&
    "fields" in value &&
    typeof value.fields === "object"
  )
}

export function isFieldMap(
  value: unknown,
): value is Readonly<Record<string, RuntimeField>> {
  return (
    typeof value === "object" &&
    value !== null &&
    Object.values(value).every(
      (field) =>
        typeof field === "object" &&
        field !== null &&
        "build" in field &&
        typeof field.build === "function",
    )
  )
}

export function assemble(
  module: Readonly<Record<string, unknown>>,
): GraphQLSchema {
  const objects = new Map<string, Collected>()
  const roots = new Map<string, Collected>([
    ["Query", { name: "Query", fields: {} }],
    ["Mutation", { name: "Mutation", fields: {} }],
    ["Subscription", { name: "Subscription", fields: {} }],
  ])
  for (const [key, value] of Object.entries(module)) {
    if (!isDefinition(value)) continue
    const fields = value.fields
    if (!isFieldMap(fields))
      return invalid(`${key}: fields must be an object of t.* fields`)
    if (value["~kind"] === "object" || value["~kind"] === "interface") {
      const name = value["~name"] ?? invalid(`${key} has no GraphQL name`)
      if (objects.has(name)) invalid(`Duplicate GraphQL object: ${name}`)
      const description = value["~description"]
      objects.set(name, {
        name,
        ...(value["~resolveType"]
          ? { resolveType: value["~resolveType"] }
          : {}),
        kind: value["~kind"],
        interfaces: value["~implements"] ?? [],
        fields: { ...fields },
        ...(description === undefined ? {} : { description }),
      })
      continue
    }
    const root = roots.get(
      value["~kind"] === "query"
        ? "Query"
        : value["~kind"] === "mutation"
          ? "Mutation"
          : "Subscription",
    )!
    for (const [field, config] of Object.entries(fields)) {
      if (Object.hasOwn(root.fields, field))
        invalid(`Duplicate field: ${root.name}.${field}`)
      root.fields[field] = config
    }
  }

  const named = new Map<string, GraphQLNamedType>([
    ["ID", GraphQLID],
    ["String", GraphQLString],
    ["Boolean", GraphQLBoolean],
    ["Int", GraphQLInt],
    ["Float", GraphQLFloat],
  ])
  const add = <T extends GraphQLNamedType>(type: T): T => {
    if (named.has(type.name)) invalid(`Duplicate GraphQL name: ${type.name}`)
    named.set(type.name, type)
    return type
  }
  const inputs = new InputTypes(named, add)
  for (const value of Object.values(module)) {
    if (typeof value !== "object" || value === null || !("~kind" in value))
      continue
    if (value["~kind"] === "scalar" || value["~kind"] === "enum") {
      // Named constructors store the schema and name together; assembly erases
      // their heterogeneous value types only after checking the definition kind.
      const def = value as NamedDefinition<
        "scalar" | "enum",
        string,
        string
      > & {
        readonly "~scalar"?: GraphQLScalarType
        readonly "~enum"?: EnumOptions<string>
      }
      const type = add(
        def["~kind"] === "scalar"
          ? guardScalar(
              def["~scalar"] ??
                scalar(
                  def["~name"],
                  def["~model"] as Schema.Codec<string, unknown>,
                ),
            )
          : def["~enum"]
            ? mappedEnumeration(def["~name"], def["~model"], def["~enum"])
            : enumeration(def["~name"], def["~model"]),
      )
      inputs.register(def["~model"].ast, type)
    }
  }
  const objectType = (name: string, coordinate: string): GraphQLObjectType => {
    const type = named.get(name) ?? derived(name)
    return type instanceof GraphQLObjectType
      ? type
      : invalid(`Missing object ${name} at ${coordinate}`)
  }
  const nodeType = (name: string, coordinate: string) => {
    const type = named.get(name)
    return type instanceof GraphQLObjectType ||
      type instanceof GraphQLInterfaceType ||
      type instanceof GraphQLUnionType
      ? type
      : invalid(
          `Missing connection node ${name} at ${coordinate}; register an object, interface or union`,
        )
  }
  const { derived, connectionType } = makeConnectionTypes(
    named,
    add,
    nodeType,
    (name) => {
      const type = named.get(name)
      return (
        type instanceof GraphQLObjectType ||
        type instanceof GraphQLInterfaceType ||
        type instanceof GraphQLUnionType
      )
    },
  )

  const context: BuildContext = {
    input: inputs,
    output(ref, coordinate) {
      if (typeof ref !== "string") {
        const inner = context.output(ref.of, coordinate)
        return ref.kind === "list"
          ? new GraphQLNonNull(new GraphQLList(inner))
          : unwrap(inner)
      }
      const scalar =
        named.get(ref) ??
        (ref === "DateTime"
          ? add(dateTime())
          : ref === "JSON"
            ? add(jsonScalar())
            : undefined)
      return new GraphQLNonNull(
        scalar instanceof GraphQLScalarType ||
          scalar instanceof GraphQLEnumType ||
          scalar instanceof GraphQLUnionType ||
          scalar instanceof GraphQLInterfaceType
          ? scalar
          : objectType(ref, coordinate),
      )
    },
    result: makeResultTypes(inputs, add, objectType, named),
    connection(node, name, coordinate, counted, edgeName) {
      nodeType(node, coordinate)
      return new GraphQLNonNull(
        connectionType(
          node,
          name ?? `${node}${counted ? "Counted" : ""}Connection`,
          counted,
          edgeName,
        ),
      )
    },
  }
  const fieldConfigs = ({ fields, name, interfaces = [] }: Collected) => {
    const inherited: GraphQLFieldConfigMap<unknown, RequestContext> = {}
    for (const name of interfaces) {
      const definition = named.get(name)
      if (!(definition instanceof GraphQLInterfaceType))
        return invalid(`Missing interface ${name}`)
      // Reuse built configs so inherited result unions retain their interface's type identity.
      Object.assign(inherited, definition.toConfig().fields)
    }
    return {
      ...inherited,
      ...Object.fromEntries(
        Object.entries(fields).map(([field, config]) => [
          field,
          config.build(context, field, `${name}.${field}`),
        ]),
      ),
    }
  }

  const build = (object: Collected) =>
    add(
      new GraphQLObjectType({
        name: object.name,
        ...describe(object),
        fields: () => fieldConfigs(object),
        interfaces: () =>
          (object.interfaces ?? []).map((name) => {
            const type = named.get(name)
            return type instanceof GraphQLInterfaceType
              ? type
              : invalid(`Missing interface ${name}`)
          }),
      }),
    )
  for (const object of objects.values()) {
    if (object.kind === "interface")
      add(
        new GraphQLInterfaceType({
          name: object.name,
          ...describe(object),
          fields: () => fieldConfigs(object),
          resolveType: (value) => {
            if (object.resolveType)
              return object.resolveType(value) ?? undefined
            if (typeof value !== "object" || value === null) return undefined
            const name: unknown =
              Reflect.get(value, "__typename") ?? Reflect.get(value, "_tag")
            return typeof name === "string" ? name : undefined
          },
        }),
      )
    else build(object)
  }
  for (const value of Object.values(module)) {
    if (
      typeof value !== "object" ||
      value === null ||
      !("~kind" in value) ||
      value["~kind"] !== "union"
    )
      continue
    // union() validates member registrations before storing this metadata.
    const def = value as NamedDefinition<"union", string, unknown> & {
      readonly options: AbstractOptions<unknown, readonly string[]>
    }
    add(
      new GraphQLUnionType({
        name: def["~name"],
        ...describe(def.options),
        types: () =>
          def.options.members.map((name) => objectType(name, def["~name"])),
        resolveType: (value) => {
          const name: unknown = def.options.resolveType
            ? def.options.resolveType(value)
            : (Reflect.get(Object(value), "__typename") ??
              Reflect.get(Object(value), "_tag"))
          return typeof name === "string" && def.options.members.includes(name)
            ? name
            : undefined
        },
      }),
    )
  }
  const query = build(roots.get("Query")!)
  const mutationRoot = roots.get("Mutation")!
  const mutation =
    Object.keys(mutationRoot.fields).length > 0
      ? build(mutationRoot)
      : undefined
  if (Object.keys(roots.get("Query")!.fields).length === 0)
    invalid("Missing Query fields")
  const subscriptionRoot = roots.get("Subscription")!
  const subscription = Object.keys(subscriptionRoot.fields).length
    ? build(subscriptionRoot)
    : undefined
  const schema = new GraphQLSchema({
    query,
    ...(subscription ? { subscription } : {}),
    ...(mutation ? { mutation } : {}),
    types: [...named.values()],
  })
  const problems = validateSchema(schema)
  if (problems.length)
    invalid(problems.map((error) => error.message).join("\n"))
  return schema
}
