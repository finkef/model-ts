import { Effect, Schema, SchemaAST } from "effect"
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

/** The description of one argument or input field. */
export interface AnnotateOptions {
  readonly description: string
}

/**
 * Describes the argument or input field that uses `schema`, without changing
 * its type. Effect's `schema.annotate({ description })` describes the type
 * instead, which for a named input type is its `input` definition.
 *
 * ```ts
 * args: {
 *   input: GraphQL.annotate(FavoriteMoveInput, {
 *     description: "The favorite to move and where to.",
 *   }),
 * }
 * ```
 *
 * It sets key annotations, as `schema.annotateKey(options)` does. There is no
 * `deprecationReason`: GraphQL can only deprecate optional arguments and input
 * fields, so `optional` and `optional.nullable` take one.
 */
export function annotate<S extends Schema.Top>(
  schema: S,
  options: AnnotateOptions,
): S["Rebuild"] {
  return schema.annotateKey({ description: options.description })
}

/** Documentation of an optional argument or input field. */
export interface OptionalOptions {
  readonly description?: string | undefined
  readonly deprecationReason?: string | undefined
}

/** An optional argument or input field with a value used when it is omitted. */
export interface DefaultOptions<T> extends OptionalOptions {
  /** A decoded value, validated and encoded with the schema for the GraphQL schema. */
  readonly default: T
}

/**
 * An argument or input field that may be omitted but not `null`. With a
 * `default`, an omitted value decodes to it and the decoded type is required.
 * An explicit `null` fails with "<field> cannot be null.", default or not; use
 * `optional.nullable` when `null` is a value, for example to clear a field.
 *
 * ```ts
 * args: {
 *   name: GraphQL.optional(Schema.String, { default: "world" }),
 *   label: GraphQL.optional(Schema.String, { description: "Shown in lists." }),
 * }
 * ```
 */
function optionalInput<S extends Schema.Top>(
  schema: S,
  options: DefaultOptions<NoInfer<S["Type"]>>,
): Schema.withDecodingDefaultTypeKey<S>
function optionalInput<S extends Schema.Top>(
  schema: S,
  options?: OptionalOptions,
): Schema.optionalKey<S>
function optionalInput(
  schema: Schema.Top,
  options?: OptionalOptions & { readonly default?: unknown },
): Schema.Top {
  return withKey(schema, options)
}

/**
 * An argument or input field that may be omitted or `null`, which is how a
 * nullable GraphQL argument behaves. Decoding keeps the two apart, so an
 * update can leave an omitted field unchanged and write `null` for one that
 * is sent as `null`. A `default` only replaces an omitted value.
 *
 * ```ts
 * args: {
 *   note: GraphQL.optional.nullable(Schema.String, { description: "Null clears it." }),
 * }
 * ```
 */
function nullableInput<S extends Schema.Top>(
  schema: S,
  options: DefaultOptions<NoInfer<S["Type"]> | null>,
): Schema.withDecodingDefaultTypeKey<Schema.NullOr<S>>
function nullableInput<S extends Schema.Top>(
  schema: S,
  options?: OptionalOptions,
): Schema.optionalKey<Schema.NullOr<S>>
function nullableInput(
  schema: Schema.Top,
  options?: OptionalOptions & { readonly default?: unknown },
): Schema.Top {
  return withKey(Schema.NullOr(schema), options)
}

function withKey(
  schema: Schema.Top,
  options: OptionalOptions & { readonly default?: unknown } = {},
): Schema.Top {
  const { description, deprecationReason } = options
  const key = {
    ...(description === undefined ? {} : { description }),
    ...(deprecationReason === undefined ? {} : { deprecationReason }),
  }
  if (options.default === undefined)
    return Schema.optionalKey(schema).annotateKey(key)
  // A default that does not decode would only fail once the value is omitted.
  const value = Schema.decodeUnknownSync(Schema.toType(schema))(options.default)
  return schema
    .pipe(Schema.withDecodingDefaultTypeKey(Effect.succeed(value)))
    .annotateKey({ ...key, graphqlDefault: value })
}

/** Optional arguments and input fields; see `optionalInput` and `nullableInput`. */
export const optional = Object.assign(optionalInput, {
  nullable: nullableInput,
})

/**
 * Whether GraphQL may omit this argument or input field. A schema with a
 * decoding default is required once decoded but optional on the wire.
 */
export function isOptionalInput(ast: SchemaAST.AST): boolean {
  return SchemaAST.isOptional(optionalSource(ast) ?? ast)
}

/** The `optionalKey(S)` a decoding default decodes from, if any. */
export function optionalSource(ast: SchemaAST.AST): SchemaAST.AST | undefined {
  if (SchemaAST.isOptional(ast)) return undefined
  const source = ast.encoding?.at(-1)?.to
  return source && SchemaAST.isOptional(source) ? source : undefined
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
