import type { Schema } from "effect"
import type { GraphQLScalarType } from "graphql"
import { GraphQLEnumType } from "graphql"
import { type AnyDefinition } from "./Assembly.ts"
import {
  type FieldMap,
  type SourceField,
  type CheckedFields,
  type SourceIssue,
  type SourceSlot,
} from "./FieldTypes.ts"
import { type Description, type Contextual } from "./FieldTypes.ts"
import {
  type Tag,
  type NamesOfKind,
  type RegistrationCheck,
  type CheckedMembers,
} from "./Registry.ts"
import { type DefinitionKind } from "./Registry.ts"
import { input } from "./SchemaTypes.ts"
import { invalid, metadata } from "./SchemaTypes.ts"

export interface ObjectDefinition<
  N extends string,
  A,
  F,
  O extends ObjectOptions = {},
  C = {},
> extends Contextual<C> {
  readonly "~kind": "object"
  readonly "~name": N
  readonly "~model": Schema.Schema<A>
  readonly "~description"?: string
  readonly "~implements"?: O["implements"]
  readonly fields: F
}

export interface NamedDefinition<
  K extends DefinitionKind,
  N extends string,
  A,
> {
  readonly "~kind": K
  readonly "~name": N
  readonly "~model": Schema.Schema<A>
}

export interface ScalarDefinition<
  K extends "scalar" | "enum",
  N extends string,
  S extends Schema.Top,
> extends NamedDefinition<K, N, S["Type"]> {
  readonly "~scalar"?: GraphQLScalarType
  readonly schema: S["Rebuild"]
}

export interface InterfaceDefinition<N extends string, A, F, C = {}>
  extends NamedDefinition<"interface", N, A>, Contextual<C> {
  readonly "~description"?: string
  readonly "~resolveType"?: (value: A) => string | null
  readonly fields: F
}

export interface ObjectOptions extends Description {
  readonly implements?: readonly string[]
}

export interface AbstractOptions<
  A,
  Members extends readonly string[],
> extends Description {
  readonly members: Members
  readonly resolveType?: (value: A) => NoInfer<Members[number]> | null
}

export interface UnionDefinition<
  N extends string,
  A,
  Members extends readonly string[],
> extends NamedDefinition<"union", N, A> {
  readonly options: AbstractOptions<A, Members>
}

export interface RootDefinition<
  K extends "query" | "mutation" | "subscription",
  F,
  C = {},
> extends Contextual<C> {
  readonly "~kind": K
  readonly fields: F
}

/** Only the literal tag is read for implicit naming; output fields never inspect model ASTs. */
export function tagName(model: {
  readonly fields?: { readonly _tag?: Schema.Constraint }
}): string {
  const tag = model.fields?._tag?.ast
  return tag?._tag === "Literal" && typeof tag.literal === "string"
    ? tag.literal
    : invalid("Object model needs a literal _tag; use an explicit name")
}

export interface InterfaceOptions<Types, A> extends Description {
  readonly resolveType?: (value: A) => NamesOfKind<Types, "object"> | null
}

export type EnumValue<A> = A | ({ readonly value: A } & Description)
export interface EnumOptions<A> extends Description {
  readonly values: Readonly<Record<string, EnumValue<A>>>
}

type MappedValues<V> = V[keyof V] extends infer A
  ? A extends { readonly value: infer Value }
    ? Value
    : A
  : never

type CheckedEnumValues<A, V> = {
  readonly [K in keyof V]: [MappedValues<Pick<V, K>>] extends [A]
    ? unknown
    : "Use a literal from the registered enum model"
}

export type CompleteEnum<A, V> = [Exclude<A, MappedValues<V>>] extends [never]
  ? unknown
  : {
      readonly error: `Missing enum values: ${Exclude<A, MappedValues<V>> & string}; map every model literal in values`
    }

export interface EnumDefinition<
  N extends string,
  S extends Schema.Top,
> extends ScalarDefinition<"enum", N, S> {
  readonly "~enum"?: EnumOptions<S["Type"]>
}

/** Explicit member names are checked against the corresponding model, in order. */
type NamedMembers<
  Types,
  M extends readonly Schema.Top[],
  Names extends readonly string[],
> = Names["length"] extends M["length"]
  ? {
      readonly [I in keyof Names]: I extends keyof M
        ? Names[I] & RegistrationCheck<Types, Names[I], M[I]["Type"], "object">
        : never
    }
  : {
      readonly error: "Provide one registered object name per union model, in the same order"
    }

type Implemented<O> = O extends {
  readonly implements: readonly (infer I extends string)[]
}
  ? I
  : never

/** Interface fields are known when the barrel is assembled, not at model registration. */
export type CheckedInterfaces<Types, M> = {
  readonly [K in keyof M]: M[K] extends ObjectDefinition<
    infer N,
    infer A,
    infer F,
    infer O,
    infer _C
  >
    ? {
        readonly [I in Implemented<O>]: Extract<
          M[keyof M],
          { readonly "~kind": "interface"; readonly "~name": I }
        > extends { readonly fields: infer IF }
          ? {
              readonly [P in Exclude<keyof IF, keyof F>]: [
                SourceIssue<Types, A, P, IF[P]>,
              ] extends [never]
                ? IF[P] extends SourceSlot<A>
                  ? never
                  : P
                : P
            }[Exclude<keyof IF, keyof F>] extends infer Bad
            ? [Bad] extends [never]
              ? never
              : `${N}.${Bad & string}: cannot inherit ${I} field; supply a compatible source property or override with a resolver`
            : never
          : never
      }[Implemented<O>] extends infer Issue
      ? [Issue] extends [never]
        ? unknown
        : { readonly error: Issue }
      : never
    : unknown
}

export function mappedEnumeration(
  name: string,
  schema: Schema.Schema<string>,
  options: EnumOptions<string>,
): GraphQLEnumType {
  const ast = schema.ast
  const literals = (ast._tag === "Union" ? ast.types : [ast]).map((value) =>
    value._tag === "Literal" && typeof value.literal === "string"
      ? value.literal
      : invalid(`${name}: enums require string Schema.Literals`),
  )
  const values = Object.entries(options.values).map(([key, entry]) => {
    const config = typeof entry === "string" ? { value: entry } : entry
    if (!literals.includes(config.value))
      invalid(`${name}.${key}: value must be a model literal`)
    return [key, config] as const
  })
  if (
    literals.some(
      (literal) => !values.some(([, config]) => config.value === literal),
    )
  )
    invalid(`${name}: map every model literal in values`)
  // Aliases are ambiguous on serialization: require one external name per internal value.
  if (new Set(values.map(([, config]) => config.value)).size !== values.length)
    invalid(
      `${name}: enum values must be unique; use one name per model literal`,
    )
  return new GraphQLEnumType({
    name,
    ...metadata(ast),
    ...options,
    values: Object.fromEntries(values),
  })
}

export interface DefinitionBuilder<Types, C> {
  object<
    M extends Schema.Schema<{ readonly _tag: string }> & {
      readonly fields: { readonly _tag: Schema.Constraint }
    },
    const F extends FieldMap<M["Type"], C>,
  >(
    model: M & RegistrationCheck<Types, Tag<M>, NoInfer<M["Type"]>, "object">,
    fields: F & CheckedFields<Types, Tag<M>, M["Type"], NoInfer<F>>,
  ): ObjectDefinition<Tag<M>, M["Type"], F, {}, C>
  object<
    M extends Schema.Schema<{ readonly _tag: string }> & {
      readonly fields: { readonly _tag: Schema.Constraint }
    },
    const F extends FieldMap<M["Type"], C>,
    const O extends ObjectOptions & {
      readonly implements?: readonly NamesOfKind<Types, "interface">[]
    },
  >(
    model: M & RegistrationCheck<Types, Tag<M>, NoInfer<M["Type"]>, "object">,
    options: O,
    fields: F & CheckedFields<Types, Tag<M>, M["Type"], NoInfer<F>>,
  ): ObjectDefinition<Tag<M>, M["Type"], F, O, C>
  object<
    const N extends string,
    A,
    const F extends FieldMap<A, C>,
    const O extends ObjectOptions & {
      readonly implements?: readonly NamesOfKind<Types, "interface">[]
    } = {},
  >(
    name: N & RegistrationCheck<Types, N, NoInfer<A>, "object">,
    model: Schema.Schema<A>,
    options: O,
    fields: F & CheckedFields<Types, N, A, NoInfer<F>>,
  ): ObjectDefinition<N, A, F, O, C>
  interface<const N extends string, A, const F extends FieldMap<A, C>>(
    name: N & RegistrationCheck<Types, N, NoInfer<A>, "interface">,
    model: Schema.Schema<A>,
    fields: F & CheckedFields<Types, N, A, NoInfer<F>>,
  ): InterfaceDefinition<N, A, F, C>
  interface<const N extends string, A, const F extends FieldMap<A, C>>(
    name: N & RegistrationCheck<Types, N, NoInfer<A>, "interface">,
    model: Schema.Schema<A>,
    options: InterfaceOptions<Types, NoInfer<A>>,
    fields: F & CheckedFields<Types, N, A, NoInfer<F>>,
  ): InterfaceDefinition<N, A, F, C>
  union<const N extends string, S extends Schema.Union<readonly Schema.Top[]>>(
    name: N & RegistrationCheck<Types, N, NoInfer<S["Type"]>, "union">,
    model: S & CheckedMembers<Types, NoInfer<S["members"]>>,
    options?: Description & {
      readonly resolveType?: (
        value: S["Type"],
      ) => NoInfer<Tag<S["members"][number]>> | null
    },
  ): UnionDefinition<N, S["Type"], readonly Tag<S["members"][number]>[]>
  union<
    const N extends string,
    S extends Schema.Union<readonly Schema.Top[]>,
    const Members extends readonly string[],
  >(
    name: N & RegistrationCheck<Types, N, NoInfer<S["Type"]>, "union">,
    model: S,
    options: AbstractOptions<NoInfer<S["Type"]>, Members> & {
      readonly members: Members &
        NamedMembers<Types, NoInfer<S["members"]>, Members>
    },
  ): UnionDefinition<N, S["Type"], Members>
  scalar<
    const N extends string,
    S extends Schema.Top & Schema.Codec<unknown, unknown>,
  >(
    name: N & RegistrationCheck<Types, N, NoInfer<S["Type"]>, "scalar">,
    codec: S,
    scalarType?: GraphQLScalarType,
  ): ScalarDefinition<"scalar", N, S>
  enum<const N extends string, S extends Schema.Literals<readonly string[]>>(
    name: N & RegistrationCheck<Types, N, NoInfer<S["Type"]>, "enum">,
    schema: S,
  ): EnumDefinition<N, S>
  enum<
    const N extends string,
    S extends Schema.Literals<readonly string[]>,
    const V extends Readonly<Record<string, EnumValue<string>>>,
  >(
    name: N & RegistrationCheck<Types, N, NoInfer<S["Type"]>, "enum">,
    schema: S,
    options: Description & {
      readonly values: V &
        CompleteEnum<NoInfer<S["Type"]>, V> &
        CheckedEnumValues<NoInfer<S["Type"]>, V>
    },
  ): EnumDefinition<N, S>
  queryField<
    const N extends string,
    const F extends SourceField<{}, unknown, C>,
  >(
    name: N,
    field: F &
      CheckedFields<Types, "Query", {}, { readonly [K in N]: NoInfer<F> }>[N],
  ): RootDefinition<"query", { readonly [K in N]: F }, C>
  mutationField<
    const N extends string,
    const F extends SourceField<{}, unknown, C>,
  >(
    name: N,
    field: F &
      CheckedFields<
        Types,
        "Mutation",
        {},
        { readonly [K in N]: NoInfer<F> }
      >[N],
  ): RootDefinition<"mutation", { readonly [K in N]: F }, C>
  subscriptionField<
    const N extends string,
    const F extends SourceField<{}, unknown, C, true>,
  >(
    name: N,
    field: F &
      CheckedFields<
        Types,
        "Subscription",
        {},
        { readonly [K in N]: NoInfer<F> }
      >[N],
  ): RootDefinition<"subscription", { readonly [K in N]: F }, C>
}

export function makeDefinitions<Types, C>(): DefinitionBuilder<Types, C> {
  const object = (...args: readonly unknown[]) => {
    const explicit = typeof args[0] === "string"
    // Public overloads validate each positional argument before this boundary.
    const model = (explicit ? args[1] : args[0]) as Schema.Top & {
      readonly fields?: { readonly _tag?: Schema.Constraint }
    }
    const name = explicit ? args[0] : tagName(model)
    const rest = args.slice(explicit ? 2 : 1)
    const options = (rest.length === 1 ? {} : rest[0]) as ObjectOptions
    return {
      "~kind": "object",
      "~name": name,
      "~model": model,
      "~implements": options.implements ?? [],
      ...(options.description === undefined
        ? {}
        : { "~description": options.description }),
      fields: rest.at(-1),
    }
  }
  // The overloads enforce registration and field contracts before this erased
  // construction boundary; the runtime stores their validated metadata unchanged.
  return {
    object,
    interface: (
      name: string,
      model: Schema.Top,
      ...rest:
        | [AnyDefinition["fields"]]
        | [
            Description & {
              readonly resolveType?: (value: unknown) => string | null
            },
            AnyDefinition["fields"],
          ]
    ) => {
      const [options, fieldMap] =
        rest.length === 1 ? [undefined, rest[0]] : rest
      return {
        "~kind": "interface",
        "~name": name,
        "~model": model,
        "~description": options?.description,
        "~resolveType": options?.resolveType,
        fields: fieldMap,
      }
    },
    union: (
      name: string,
      model: Schema.Union<readonly Schema.Top[]>,
      options?: Description & {
        readonly members?: readonly string[]
        readonly resolveType?: (value: unknown) => string | null
      },
    ) => ({
      "~kind": "union",
      "~name": name,
      "~model": model,
      options: {
        ...options,
        members:
          options?.members ??
          model.members.map((member) =>
            // The implicit-members overload requires tagged models; explicit members bypass this branch.
            tagName(
              member as Schema.Top & {
                readonly fields?: { readonly _tag?: Schema.Constraint }
              },
            ),
          ),
      },
    }),
    scalar: (name, model, scalarType) => {
      if (scalarType && scalarType.name !== name)
        invalid(
          `Scalar name ${scalarType.name} does not match ${name}; register the scalar under its existing name`,
        )
      return {
        "~kind": "scalar",
        "~name": name,
        "~model": model,
        schema: input(name, model),
        "~scalar": scalarType,
      }
    },
    enum: (
      name: string,
      model: Schema.Literals<readonly string[]>,
      options?: EnumOptions<string>,
    ) => ({
      "~enum": options,
      "~kind": "enum",
      "~name": name,
      "~model": model,
      schema: input(name, model),
    }),
    queryField: (name, field) => ({
      "~kind": "query",
      fields: { [name]: field },
    }),
    mutationField: (name, field) => ({
      "~kind": "mutation",
      fields: { [name]: field },
    }),
    subscriptionField: (name, field) => ({
      "~kind": "subscription",
      fields: { [name]: field },
    }),
  } as DefinitionBuilder<Types, C>
}
