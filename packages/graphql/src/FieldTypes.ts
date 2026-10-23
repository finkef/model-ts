import type { Effect, Option, Schema, Stream } from "effect"
import { type GraphQLFieldConfig, type GraphQLOutputType } from "graphql"
import { type Connection, type ConnectionArgs } from "./Connections.ts"
import type { InputTypes } from "./InputTypes.ts"
import {
  type Scalars,
  type NamesOfKind,
  type ModelAt,
  type Values,
} from "./Registry.ts"
import {
  type ErrorSchema,
  type ErrorsOf,
  type ResultOptions,
} from "./Results.ts"

export type Resolved<A, E, R> = A | Effect.Effect<A, E, R>

export type RuntimeRef =
  | string
  | { readonly kind: "list" | "nullable"; readonly of: RuntimeRef }

export declare const outputValue: unique symbol

export interface OutputRef<A> {
  readonly kind: "list" | "nullable"
  readonly of: RuntimeRef
  readonly [outputValue]: A
}

export type ConnectionNode<Types> = NamesOfKind<
  Types,
  "object" | "interface" | "union"
>

export type ObjectName<Types> = NamesOfKind<Types, "object">

export type ResultName<Types> =
  | ObjectName<Types>
  | NamesOfKind<Types, "union">
  | `${ObjectName<Types>}Edge`
  | `${ObjectName<Types>}Connection`
  | "PageInfo"

export type Ref<Types> =
  | Extract<keyof Values<Types>, string>
  | keyof Scalars
  | OutputRef<unknown>

export type Value<Types, T> =
  T extends OutputRef<infer A>
    ? A
    : T extends keyof Values<Types>
      ? Values<Types>[T]
      : T extends keyof Scalars
        ? Scalars[T]
        : never

export type ListOf<Types, T> = ReadonlyArray<Value<Types, T>>

export type NullableOf<Types, T> =
  | Value<Types, T>
  | null
  | undefined
  | Option.Option<Value<Types, T>>

export type ArgFields = Readonly<
  Record<string, Schema.ConstraintDecoder<unknown, never>>
>

export type ArgsOf<Args extends ArgFields> = Schema.Struct<Args>["Type"]

export interface Description {
  readonly description?: string
  readonly deprecationReason?: string
}

/** Resolvers, authorize, totalCount and nodes all receive the request context as the third argument. */

export type Resolve<S, Args, A, E, R, C> = (
  source: S,
  args: Args,
  context: C,
) => Resolved<A, E, R>

export interface FieldConfig<
  S,
  Args extends ArgFields,
  A,
  E,
  R,
  AR,
  C,
> extends Description {
  readonly args?: Args
  readonly resolve: Resolve<S, ArgsOf<NoInfer<Args>>, A, E, R, C>
  readonly authorize?: Resolve<S, ArgsOf<NoInfer<Args>>, boolean, E, AR, C>
}

export interface UnionFieldConfig<
  S,
  Args extends ArgFields,
  A,
  E extends readonly ErrorSchema[],
  R,
  G,
  AR,
  C,
> extends FieldConfig<S, Args, A, ErrorsOf<NoInfer<E>> | G, R, AR, C> {
  /** Typed errors returned as members of the generated "<Field>Result" union. */
  readonly errors?: E
  readonly result?: ResultOptions
}
/** A resolver, or a config with args, errors or a description. */

export type FieldInput<S, A, R, G, Args extends ArgFields, AR, C> =
  | Resolve<S, {}, A, NoInfer<G>, R, C>
  | FieldConfig<S, Args, A, NoInfer<G>, R, AR, C>

export type UnionRule<Types, T, E> = E extends readonly []
  ? unknown
  : T extends ResultName<Types>
    ? unknown
    : {
        readonly errors: "Result unions need an object type or object union; use a registered object or union"
      }

export type ResultRule<Types, T> =
  T extends ResultName<Types>
    ? unknown
    : {
        readonly result?: "Result unions need an object type or object union; use a registered object or union"
      }

export type ConnectionArgCheck<Args> = {
  readonly [K in keyof Args]: K extends keyof ConnectionArgs
    ? "Reserved pagination argument; remove it from args and use the built-in connection arguments"
    : unknown
}

export interface PagedConnection<
  S,
  A,
  Args extends ArgFields,
  R,
  G,
  AR,
  CR,
  C,
> extends Description {
  readonly authorize?: Resolve<S, ArgsOf<NoInfer<Args>>, boolean, G, AR, C>
  readonly totalCount?: Resolve<S, ArgsOf<NoInfer<Args>>, number, G, CR, C>
  /** Overrides the default "<Node>Connection" name. */
  readonly name?: string
  readonly edgeName?: string
  readonly args?: Args & ConnectionArgCheck<Args>
  /** Returns one page, e.g. a model-ts paginate() result. */
  readonly resolve: Resolve<
    S,
    ConnectionArgs & ArgsOf<NoInfer<Args>>,
    Connection<A>,
    G,
    R,
    C
  >
  readonly nodes?: never
}

export interface NodesConnection<
  S,
  A,
  Args extends ArgFields,
  R,
  G,
  AR,
  CR,
  C,
> extends Description {
  readonly authorize?: Resolve<S, ArgsOf<NoInfer<Args>>, boolean, G, AR, C>
  readonly totalCount?: Resolve<S, ArgsOf<NoInfer<Args>>, number, G, CR, C>
  readonly name?: string
  readonly edgeName?: string
  readonly args?: Args & ConnectionArgCheck<Args>
  /** Returns every node; the builder slices the page and issues offset cursors. */
  readonly nodes: Resolve<S, ArgsOf<NoInfer<Args>>, ReadonlyArray<A>, G, R, C>
  readonly resolve?: never
}

export interface BuildContext {
  readonly input: InputTypes
  readonly output: (ref: RuntimeRef, coordinate: string) => GraphQLOutputType
  readonly result: (
    ref: string,
    errors: readonly ErrorSchema[],
    field: string,
    options?: ResultOptions,
    nullable?: boolean,
  ) => GraphQLOutputType
  readonly connection: (
    node: string,
    name: string | undefined,
    coordinate: string,
    counted: boolean,
    edgeName?: string,
  ) => GraphQLOutputType
}

export interface RequestContext {
  /** The caller's request context, passed to every resolver. */
  readonly context: unknown
  readonly stream: (
    stream: Stream.Stream<unknown, never, unknown>,
  ) => AsyncIterable<unknown>
  readonly run: (
    effect: Effect.Effect<unknown, never, unknown>,
  ) => Promise<unknown>
}

export const fieldRequirements: unique symbol = Symbol(
  "GraphQL field requirements",
)

export interface Field<out R, C = {}> extends Contextual<C> {
  readonly kind: "field" | "list" | "nullable"
  readonly build: (
    context: BuildContext,
    name: string,
    coordinate?: string,
  ) => GraphQLFieldConfig<unknown, RequestContext>
  readonly [fieldRequirements]: R | undefined
}

export declare const fieldSource: unique symbol

export interface SourceSlot<S> {
  readonly [fieldSource]: (source: S) => void
}

export type SourceField<S, R, C = {}> = SourceSlot<S> & NoInfer<Field<R, C>>

export type FieldMap<S = never, C = {}> = Readonly<
  Record<string, SourceField<S, unknown, C>>
>

export declare const defaultRef: unique symbol
/** Phantom declaration used by both local and root default-source checks. */

export interface DefaultField<R, T, C = {}> extends Field<R, C> {
  readonly [defaultRef]: T
}

export type SourceDefault<S, R, T, C = {}> = SourceSlot<S> &
  NoInfer<DefaultField<R, T, C>>

export interface DefaultOptions<S, G, AR, C> extends Description {
  readonly authorize?: Resolve<S, {}, boolean, G, AR, C>
  readonly resolve?: never
  readonly args?: never
  readonly errors?: never
}

export type ScalarHelpers<Types, G, C, Nullable extends boolean = false> = {
  readonly [K in keyof ScalarMethods]: <S, AR = never>(
    options?: DefaultOptions<S, G, AR, C>,
  ) => SourceDefault<
    S,
    AR,
    Nullable extends true
      ? OutputRef<
          | Value<Types, ScalarMethods[K]>
          | null
          | undefined
          | Option.Option<Value<Types, ScalarMethods[K]>>
        >
      : ScalarMethods[K],
    C
  >
}

export interface ScalarMethods {
  id: "ID"
  string: "String"
  int: "Int"
  float: "Float"
  boolean: "Boolean"
  dateTime: "DateTime"
  json: "JSON"
}

/** Keep resolver overloads last so invalid results report their expected value type. */
export interface NullableFields<Types, G, C> extends ScalarHelpers<
  Types,
  G,
  C,
  true
> {
  <const T extends Ref<Types>>(
    of: T,
  ): OutputRef<NullableOf<Types, T>> &
    SourceDefault<unknown, never, OutputRef<NullableOf<Types, T>>, C>
  <S, const T extends Ref<Types>, AR = never>(
    of: T,
    options: DefaultOptions<S, G, AR, C>,
  ): SourceDefault<S, AR, OutputRef<NullableOf<Types, T>>, C>
  <
    S,
    const T extends Ref<Types>,
    R = never,
    const E extends readonly ErrorSchema[] = readonly [],
    const Args extends ArgFields = {},
    AR = never,
  >(
    of: T,
    input:
      | Resolve<S, {}, NullableOf<Types, NoInfer<T>>, NoInfer<G>, R, C>
      | (UnionFieldConfig<
          S,
          Args,
          NullableOf<Types, NoInfer<T>>,
          E,
          R,
          NoInfer<G>,
          AR,
          C
        > &
          UnionRule<Types, T, E> &
          ResultRule<Types, T>),
  ): SourceField<S, R | AR, C>
}

export type SourceIssue<Types, S, K, F> = F extends {
  readonly [defaultRef]: infer T
}
  ? K extends keyof S
    ? [S[K]] extends [Value<Types, T>]
      ? never
      : { readonly source: S[K]; readonly declared: Value<Types, T> }
    : { readonly key: K }
  : never

export type CheckedFields<Types, N extends string, S, F> = {
  readonly [K in keyof F]: [SourceIssue<Types, S, K, F[K]>] extends [never]
    ? unknown
    : `${N}.${K & string}: ${DefaultMessage<S, K, F[K]>}`
}

export interface Fields<Types, G, C = {}> extends ScalarHelpers<Types, G, C> {
  events<
    S,
    const T extends Ref<Types>,
    R = never,
    const Args extends ArgFields = {},
    AR = never,
  >(
    type: T,
    config: Description & {
      readonly authorize?: Resolve<
        Value<Types, NoInfer<T>>,
        ArgsOf<NoInfer<Args>>,
        boolean,
        NoInfer<G>,
        AR,
        C
      >
      readonly args?: Args
      readonly subscribe: (
        source: S,
        args: ArgsOf<NoInfer<Args>>,
        context: C,
      ) => Stream.Stream<Value<Types, NoInfer<T>>, NoInfer<G>, R>
    },
  ): SourceField<S, R | AR, C>
  // Keep shorthand first: the broader overload widens literal returns before TS7 checks them.
  field<S, const T extends NamesOfKind<Types, "enum">, const A extends string>(
    type: T &
      ([NoInfer<A>] extends [ModelAt<Types, NoInfer<T>>]
        ? unknown
        : "Return a registered enum model literal"),
    resolve: (source: S, args: {}, context: C) => A,
  ): SourceField<S, never, C>
  field<S, const T extends Ref<Types>, AR = never>(
    type: T,
    options?: DefaultOptions<S, G, AR, C>,
  ): SourceDefault<S, AR, T, C>
  field<
    S,
    const T extends Ref<Types>,
    R = never,
    const E extends readonly ErrorSchema[] = readonly [],
    const Args extends ArgFields = {},
    AR = never,
  >(
    type: T,
    input:
      | Resolve<S, {}, Value<Types, NoInfer<T>>, NoInfer<G>, R, C>
      | (UnionFieldConfig<
          S,
          Args,
          Value<Types, NoInfer<T>>,
          E,
          R,
          NoInfer<G>,
          AR,
          C
        > &
          UnionRule<Types, T, E> &
          ResultRule<Types, T>),
  ): SourceField<S, R | AR, C>
  list<const T extends Ref<Types>>(
    of: T,
  ): OutputRef<ListOf<Types, T>> &
    SourceDefault<unknown, never, OutputRef<ListOf<Types, T>>, C>
  list<S, const T extends Ref<Types>, AR = never>(
    of: T,
    options: DefaultOptions<S, G, AR, C>,
  ): SourceDefault<S, AR, OutputRef<ListOf<Types, T>>, C>
  list<
    S,
    const T extends Ref<Types>,
    R = never,
    const Args extends ArgFields = {},
    AR = never,
  >(
    of: T,
    input: FieldInput<S, ListOf<Types, NoInfer<T>>, R, G, Args, AR, C>,
  ): SourceField<S, R | AR, C>
  readonly nullable: NullableFields<Types, G, C>
  connection<S, const N extends ConnectionNode<Types>, AR = never>(
    node: N,
    options?: DefaultOptions<S, G, AR, C>,
  ): SourceDefault<S, AR, `${N}Connection`, C>
  connection<
    S,
    const N extends ConnectionNode<Types>,
    R = never,
    const Args extends ArgFields = {},
    AR = never,
    CR = never,
  >(
    node: N,
    input:
      | Resolve<
          S,
          ConnectionArgs,
          Connection<ModelAt<Types, NoInfer<N>>>,
          NoInfer<G>,
          R,
          C
        >
      | PagedConnection<
          S,
          ModelAt<Types, NoInfer<N>>,
          Args,
          R,
          NoInfer<G>,
          AR,
          CR,
          C
        >
      | NodesConnection<
          S,
          ModelAt<Types, NoInfer<N>>,
          Args,
          R,
          NoInfer<G>,
          AR,
          CR,
          C
        >,
  ): SourceField<S, R | AR | CR, C>
}

export type SourceBase<A> = [A] extends [never]
  ? "never"
  : [A] extends [string]
    ? "string"
    : [A] extends [number]
      ? "number"
      : [A] extends [boolean]
        ? "boolean"
        : [A] extends [Date]
          ? "Date"
          : [A] extends [Option.Option<unknown>]
            ? "Option"
            : [A] extends [ReadonlyArray<unknown>]
              ? "ReadonlyArray"
              : "object"

export type SourceDescription<A> =
  `${SourceBase<NonNullable<A>>}${null extends A ? " | null" : ""}${undefined extends A ? " | undefined" : ""}`

export type DefaultMessage<S, K, F> = K extends keyof S
  ? F extends { readonly [defaultRef]: infer T }
    ? `model has ${SourceDescription<S[K]>}, field declares ${T extends string ? T : T extends OutputRef<infer A> ? SourceDescription<A> : "declared GraphQL value"}`
    : never
  : "missing source property; supply a resolver"

export type RequirementsOfFields<F> = [keyof F] extends [never]
  ? never
  : F[keyof F] extends {
        readonly [fieldRequirements]: infer R
      }
    ? Exclude<R, undefined>
    : never

export type Requirements<M> = M[keyof M] extends infer D
  ? D extends { readonly "~kind": string; readonly fields: infer F }
    ? RequirementsOfFields<F>
    : never
  : never

export interface Contextual<C> {
  readonly "~context"?: (context: C) => C
}

export type RuntimeField = Pick<Field<unknown>, "kind" | "build">
