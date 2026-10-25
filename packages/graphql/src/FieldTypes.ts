import type { Effect, Option, Schema, Stream } from "effect"
import { type GraphQLFieldConfig, type GraphQLOutputType } from "graphql"
import { type Connection, type ConnectionArgs } from "./Connections.ts"
import type { RootDefinition } from "./Definitions.ts"
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
import type { nullableArg } from "./SchemaTypes.ts"

export type Resolved<A, E, R> = A | Effect.Effect<A, E, R>

export type RuntimeRef =
  | string
  | { readonly kind: "list" | "nullable"; readonly of: RuntimeRef }

export declare const outputValue: unique symbol

/** Preserve a nullable ref's named success type for result-union validation. */
export declare const outputResult: unique symbol

export interface OutputRef<A, Result = unknown> {
  readonly [outputResult]: Result
  readonly kind: "list" | "nullable"
  readonly of: RuntimeRef
  readonly [outputValue]: A
}

/** A config-less connection helper retains the node for the connection overloads. */
export interface ConnectionRef<N extends string> {
  readonly kind: "connection"
  readonly node: N
}

export type RootKind = "query" | "mutation" | "subscription"

/** Root helpers add a name and return a definition; the field config stays shared. */
type FieldArguments<K, N extends string, T, I> = [
  ...name: K extends RootKind ? [name: N] : [],
  type: T,
  input: I,
]

type FieldSource<K, S> = K extends RootKind ? unknown : S

type FieldResult<
  K,
  N extends string,
  S,
  R,
  C,
  Subscription extends boolean = false,
> = K extends RootKind
  ? RootDefinition<
      K,
      { readonly [P in N]: SourceField<unknown, R, C, Subscription> },
      C
    >
  : SourceField<S, R, C, Subscription>

type ResultTarget<T> = T extends OutputRef<unknown, infer Result> ? Result : T

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
  | OutputRef<unknown, unknown>

export type Value<Types, T> =
  T extends OutputRef<infer A, unknown>
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
  readonly subscribe?: never
  readonly resolve: Resolve<S, ArgsOf<NoInfer<Args>>, A, E, R, C>
  readonly authorize?: Resolve<S, ArgsOf<NoInfer<Args>>, boolean, E, AR, C>
}

/** Subscription setup returns a stream; authorization receives each delivered value. */
export interface SubscriptionConfig<
  S,
  Args extends ArgFields,
  A,
  G,
  R,
  AR,
  C,
> extends Description {
  readonly args?: Args
  readonly subscribe: (
    source: S,
    args: ArgsOf<NoInfer<Args>>,
    context: C,
  ) => Stream.Stream<A, NoInfer<G>, R>
  readonly authorize?: Resolve<
    A,
    ArgsOf<NoInfer<Args>>,
    boolean,
    NoInfer<G>,
    AR,
    C
  >
  readonly resolve?: never
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
  : ResultTarget<T> extends ResultName<Types>
    ? unknown
    : {
        readonly errors: "Result unions need an object type or object union; use a registered object or union"
      }

export type ResultRule<Types, T> =
  ResultTarget<T> extends ResultName<Types>
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
  readonly subscribe?: never
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
  readonly subscribe?: never
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
  readonly kind: "field" | "list" | "nullable" | "connection"
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

export type SourceField<
  S,
  R,
  C = {},
  Subscription extends boolean = false,
> = SourceSlot<S> & NoInfer<Field<R, C> & SubscriptionMarker<Subscription>>

export type SubscriptionMarker<Subscription extends boolean> =
  Subscription extends true
    ? { readonly "~subscription": true }
    : { readonly "~subscription"?: false }

export type FieldMap<
  S = never,
  C = {},
  Subscription extends boolean = false,
> = Readonly<Record<string, SourceField<S, unknown, C, Subscription>>>

export declare const defaultRef: unique symbol
/** Phantom declaration used by both local and root default-source checks. */

export interface DefaultField<R, T, C = {}> extends Field<R, C> {
  readonly "~subscription"?: false
  readonly [defaultRef]: T
}

export type SourceDefault<S, R, T, C = {}> = SourceSlot<S> &
  NoInfer<DefaultField<R, T, C>>

export interface DefaultOptions<S, G, AR, C> extends Description {
  readonly subscribe?: never
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
  /**
   * A nullable argument or input field, omittable or `null`:
   * `Schema.optionalKey(Schema.NullOr(schema))` with optional `GraphQL.arg`
   * documentation.
   */
  readonly arg: typeof nullableArg
  <const T extends Ref<Types>>(
    of: T,
  ): OutputRef<NullableOf<Types, T>, T> &
    SourceDefault<unknown, never, OutputRef<NullableOf<Types, T>, T>, C>
  <S, const T extends Ref<Types>, AR = never>(
    of: T,
    options: DefaultOptions<S, G, AR, C>,
  ): SourceDefault<S, AR, OutputRef<NullableOf<Types, T>, T>, C>
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

export interface EnumFields<Types, C, K = undefined> {
  // Keep shorthand first: the broader overload widens literal returns before TS7 checks them.
  <
    const N extends string,
    S,
    const T extends NamesOfKind<Types, "enum">,
    const A extends string,
  >(
    ...args: FieldArguments<
      K,
      N,
      T &
        ([NoInfer<A>] extends [ModelAt<Types, NoInfer<T>>]
          ? unknown
          : "Return a registered enum model literal"),
      (source: FieldSource<K, S>, args: {}, context: C) => A
    >
  ): FieldResult<K, N, S, never, C>
}

export interface SubscriptionFields<Types, G, C, K = undefined> {
  <
    const N extends string,
    S,
    const T extends Ref<Types>,
    R = never,
    const Args extends ArgFields = {},
    AR = never,
  >(
    ...args: FieldArguments<
      K,
      N,
      T,
      SubscriptionConfig<
        FieldSource<K, S>,
        Args,
        Value<Types, NoInfer<T>>,
        G,
        R,
        AR,
        C
      >
    >
  ): FieldResult<K, N, S, R | AR, C, true>
}

type ResolverInput<
  Types,
  T,
  S,
  R,
  G,
  E extends readonly ErrorSchema[],
  Args extends ArgFields,
  AR,
  CR,
  C,
> =
  T extends ConnectionRef<infer N>
    ? ConnectionInput<S, ModelAt<Types, N>, R, G, Args, AR, CR, C>
    : T extends { readonly kind: "list" }
      ? FieldInput<S, Value<Types, T>, R, G, Args, AR, C>
      :
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
              ResultRule<Types, T>)

export interface ResolverFields<Types, G, C, K = undefined> {
  <
    const N extends string,
    S,
    const T extends
      | Ref<Types>
      | (K extends RootKind ? ConnectionRef<ConnectionNode<Types>> : never),
    R = never,
    const E extends readonly ErrorSchema[] = readonly [],
    const Args extends ArgFields = {},
    AR = never,
    CR = never,
  >(
    ...args: FieldArguments<
      K,
      N,
      T,
      ResolverInput<
        Types,
        NoInfer<T>,
        FieldSource<K, S>,
        R,
        G,
        E,
        Args,
        AR,
        CR,
        C
      >
    >
  ): FieldResult<K, N, S, R | AR | CR, C>
}

export type ObjectFields<Types, G, C> = EnumFields<Types, C> &
  DefaultFields<Types, G, C> &
  ResolverFields<Types, G, C>

export interface DefaultFields<Types, G, C> {
  <S, const T extends Ref<Types>, AR = never>(
    type: T,
    options?: DefaultOptions<S, G, AR, C>,
  ): SourceDefault<S, AR, T, C>
}

type ConnectionInput<S, A, R, G, Args extends ArgFields, AR, CR, C> =
  | Resolve<S, ConnectionArgs, Connection<A>, NoInfer<G>, R, C>
  | PagedConnection<S, A, Args, R, NoInfer<G>, AR, CR, C>
  | NodesConnection<S, A, Args, R, NoInfer<G>, AR, CR, C>

export interface ObjectConnections<Types, G, C> {
  <const N extends ConnectionNode<Types>>(
    node: N,
  ): ConnectionRef<N> & SourceDefault<unknown, never, `${N}Connection`, C>
  <S, const N extends ConnectionNode<Types>, AR = never>(
    node: N,
    options: DefaultOptions<S, G, AR, C> | undefined,
  ): SourceDefault<S, AR, `${N}Connection`, C>
  <
    S,
    const N extends ConnectionNode<Types>,
    R = never,
    const Args extends ArgFields = {},
    AR = never,
    CR = never,
  >(
    node: N,
    input: ConnectionInput<
      S,
      ModelAt<Types, NoInfer<N>>,
      R,
      G,
      Args,
      AR,
      CR,
      C
    >,
  ): SourceField<S, R | AR | CR, C>
}

/** Field helpers of a builder; `id` covers both the output field and ID arguments. */
export interface Fields<Types, G, C = {}> extends Omit<
  ScalarHelpers<Types, G, C>,
  "id"
> {
  /** A non-null `ID` output field read from the source or resolved by `authorize`. */
  id<S, AR = never>(
    options?: DefaultOptions<S, G, AR, C>,
  ): SourceDefault<S, AR, "ID", C>
  /** Exposes a string schema, such as a branded ID, as a GraphQL `ID` argument or input field. */
  id<S extends Schema.Top & Schema.Schema<string>>(schema: S): S["Rebuild"]
  readonly field: ObjectFields<Types, G, C>
  list<const T extends Ref<Types>>(
    of: T,
  ): OutputRef<ListOf<Types, T>> & { readonly kind: "list" } & SourceDefault<
      unknown,
      never,
      OutputRef<ListOf<Types, T>>,
      C
    >
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
  readonly connection: ObjectConnections<Types, G, C>
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
    ? `model has ${SourceDescription<S[K]>}, field declares ${T extends string ? T : T extends OutputRef<infer A, unknown> ? SourceDescription<A> : "declared GraphQL value"}`
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
