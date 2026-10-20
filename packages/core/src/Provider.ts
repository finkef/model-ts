/**
 * Providers add members to models: instance members (methods, getters,
 * values), class members (statics) and union members. A provider is a plain
 * value, so it can be shared, merged and tested on its own.
 *
 * A provider describes its members as a type lambda: an interface that reads
 * `this["Self"]` (the model instance type) and `this["Fields"]` (the model's
 * Schema fields). `model()` fills both in for every class, so a provider
 * method can return the concrete model type. `this` is only available at
 * the top level of the interface, so pass it on to a generic interface:
 *
 * ```ts
 * interface TimestampsInstance<Self> {
 *   touch(this: Self): Self
 * }
 * interface TimestampsMembers extends ProviderMembers {
 *   readonly instance: TimestampsInstance<this["Self"]>
 * }
 * ```
 *
 * @since 7.0.0
 */

/**
 * The type lambda every provider extends. Leave `Self` and `Fields` as they
 * are and override `instance`, `static` and `union` as needed.
 *
 * @since 7.0.0
 */
export interface ProviderMembers {
  /** Filled in by `model()`: the model instance type. */
  readonly Self: unknown
  /**
   * Filled in by `model()`: the model's Schema fields. Typed `unknown`, like
   * Effect's own type lambdas, so that the real fields are not widened by the
   * index signature of `Schema.Struct.Fields`.
   */
  readonly Fields: unknown
  /** Members added to every instance. */
  readonly instance: unknown
  /** Members added to the model class. */
  readonly static: unknown
  /** Members added to unions built with `union(members, provider)`. */
  readonly union: unknown
}

/**
 * A provider value. Create one with {@link makeProvider} and combine several
 * with {@link mergeProviders}, or pass an array to `model()`.
 *
 * @since 7.0.0
 */
export interface Provider<M extends ProviderMembers = ProviderMembers> {
  /** Phantom field that carries the member types; never set at runtime. */
  readonly "~members"?: M
  readonly instanceProps: object
  readonly classProps: object
  readonly unionProps: object
}

type Apply<P, Self, F> =
  P extends Provider<infer M>
    ? M & { readonly Self: Self; readonly Fields: F }
    : never

/** The instance members a provider adds to a model with the given `Self` and fields. */
export type InstanceMembers<P, Self, F> = Apply<P, Self, F>["instance"]
/** The class members a provider adds to a model with the given `Self` and fields. */
export type StaticMembers<P, Self, F> = Apply<P, Self, F>["static"]
/** The members a provider adds to a union of models whose instances are `Self`. */
export type UnionMembers<P, Self> = Apply<P, Self, never>["union"]

/**
 * The implementation is checked against the declared members with `Self`
 * and `Fields` left open, so a provider cannot miss a member or change a
 * signature.
 */
type Implementation<M extends ProviderMembers> = M & {
  readonly Self: any
  readonly Fields: any
}

/**
 * `{ [K]: T }`, with the key optional only when `T` has no required
 * members, so an implementation cannot leave out a declared method.
 */
type Group<K extends string, T> = {} extends T
  ? { readonly [P in K]?: T }
  : { readonly [P in K]: T }

/** The implementation `makeProvider` expects for members `M`. */
type ProviderImplementation<M extends ProviderMembers> = Group<
  "instanceProps",
  Implementation<M>["instance"]
> &
  Group<"classProps", Implementation<M>["static"]> &
  Group<"unionProps", Implementation<M>["union"]>

/**
 * Builds a provider from its declared members and their implementation.
 * Getters and other property descriptors are kept as written.
 *
 * ```ts
 * interface GreeterMembers extends ProviderMembers {
 *   readonly instance: { greet(this: { readonly name: string }): string }
 * }
 *
 * const greeter = makeProvider<GreeterMembers>()({
 *   instanceProps: {
 *     greet() {
 *       return `Hello, ${this.name}`
 *     },
 *   },
 * })
 * ```
 *
 * @since 7.0.0
 */
export const makeProvider =
  <M extends ProviderMembers>() =>
  (implementation: ProviderImplementation<M>): Provider<M> => ({
    instanceProps: implementation.instanceProps ?? {},
    classProps: implementation.classProps ?? {},
    unionProps: implementation.unionProps ?? {},
  })

/** Left fold in which a later member replaces an earlier one of the same name. */
type Override<
  Ts extends ReadonlyArray<unknown>,
  Acc = {},
> = Ts extends readonly [infer Head, ...infer Rest]
  ? Override<Rest, Omit<Acc, keyof Head> & Head>
  : Acc

/**
 * The members of several providers merged left to right.
 *
 * @since 7.0.0
 */
export interface MergedMembers<
  Ps extends ReadonlyArray<Provider>,
> extends ProviderMembers {
  readonly instance: Override<{
    [K in keyof Ps]: InstanceMembers<Ps[K], this["Self"], this["Fields"]>
  }>
  readonly static: Override<{
    [K in keyof Ps]: StaticMembers<Ps[K], this["Self"], this["Fields"]>
  }>
  readonly union: Override<{
    [K in keyof Ps]: UnionMembers<Ps[K], this["Self"]>
  }>
}

/**
 * Merges providers left to right. When two providers define a member with
 * the same name, the later one wins, both in the types and at runtime.
 *
 * ```ts
 * const store = mergeProviders([dynamodb, softDeleteFlag])
 * ```
 *
 * @since 7.0.0
 */
export const mergeProviders = <const Ps extends ReadonlyArray<Provider>>(
  providers: Ps,
): Provider<MergedMembers<Ps>> => ({
  instanceProps: mergeDescriptors(providers.map((p) => p.instanceProps)),
  classProps: mergeDescriptors(providers.map((p) => p.classProps)),
  unionProps: mergeDescriptors(providers.map((p) => p.unionProps)),
})

/**
 * The members of a model without a provider.
 *
 * @since 7.0.0
 */
export interface NoMembers extends ProviderMembers {
  readonly instance: {}
  readonly static: {}
  readonly union: {}
}

/** @internal */
export const noProvider: Provider<NoMembers> = {
  instanceProps: {},
  classProps: {},
  unionProps: {},
}

/** One provider, or several that are merged left to right. */
export type ProviderInput = Provider | ReadonlyArray<Provider>

/** The provider that a {@link ProviderInput} stands for. */
export type AsProvider<P extends ProviderInput> =
  P extends ReadonlyArray<Provider> ? Provider<MergedMembers<P>> : P

/** @internal */
export const toProvider = (input: ProviderInput): Provider =>
  isProviderList(input) ? mergeProviders(input) : input

const isProviderList = (
  input: ProviderInput,
): input is ReadonlyArray<Provider> => Array.isArray(input)

const mergeDescriptors = (sources: ReadonlyArray<object>): object => {
  const target = {}
  for (const source of sources) {
    Object.defineProperties(target, Object.getOwnPropertyDescriptors(source))
  }
  return target
}

/** @internal Copies provider members onto a target and its prototype. */
export const defineMembers = (target: object, members: object) =>
  Object.defineProperties(target, Object.getOwnPropertyDescriptors(members))
