/**
 * `union()` combines models into one schema that decodes to whichever model
 * matches, using the stored `_tag` when there is one.
 *
 * ```ts
 * const Shape = union([Circle, Square])
 * type Shape = typeof Shape.Type // Circle | Square
 *
 * Shape.from({ _tag: "Circle", radius: 1 }) // Circle
 * ```
 *
 * @since 7.0.0
 */
import { type Effect, Schema } from "effect"
import {
  defineMembers,
  type NoMembers,
  noProvider,
  type Provider,
  type ProviderInput,
  type AsProvider,
  toProvider,
  type UnionMembers,
} from "./Provider.ts"

/**
 * Any model class (or other Schema class) that can be a union member.
 *
 * @since 7.0.0
 */
export interface AnyModel extends Schema.Codec<any, any> {
  readonly identifier: string
  new (...args: ReadonlyArray<any>): any
}

/**
 * Members every union has, in addition to the `Schema.Union` API.
 *
 * @since 7.0.0
 */
export interface UnionStatics<Members extends ReadonlyArray<AnyModel>> {
  /**
   * Decodes untrusted input into one of the members.
   *
   * @throws {Schema.SchemaError} when no member matches
   */
  from(input: unknown): Members[number]["Type"]
  /** Decodes untrusted input into one of the members, failing with a `SchemaError`. */
  decode(
    input: unknown,
  ): Effect.Effect<Members[number]["Type"], Schema.SchemaError>
}

/**
 * The schema `union()` returns.
 *
 * @since 7.0.0
 */
export type Union<
  Members extends ReadonlyArray<AnyModel>,
  P extends Provider = Provider<NoMembers>,
> = Schema.Union<Members> &
  UnionStatics<Members> &
  UnionMembers<P, Members[number]["Type"]>

/**
 * Builds a union of models, optionally with the union members of providers.
 *
 * ```ts
 * const Content = union([Post, Comment], dynamodb)
 * const items = yield* Content.query({ KeyConditionExpression: "PK = :pk", ... })
 * ```
 *
 * @since 7.0.0
 */
export function union<const Members extends ReadonlyArray<AnyModel>>(
  members: Members,
): Union<Members>
export function union<
  const Members extends ReadonlyArray<AnyModel>,
  const P extends ProviderInput,
>(members: Members, provider: P): Union<Members, AsProvider<P>>
export function union(
  members: ReadonlyArray<AnyModel>,
  provider: ProviderInput = noProvider,
): unknown {
  const schema = Schema.Union(members)
  defineMembers(schema, {
    from: (input: unknown) => Schema.decodeUnknownSync(schema)(input),
    decode: (input: unknown) => Schema.decodeUnknownEffect(schema)(input),
  })
  return defineMembers(schema, toProvider(provider).unionProps)
}
