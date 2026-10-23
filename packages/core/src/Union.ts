/**
 * `Model.Union` combines models into one schema that decodes to whichever model
 * matches, using the stored `_tag` when there is one.
 *
 * ```ts
 * const Shape = Model.Union([Circle, Square])
 * type Shape = typeof Shape.Type // Circle | Square
 *
 * Shape.from({ _tag: "Circle", radius: 1 }) // Circle
 * ```
 *
 * @since 7.0.0
 */
import { type Effect, Schema } from "effect"
import { defineMembers, noProvider, toProvider } from "./internal/provider.ts"
import type {
  AsProvider,
  Input,
  NoMembers,
  Provider,
  UnionMembers,
} from "./Provider.ts"

/**
 * Any model class (or other Schema class) that can be a union member: a
 * schema with an identifier whose decoding and encoding need no services.
 *
 * @since 7.0.0
 */
export interface Any extends Schema.Top {
  readonly identifier: string
  readonly DecodingServices: never
  readonly EncodingServices: never
}

/**
 * Members every union has, in addition to the `Schema.Union` API.
 *
 * @since 7.0.0
 */
export interface UnionStatics<Members extends ReadonlyArray<Any>> {
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
 * The schema `Model.Union` returns.
 *
 * @since 7.0.0
 */
export type Union<
  Members extends ReadonlyArray<Any>,
  P extends Provider = Provider<NoMembers>,
> = Schema.Union<Members> &
  UnionStatics<Members> &
  UnionMembers<P, Members[number]["Type"]>

/**
 * Builds a union of models, optionally with the union members of providers.
 *
 * ```ts
 * const Content = Model.Union([Post, Comment], DynamoDB.provider)
 * const items = yield* Content.query({ KeyConditionExpression: "PK = :pk", ... })
 * ```
 *
 * @since 7.0.0
 */
export function Union<const Members extends ReadonlyArray<Any>>(
  members: Members,
): Union<Members>
export function Union<
  const Members extends ReadonlyArray<Any>,
  const P extends Input,
>(members: Members, provider: P): Union<Members, AsProvider<P>>
export function Union(
  members: ReadonlyArray<Any>,
  provider: Input = noProvider,
): unknown {
  const schema = Schema.Union(members)
  defineMembers(schema, {
    from: (input: unknown) => Schema.decodeUnknownSync(schema)(input),
    decode: (input: unknown) => Schema.decodeUnknownEffect(schema)(input),
  })
  return defineMembers(schema, toProvider(provider).unionProps)
}
