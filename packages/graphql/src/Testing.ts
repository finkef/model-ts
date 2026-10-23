/** Test operations keep their Effect requirements so @effect/vitest can supply any layer. */
import { Effect, Stream } from "effect"
import { GraphQLError, stripIgnoredCharacters } from "graphql"
import {
  type BuiltSchema,
  type ContextArgument,
  type Request,
  execute as run,
  subscribe as events,
} from "./Execution.ts"
import {
  snapshot,
  type FormattedExecutionResult as Result,
} from "./Formatting.ts"
export { snapshot } from "./Formatting.ts"
export type { FormattedExecutionResult as Result } from "./Formatting.ts"

export const execute = <R, C>(
  built: BuiltSchema<R, C>,
  request: Request,
  ...context: ContextArgument<NoInfer<C>>
): Effect.Effect<Result, never, R> =>
  run(built, request, ...context).pipe(Effect.map(snapshot))

export const subscribe = <R, C>(
  built: BuiltSchema<R, C>,
  request: Request,
  ...context: ContextArgument<NoInfer<C>>
): Stream.Stream<Result, never, R> =>
  events(built, request, ...context).pipe(Stream.map(snapshot))

/**
 * Tags a GraphQL document for editor highlighting and removes ignored
 * characters, so snapshots and error locations do not depend on indentation.
 * String literals are kept verbatim. Like an editor, it reads backslash
 * escapes as GraphQL escapes.
 *
 * @example
 * const QUERY = graphql`
 *   query List($id: ID!) { list(id: $id) { id name } }
 * `
 */
export function graphql(
  strings: TemplateStringsArray,
  ...values: ReadonlyArray<string | number | boolean>
): string {
  const source = String.raw({ raw: strings.raw }, ...values)
  try {
    return stripIgnoredCharacters(source)
  } catch (error) {
    // Leave invalid documents to execution, which reports a located syntax error.
    if (error instanceof GraphQLError) return source
    throw error
  }
}

export interface RequestOptions {
  readonly variables?: { readonly [name: string]: unknown }
  readonly operationName?: string
}

/** Request options are optional unless the context factory requires one. */
export type RequestArgument<O> = {} extends O
  ? [options?: O & RequestOptions]
  : [options: O & RequestOptions]

export interface ClientOptions<C, O, R> {
  /** Builds each request's context from its options, as a value or an Effect. */
  readonly context: (options: O) => C | Effect.Effect<C, never, R>
}

/** A context factory is required whenever the schema has a request context. */
export type ClientArgument<C, O, R> = {} extends C
  ? [options?: ClientOptions<C, O, R>]
  : [options: ClientOptions<C, O, R>]

export interface Client<O, R> {
  readonly request: (
    source: string,
    ...options: RequestArgument<O>
  ) => Effect.Effect<Result, never, R>
  readonly subscribe: (
    source: string,
    ...options: RequestArgument<O>
  ) => Stream.Stream<Result, never, R>
}

// A context is never itself an Effect, so Effect.isEffect separates the two
// cases exactly; TypeScript cannot exclude an Effect from a generic A.
const lift = <A, R>(
  value: A | Effect.Effect<A, never, R>,
): Effect.Effect<A, never, R> =>
  Effect.isEffect(value)
    ? (value as Effect.Effect<A, never, R>)
    : Effect.succeed(value)

/**
 * Binds a schema to a default request context so each test only states what
 * differs, such as the identity.
 *
 * @example
 * export const { request, subscribe } = Testing.client(schema, {
 *   context: (options: { identity?: Identity }) => ({
 *     requestId: "test",
 *     identity: options.identity ?? anonymous,
 *   }),
 * })
 */
export const client = <R, C, O = {}, R2 = never>(
  built: BuiltSchema<R, C>,
  ...[options]: ClientArgument<NoInfer<C>, O, R2>
): Client<O, R | R2> => {
  // RequestArgument and ClientArgument make an argument optional only when
  // `{}` satisfies its type, and both ContextArgument branches accept [C].
  // TypeScript cannot narrow these conditionals for generic O and C.
  const prepare = (source: string, input?: O & RequestOptions) => {
    const request = input ?? ({} as O & RequestOptions)
    const context = options
      ? lift(options.context(request))
      : Effect.succeed({} as C)
    return Effect.map(context, (context) => ({
      request: {
        source,
        ...(request.variables ? { variableValues: request.variables } : {}),
        ...(request.operationName === undefined
          ? {}
          : { operationName: request.operationName }),
      },
      context: [context] as ContextArgument<C>,
    }))
  }
  return {
    request: (source, ...[input]) =>
      Effect.flatMap(prepare(source, input), ({ request, context }) =>
        execute(built, request, ...context),
      ),
    subscribe: (source, ...[input]) =>
      Stream.unwrap(
        Effect.map(prepare(source, input), ({ request, context }) =>
          subscribe(built, request, ...context),
        ),
      ),
  }
}
