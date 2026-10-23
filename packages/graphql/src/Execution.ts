import { formatResult, type FormattedExecutionResult } from "./Formatting.ts"
import { ScalarInputFailure } from "./ScalarBoundary.ts"
export type { FormattedExecutionResult } from "./Formatting.ts"
import type { Context } from "effect"
import { Cause, Effect, Stream } from "effect"
import {
  GraphQLError,
  type GraphQLSchema,
  type DocumentNode,
  execute as graphqlExecute,
  subscribe as graphqlSubscribe,
  parse,
  validate,
  specifiedRules,
  SingleFieldSubscriptionsRule,
  type ExecutionResult as GraphQLExecutionResult,
} from "graphql"
import {
  type ErrorPolicy,
  defaultErrorPolicy,
  isBoundaryError,
  recoverCause,
} from "./Errors.ts"
import { type RequestContext, type Contextual } from "./FieldTypes.ts"
import { singleRootFieldSubscriptionsRule } from "./internal/SubscriptionRule.ts"

// Replace the native rule: v15 counts selections, while v16 evaluates root
// directives without variables. Both execution paths must use the same semantics.
const validationRules = specifiedRules.map((rule) =>
  rule === SingleFieldSubscriptionsRule
    ? singleRootFieldSubscriptionsRule
    : rule,
)

export const schemaRequirements: unique symbol = Symbol(
  "GraphQL schema requirements",
)
export const schemaErrorPolicy: unique symbol = Symbol(
  "GraphQL schema error policy",
)

export interface BuiltSchema<out R, C = {}> extends Contextual<C> {
  readonly schema: GraphQLSchema
  readonly [schemaRequirements]: R | undefined
  readonly [schemaErrorPolicy]?: ErrorPolicy
}

export type ExecutionResult = GraphQLExecutionResult<
  { readonly [key: string]: unknown },
  { readonly [key: string]: unknown }
>

export interface Request {
  readonly source: string
  readonly variableValues?: { readonly [name: string]: unknown }
  readonly operationName?: string
}

export type Json =
  | null
  | boolean
  | number
  | string
  | readonly Json[]
  | { readonly [key: string]: Json }

/** Structural transport contract: no dependency on a particular HTTP/WS adapter. */
export interface Execution<C> {
  readonly document: DocumentNode
  readonly request: {
    readonly query: string
    readonly variables?: Record<string, Json> | null
    readonly operationName?: string | null
    readonly extensions?: Record<string, Json> | null
  }
  readonly context: C
}

export interface Executor<C, E = never, R = never> {
  /** Standard schema validation runs here so scalar failures use the executor error policy. */
  readonly validation: "executor"
  readonly schema: GraphQLSchema
  readonly execute: (
    input: Execution<C>,
  ) => Effect.Effect<FormattedExecutionResult, E, R>
  readonly subscribe: (
    input: Execution<C>,
  ) => Stream.Stream<FormattedExecutionResult, E, R>
}

/** Required when the builder declares a context type; optional for builders without one. */
export type ContextArgument<C> = {} extends C ? [context?: C] : [context: C]

function bridge<R>(
  services: Context.Context<R>,
  context: unknown,
  signal?: AbortSignal,
  pending?: Set<Promise<unknown>>,
  sources?: Set<AsyncIterator<unknown, unknown>>,
): RequestContext {
  // The builder collects heterogeneous resolver requirements in BuiltSchema.R.
  // Restore that union only here, when crossing into graphql-js's Promise API.
  return {
    context,
    stream: (stream) => {
      // GraphQL consumes one iterator per subscription. Own it before handing
      // it off so closing the lifetime can join its actual source scope cleanup.
      const source = Stream.toAsyncIterableWith(
        stream as Stream.Stream<unknown, never, R>,
        services,
      )[Symbol.asyncIterator]()
      sources?.add(source)
      return { [Symbol.asyncIterator]: () => source }
    },
    run: (effect) => {
      const promise = Effect.runPromiseWith(services)(
        effect as Effect.Effect<unknown, never, R>,
        { signal },
      )
      pending?.add(promise)
      return promise.finally(() => pending?.delete(promise))
    },
  }
}

/** graphql-js can also fail in scalar serialization or abstract resolution. */
const sanitize = (
  result: ExecutionResult,
  policy: ErrorPolicy,
  requestStage = false,
): Effect.Effect<ExecutionResult> =>
  Effect.gen(function* () {
    if (!result.errors) return result
    const errors = yield* Effect.forEach(result.errors, (error) =>
      Effect.gen(function* () {
        let original: unknown = error
        let scalarFailure = false
        while (true) {
          if (original instanceof ScalarInputFailure) {
            scalarFailure = true
            original = original.original
          } else if (
            original instanceof GraphQLError &&
            !isBoundaryError(original) &&
            original.originalError
          ) {
            original = original.originalError
          } else break
        }
        // Only request-stage diagnostics without application callback provenance
        // are graphql-js's own messages. Execution errors never gain trust from
        // a missing originalError (prelocated errors are forwarded unchanged).
        if (
          (requestStage || !error.path) &&
          !scalarFailure &&
          original instanceof GraphQLError &&
          !isBoundaryError(original)
        )
          return error
        const mapped = yield* recoverCause(Cause.die(original), [], policy, {
          coordinate: error.path?.join(".") ?? "operation",
          phase: "execute",
        })
        if (!(mapped instanceof GraphQLError))
          throw new Error("Error policy must return a GraphQL error")
        return new GraphQLError(mapped.message, {
          nodes: error.nodes,
          source: error.source,
          positions: error.positions,
          path: error.path,
          originalError: mapped,
          extensions: mapped.extensions,
        })
      }),
    )
    return { ...result, errors }
  })

const unexpected = (
  cause: Cause.Cause<unknown>,
  policy: ErrorPolicy,
): Effect.Effect<ExecutionResult> =>
  Effect.map(
    recoverCause(cause, [], policy, {
      coordinate: "operation",
      phase: "execute",
    }),
    (error) => {
      if (!(error instanceof GraphQLError))
        throw new Error("Error policy must return a GraphQL error")
      return { errors: [error] }
    },
  )

function executeDocument<R, C>(
  built: BuiltSchema<R, C>,
  document: DocumentNode,
  request: Request,
  context: unknown,
): Effect.Effect<ExecutionResult, never, R> {
  const policy = built[schemaErrorPolicy] ?? defaultErrorPolicy
  return Effect.gen(function* () {
    const errors = validate(built.schema, document, validationRules)
    if (errors.length) return yield* sanitize({ errors }, policy, true)
    const services = yield* Effect.context<R>()
    const result = yield* Effect.promise((signal) =>
      Promise.resolve(
        graphqlExecute({
          schema: built.schema,
          document,
          variableValues: request.variableValues,
          operationName: request.operationName,
          contextValue: bridge(services, context, signal),
        }),
      ),
    )
    return yield* sanitize(result, policy)
  }).pipe(
    Effect.catchCause((cause) => unexpected(cause, policy)),
    Effect.withSpan("GraphQL.execute"),
  )
}

function subscribeDocument<R, C>(
  built: BuiltSchema<R, C>,
  document: DocumentNode,
  request: Request,
  context: unknown,
): Stream.Stream<ExecutionResult, never, R> {
  const policy = built[schemaErrorPolicy] ?? defaultErrorPolicy
  return Stream.unwrap(
    Effect.gen(function* () {
      const errors = validate(built.schema, document, validationRules)
      if (errors.length)
        return Stream.fromEffect(sanitize({ errors }, policy, true))
      const lifetime = yield* Effect.acquireRelease(
        Effect.sync(() => ({
          controller: new AbortController(),
          pending: new Set<Promise<unknown>>(),
          sources: new Set<AsyncIterator<unknown, unknown>>(),
        })),
        ({ controller, pending, sources }) =>
          Effect.promise(async () => {
            controller.abort()
            // fromAsyncIterable does not await return() during a pending pull.
            // Closing the owned Effect iterators joins their scope finalizers,
            // including any close already started by graphql-js or the consumer.
            const closing = Array.from(sources, (source) => source.return?.())
            await Promise.allSettled([...pending, ...closing])
          }),
      )
      const services = yield* Effect.context<R>()
      const result: ExecutionResult | AsyncIterable<ExecutionResult> =
        yield* Effect.promise(() =>
          graphqlSubscribe({
            schema: built.schema,
            document,
            variableValues: request.variableValues,
            operationName: request.operationName,
            contextValue: bridge(
              services,
              context,
              lifetime.controller.signal,
              lifetime.pending,
              lifetime.sources,
            ),
          }),
        )
      return (
        Symbol.asyncIterator in result
          ? Stream.fromAsyncIterable(result, (error) => error)
          : Stream.succeed(result)
      ).pipe(Stream.mapEffect((result) => sanitize(result, policy)))
    }),
  ).pipe(
    Stream.catchCause((cause) => Stream.fromEffect(unexpected(cause, policy))),
    Stream.withSpan("GraphQL.subscribe"),
  )
}

function parsed(source: string): DocumentNode | ExecutionResult {
  try {
    return parse(source)
  } catch (error) {
    if (!(error instanceof GraphQLError)) throw error
    return { errors: [error] }
  }
}

export const execute = <R, C>(
  built: BuiltSchema<R, C>,
  request: Request,
  ...context: ContextArgument<NoInfer<C>>
): Effect.Effect<ExecutionResult, never, R> =>
  Effect.suspend(() => {
    const document = parsed(request.source)
    return "kind" in document
      ? executeDocument(
          built,
          document,
          request,
          context.length === 0 ? {} : context[0],
        )
      : Effect.succeed(document)
  })

export const subscribe = <R, C>(
  built: BuiltSchema<R, C>,
  request: Request,
  ...context: ContextArgument<NoInfer<C>>
): Stream.Stream<ExecutionResult, never, R> =>
  Stream.suspend(() => {
    const document = parsed(request.source)
    return "kind" in document
      ? subscribeDocument(
          built,
          document,
          request,
          context.length === 0 ? {} : context[0],
        )
      : Stream.succeed(document)
  })

export const executor = <R, C>(
  built: BuiltSchema<R, C>,
): Executor<C, never, R> => {
  const request = (input: Execution<C>): Request => ({
    source: input.request.query,
    ...(input.request.variables
      ? { variableValues: input.request.variables }
      : {}),
    ...(input.request.operationName != null
      ? { operationName: input.request.operationName }
      : {}),
  })
  return {
    validation: "executor",
    schema: built.schema,
    execute: (input) =>
      executeDocument(
        built,
        input.document,
        request(input),
        input.context,
      ).pipe(Effect.flatMap((result) => formatResult(result))),
    subscribe: (input) =>
      subscribeDocument(
        built,
        input.document,
        request(input),
        input.context,
      ).pipe(Stream.mapEffect((result) => formatResult(result))),
  }
}
