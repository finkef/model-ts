import { Effect, Schema, Stream } from "effect"
import {
  execute,
  subscribe,
  type DocumentNode,
  type ExecutionResult,
  type GraphQLSchema,
} from "graphql"
import {
  ExecutionError,
  requestError,
  formatError as defaultFormatError,
  type ErrorFormatter,
} from "./Errors.ts"
import { inputSafeSchema } from "./internal/schema.ts"
import { JsonObject, type OperationRequest } from "./Models.ts"

const ResultSchema = Schema.Struct({
  data: Schema.optionalKey(Schema.NullOr(JsonObject)),
  errors: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        message: Schema.String,
        locations: Schema.optionalKey(
          Schema.Array(
            Schema.Struct({ line: Schema.Number, column: Schema.Number }),
          ),
        ),
        path: Schema.optionalKey(
          Schema.Array(Schema.Union([Schema.String, Schema.Number])),
        ),
        extensions: Schema.optionalKey(JsonObject),
      }),
    ),
  ),
  extensions: Schema.optionalKey(JsonObject),
})

/** FormattedExecutionResult-shaped JSON. Executors own error policy; transport never reformats it. */
export type Result = typeof ResultSchema.Type
export interface Execution<C> {
  readonly document: DocumentNode
  readonly request: OperationRequest
  readonly context: C
}
export interface Executor<C, E = never, R = never> {
  /** Opt in when execute/subscribe validate and format their own schema diagnostics. */
  readonly validation?: "executor"
  readonly schema: GraphQLSchema
  readonly execute: (input: Execution<C>) => Effect.Effect<Result, E, R>
  readonly subscribe: (input: Execution<C>) => Stream.Stream<Result, E, R>
}

/** Plain graphql-js adapter; custom formatters belong here for executor-produced errors. */
export const graphqlExecutor = <C>(
  schema: GraphQLSchema,
  options: { readonly formatError?: ErrorFormatter } = {},
): Executor<C, ExecutionError> => {
  const safeSchema = inputSafeSchema(schema)
  const format = options.formatError ?? defaultFormatError
  const args = ({ document, request, context }: Execution<C>) => ({
    schema: safeSchema,
    document,
    contextValue: context,
    variableValues: request.variables,
    operationName: request.operationName,
  })
  const formatted = (
    result: ExecutionResult<Readonly<Record<string, unknown>>>,
  ): Result => {
    const value = {
      ...result,
      ...(result.errors
        ? {
            errors: result.errors.map((error) =>
              format(
                result.data === undefined && error.path === undefined
                  ? requestError(error, "BAD_USER_INPUT")
                  : error,
              ),
            ),
          }
        : {}),
    }
    // Normalize dates/undefined and reject cyclic, BigInt or malformed custom formatter
    // output before it crosses the executor seam. No GraphQLError instances escape.
    return Schema.decodeUnknownSync(ResultSchema)(
      JSON.parse(JSON.stringify(value)),
    )
  }
  return {
    schema: safeSchema,
    execute: (input) =>
      Effect.tryPromise({
        try: async () => formatted(await execute(args(input))),
        catch: (cause) => new ExecutionError({ cause }),
      }).pipe(Effect.withSpan("GraphQL.execute")),
    subscribe: (input) =>
      Stream.unwrap(
        Effect.tryPromise({
          try: async () => subscribe(args(input)),
          catch: (cause) => new ExecutionError({ cause }),
        }).pipe(
          Effect.map((result) =>
            (Symbol.asyncIterator in result
              ? Stream.fromAsyncIterable(
                  result,
                  (cause) => new ExecutionError({ cause }),
                )
              : Stream.succeed(result)
            ).pipe(
              Stream.mapEffect((result) =>
                Effect.try({
                  try: () => formatted(result),
                  catch: (cause) => new ExecutionError({ cause }),
                }),
              ),
            ),
          ),
        ),
      ).pipe(Stream.withSpan("GraphQL.subscribe")),
  }
}
