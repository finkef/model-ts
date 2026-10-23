import { Effect, Schema } from "effect"
import { GraphQLError } from "graphql"
import { recoverCause, defaultErrorPolicy } from "./Errors.ts"
import type { ExecutionResult } from "./Execution.ts"

const Result = Schema.Struct({
  data: Schema.optionalKey(Schema.NullOr(Schema.JsonObject)),
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
        extensions: Schema.optionalKey(Schema.JsonObject),
      }),
    ),
  ),
  extensions: Schema.optionalKey(Schema.JsonObject),
})
export type FormattedExecutionResult = typeof Result.Type
const decode = Schema.decodeUnknownSync(Schema.fromJsonString(Result))

/** Match the JSON wire response: plain records, no Error stacks or graphql-js prototypes. */
export const snapshot = (result: ExecutionResult): FormattedExecutionResult =>
  decode(JSON.stringify(result))

/** Formatting belongs to the executor; transports receive only JSON values.
 * Serialization defects always become a fixed internal error, including failures
 * in a custom toJSON method, so even the fallback is guaranteed to serialize.
 */
export const formatResult = (
  result: ExecutionResult,
): Effect.Effect<FormattedExecutionResult> =>
  Effect.sync(() => snapshot(result)).pipe(
    Effect.catchCause((cause) =>
      Effect.map(
        recoverCause(cause, [], defaultErrorPolicy, {
          coordinate: "operation",
          phase: "execute",
        }),
        (error) => {
          if (!(error instanceof GraphQLError))
            throw new Error("Error policy must return a GraphQL error")
          return snapshot({ errors: [error] })
        },
      ),
    ),
  )
