import { Effect } from "effect"
import type { GraphQLFormattedError } from "graphql"
import { ProtocolClose } from "../Errors.ts"
import type { Result } from "../Executor.ts"
import { Gateway } from "../Gateway.ts"
import type { OperationId } from "../Identifiers.ts"
import type { Connection, JsonObject } from "../Models.ts"

export type ServerMessage =
  | {
      readonly type: "connection_ack" | "ping" | "pong"
      readonly payload?: JsonObject | null
    }
  | {
      readonly type: "next"
      readonly id: OperationId
      readonly payload:
        | Result
        | { readonly errors: ReadonlyArray<GraphQLFormattedError> }
    }
  | {
      readonly type: "error"
      readonly id: OperationId
      readonly payload: ReadonlyArray<GraphQLFormattedError>
    }
  | { readonly type: "complete"; readonly id: OperationId }

/** API Gateway cannot carry application close codes; retain intent on the span. */
export const close = Effect.fn("GraphQL.close")(function* (
  connection: Pick<Connection, "id" | "endpoint">,
  reason: ProtocolClose["reason"],
) {
  const code = new ProtocolClose({ reason }).code
  yield* Effect.annotateCurrentSpan({
    "graphql.close.code": code,
    "graphql.close.reason": reason,
  })
  yield* Effect.logWarning("GraphQL WebSocket closed", { code, reason })
  const gateway = yield* Gateway
  yield* gateway
    .close(connection.endpoint, connection.id)
    .pipe(
      Effect.catchTag("GatewayError", (error) =>
        error.gone ? Effect.void : Effect.fail(error),
      ),
    )
})
