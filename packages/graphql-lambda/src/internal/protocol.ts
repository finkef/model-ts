import { Effect } from "effect"
import type { GraphQLFormattedError } from "graphql"
import { badRequest, MessageTooLarge, ProtocolClose } from "../Errors.ts"
import type { Result } from "../Executor.ts"
import { Gateway } from "../Gateway.ts"
import type { OperationId } from "../Identifiers.ts"
import type { Connection, JsonObject } from "../Models.ts"
import { Storage } from "../Storage.ts"

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

export const send = Effect.fn("GraphQL.send")(function* (
  connection: Connection,
  frame: ServerMessage,
) {
  const storage = yield* Storage
  const gateway = yield* Gateway
  const data = yield* Effect.try({
    try: () => JSON.stringify(frame),
    catch: () => badRequest("Cannot serialize result"),
  })
  // Limit the encoded message, not individual WebSocket frames.
  const bytes = Buffer.byteLength(data)
  if (bytes > 32 * 1024) {
    const error = new MessageTooLarge({ bytes, limit: 32 * 1024 })
    yield* Effect.logError("GraphQL WebSocket send rejected", error)
    return yield* error
  }
  return yield* gateway.post(connection.endpoint, connection.id, data).pipe(
    Effect.as(true),
    Effect.catchTag("GatewayError", (error) =>
      Effect.gen(function* () {
        if (!error.gone) return yield* error
        yield* storage.disconnect(connection.id)
        return false
      }),
    ),
  )
})
