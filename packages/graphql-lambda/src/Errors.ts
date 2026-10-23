import { Schema } from "effect"
import { GraphQLError, type GraphQLFormattedError } from "graphql"
import { ConnectionId, EventId, OperationId } from "./Identifiers.ts"
import { InputError } from "./internal/InputError.ts"

/** Safe, deliberately public transport errors. Resolver errors are private by default. */
export class TransportError extends Schema.TaggedError<TransportError>()(
  "TransportError",
  {
    message: Schema.String,
    code: Schema.String,
    status: Schema.Number,
  },
) {}

/** An explicit authentication rejection, distinct from failures while checking credentials. */
export class ConnectionRejected extends Schema.TaggedError<ConnectionRejected>()(
  "ConnectionRejected",
  {},
) {
  override get message() {
    return "Unauthorized connection"
  }
}

export class MessageTooLarge extends Schema.TaggedError<MessageTooLarge>()(
  "MessageTooLarge",
  { bytes: Schema.Number, limit: Schema.Number },
) {
  override get message() {
    return "WebSocket result exceeds the 32 KiB single-frame limit"
  }
}

export class GatewayError extends Schema.TaggedError<GatewayError>()(
  "GatewayError",
  {
    operation: Schema.Literals(["PostToConnection", "DeleteConnection"]),
    gone: Schema.Boolean,
    cause: Schema.Defect(),
  },
) {}

/** Backend failure with its original cause retained for logging and retries. */
export class StorageError extends Schema.TaggedError<StorageError>()(
  "StorageError",
  { operation: Schema.String, cause: Schema.Defect() },
) {}

/** Every subscriber was attempted; these infrastructure failures require a retry. */
export class DeliveryError extends Schema.TaggedError<DeliveryError>()(
  "DeliveryError",
  {
    eventId: EventId,
    failures: Schema.Array(
      Schema.Struct({
        connectionId: ConnectionId,
        operationId: OperationId,
        cause: Schema.Defect(),
      }),
    ),
  },
) {}

const reasons = {
  InvalidMessage: 4400,
  Unauthorized: 4401,
  Forbidden: 4403,
  Subprotocol: 4406,
  DuplicateSubscriber: 4409,
  TooManyInitRequests: 4429,
  Internal: 4500,
} as const

export class ProtocolClose extends Schema.TaggedError<ProtocolClose>()(
  "ProtocolClose",
  {
    reason: Schema.Literals([
      "InvalidMessage",
      "Unauthorized",
      "Forbidden",
      "Subprotocol",
      "DuplicateSubscriber",
      "TooManyInitRequests",
      "Internal",
    ]),
  },
) {
  get code() {
    return reasons[this.reason]
  }
}

export class PersistedContextError extends Schema.TaggedError<PersistedContextError>()(
  "PersistedContextError",
  { operation: Schema.Literals(["encode", "decode"]), cause: Schema.Defect() },
) {}

export class ExecutionError extends Schema.TaggedError<ExecutionError>()(
  "ExecutionError",
  {
    cause: Schema.Defect(),
  },
) {}

export const badRequest = (message: string) =>
  new TransportError({ message, code: "BAD_REQUEST", status: 400 })

/** A formatter can report originalError to Sentry before returning a public error. */
export type ErrorFormatter = (error: GraphQLError) => GraphQLFormattedError

// Trust is granted by a request stage, never inferred from missing originalError or extensions.
const diagnostics = new WeakMap<GraphQLError, string>()

const publicFailure = (
  error: unknown,
): ConnectionRejected | TransportError | MessageTooLarge | undefined => {
  const visited = new Set<unknown>()
  while (error && !visited.has(error)) {
    visited.add(error)
    if (
      error instanceof ConnectionRejected ||
      error instanceof TransportError ||
      error instanceof MessageTooLarge
    )
      return error
    error =
      error instanceof GraphQLError
        ? error.originalError
        : error instanceof InputError
          ? error.cause
          : undefined
  }
}

export const formatError: ErrorFormatter = (error) => {
  const visible = publicFailure(error)
  if (visible)
    return {
      message: visible.message,
      path: error.path,
      extensions: {
        code:
          visible instanceof TransportError
            ? visible.code
            : visible instanceof ConnectionRejected
              ? "UNAUTHORIZED"
              : "MESSAGE_TOO_LARGE",
      },
    }
  const code = diagnostics.get(error)
  if (code)
    return {
      message: error.message,
      locations: error.locations,
      extensions: { code },
    }
  return {
    message: "Internal server error",
    path: error.path,
    extensions: { code: "INTERNAL_SERVER_ERROR" },
  }
}

export const toGraphQLError = (error: unknown): GraphQLError =>
  error instanceof GraphQLError
    ? error
    : new GraphQLError("Internal server error", {
        originalError:
          error instanceof Error
            ? error
            : new Error("Non-Error failure", { cause: error }),
      })

/** Call only for graphql-js request diagnostics; retain all exception provenance. */
export const requestError = (
  error: GraphQLError,
  code: string,
): GraphQLError => {
  // Built-in coercion can wrap another GraphQLError. Custom scalar callbacks are
  // guarded by inputSafeSchema and carry InputError even for prelocated errors.
  let current: Error | undefined = error
  const visited = new Set<Error>()
  while (current instanceof GraphQLError && !visited.has(current)) {
    visited.add(current)
    current = current.originalError ?? undefined
  }
  if (current === undefined) diagnostics.set(error, code)
  return error
}
