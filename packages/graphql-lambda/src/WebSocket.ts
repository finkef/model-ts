import { Cause, Clock, Effect, Option, Schema, Stream } from "effect"
import {
  badRequest,
  ConnectionRejected,
  ExecutionError,
  MessageTooLarge,
  formatError as defaultFormatError,
  toGraphQLError,
  TransportError,
  type ErrorFormatter,
} from "./Errors.ts"
import { type Executor } from "./Executor.ts"
import { Gateway } from "./Gateway.ts"
import { ConnectionId, OperationId, type Topic } from "./Identifiers.ts"
import {
  decodeRequest,
  header,
  parseJson,
  prepare,
} from "./internal/request.ts"
import type { WebSocketRequirements } from "./internal/requirements.ts"
import type { SubscriptionOperation } from "./Models.ts"
import { Connection, JsonObject } from "./Models.ts"
import { operationPubSub } from "./PubSub.ts"
import type {
  ContextRequest,
  ProxyResponse,
  StreamRecord,
  DirectEvent,
  ValidationOptions,
  WebSocketEvent,
} from "./Request.ts"
import { Storage } from "./Storage.ts"

const Message = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("connection_init"),
    payload: Schema.optionalKey(JsonObject),
  }),
  Schema.Struct({
    type: Schema.Literal("start"),
    id: OperationId,
    payload: Schema.Unknown,
  }),
  Schema.Struct({ type: Schema.Literal("stop"), id: OperationId }),
  Schema.Struct({ type: Schema.Literal("connection_terminate") }),
])

export interface WebSocketOptions<C, L, E, R> extends ValidationOptions {
  readonly executor: Executor<C, E, R>
  readonly context: (request: ContextRequest<L>) => Effect.Effect<C, E, R>
  readonly onWebsocketConnect?: (
    connection: Connection,
    event: WebSocketEvent,
    lambdaContext: L,
  ) => Effect.Effect<JsonObject | false, E, R>
  readonly onConnect?: (
    payload: JsonObject,
    connection: Connection,
    event: WebSocketEvent,
    lambdaContext: L,
  ) => Effect.Effect<JsonObject | false, E, R>
  readonly formatError?: ErrorFormatter
  readonly ttlSeconds?: number
  readonly maxOperations?: number
  readonly initRetries?: number
  readonly initDelayMillis?: number
}

export const send = Effect.fn("GraphQL.send")(function* (
  connection: Connection,
  frame: unknown,
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

const close = Effect.fn("GraphQL.close")(function* (connection: Connection) {
  const storage = yield* Storage
  yield* storage.disconnect(connection.id)
  const gateway = yield* Gateway
  yield* gateway
    .close(connection.endpoint, connection.id)
    .pipe(
      Effect.catchTag("GatewayError", (error) =>
        error.gone ? Effect.void : Effect.fail(error),
      ),
    )
})

const authenticate = <E, R>(
  run: () => Effect.Effect<JsonObject | false, E, R>,
) =>
  Effect.suspend(run).pipe(
    Effect.mapError((cause): ConnectionRejected | ExecutionError =>
      Schema.is(ConnectionRejected)(cause)
        ? cause
        : new ExecutionError({ cause }),
    ),
    Effect.catchTag("ConnectionRejected", () => Effect.succeed(false as const)),
  )

export function createWebSocketHandler<
  C,
  L,
  const O extends WebSocketOptions<C, L, unknown, unknown>,
>(
  options: O & WebSocketOptions<C, L, unknown, unknown>,
): (
  event: WebSocketEvent,
  lambdaContext: L,
) => Effect.Effect<
  ProxyResponse,
  never,
  WebSocketRequirements<O> | Storage | Gateway
>
export function createWebSocketHandler<C, L>(
  options: WebSocketOptions<C, L, unknown, unknown>,
) {
  const format = options.formatError ?? defaultFormatError
  const reject = (connection: Connection) =>
    Effect.gen(function* () {
      yield* send(connection, {
        type: "error",
        payload: format(toGraphQLError(new ConnectionRejected({}))),
      }).pipe(
        Effect.catch((error) =>
          Effect.logError("GraphQL rejection frame failed", error),
        ),
      )
      yield* close(connection)
    })
  return Effect.fn("GraphQL.websocket")(function* (
    event: WebSocketEvent,
    lambdaContext: L,
  ) {
    const storage = yield* Storage
    const { connectionId, routeKey, domainName, stage } = event.requestContext
    const id = ConnectionId.make(connectionId)
    return yield* Effect.gen(function* () {
      if (routeKey === "$disconnect") {
        yield* storage.disconnect(id)
        return { statusCode: 200, body: "" }
      }
      if (routeKey === "$connect") {
        const now = yield* Clock.currentTimeMillis
        const ttl = options.ttlSeconds ?? 7200
        if (!Number.isFinite(ttl) || ttl <= 0 || ttl > 7200)
          return yield* badRequest(
            "Connection TTL must be between 1 and 7200 seconds",
          )
        let connection = yield* storage.create(
          new Connection({
            id,
            endpoint: `https://${domainName}/${stage}`,
            context: {},
            initialized: false,
            createdAt: now,
            ttl: Math.floor(now / 1000) + ttl,
          }),
        )
        if (options.onWebsocketConnect) {
          const hook = options.onWebsocketConnect
          const context = yield* authenticate(() =>
            hook(connection, event, lambdaContext),
          )
          if (context === false) {
            yield* storage.disconnect(id)
            return { statusCode: 401, body: "Unauthorized connection" }
          }
          connection = yield* storage.update(connection, { context })
        }
        const protocols =
          header(event, "sec-websocket-protocol")
            ?.split(",")
            .map((value) => value.trim()) ?? []
        return {
          statusCode: 200,
          headers: Object.fromEntries(
            protocols.includes("graphql-ws")
              ? [["Sec-WebSocket-Protocol", "graphql-ws"]]
              : [],
          ),
          body: "",
        }
      }
      if (routeKey !== "$default")
        return yield* badRequest("Unknown WebSocket route")
      const body = event.isBase64Encoded
        ? Buffer.from(event.body ?? "", "base64").toString("utf8")
        : (event.body ?? "")
      if (Buffer.byteLength(body) > (options.maxQueryBytes ?? 64 * 1024))
        return yield* badRequest("WebSocket operation is too large")
      const message = yield* parseJson(body).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Message)),
        Effect.mapError(() => badRequest("Invalid WebSocket message")),
      )
      let connection = yield* storage.liveConnection(id)
      if (message.type === "connection_init") {
        // Duplicate init must not replace authenticated state or race active operations.
        if (connection.initialized) {
          yield* send(connection, { type: "connection_ack" })
          return { statusCode: 200, body: "" }
        }
        const hook = options.onConnect
        const context = hook
          ? yield* authenticate(() =>
              hook(message.payload ?? {}, connection, event, lambdaContext),
            )
          : {}
        if (context === false) {
          yield* reject(connection)
          return { statusCode: 401, body: "" }
        }
        connection = yield* storage.update(connection, {
          initialized: true,
          initParams: message.payload ?? {},
          context: { ...connection.context, ...context },
        })
        yield* send(connection, { type: "connection_ack" })
        return { statusCode: 200, body: "" }
      }
      if (message.type === "connection_terminate") {
        yield* close(connection)
        return { statusCode: 200, body: "" }
      }
      for (
        let i = 0;
        !connection.initialized && i < (options.initRetries ?? 10);
        i++
      ) {
        yield* Effect.sleep(options.initDelayMillis ?? 50)
        connection = yield* storage.liveConnection(id)
      }
      if (!connection.initialized) {
        yield* reject(connection)
        return { statusCode: 401, body: "" }
      }
      if (!message.id || message.id.length > 128)
        return yield* badRequest("Invalid operation id")
      if (message.type === "stop") {
        yield* storage.stop(id, message.id)
        yield* send(connection, { type: "complete", id: message.id })
        return { statusCode: 200, body: "" }
      }
      yield* Effect.gen(function* () {
        const request = yield* decodeRequest(message.payload)
        const generation = yield* storage.reserve(
          id,
          message.id,
          request,
          options.maxOperations ?? 100,
        )
        let active = false
        yield* Effect.gen(function* () {
          const prepared = yield* prepare(options.executor, request, options)
          if (prepared.errors.length) {
            if (yield* storage.stop(id, message.id, generation))
              yield* send(connection, {
                type: "error",
                id: message.id,
                payload: prepared.errors.map(format),
              })
            return
          }
          const { pubSub, topics } = operationPubSub()
          const context = yield* options.context({
            event,
            lambdaContext,
            connection,
            persisted: connection.context,
            pubSub,
            response: { headers: {}, cookies: [] },
          })
          const input = { request, document: prepared.document, context }
          const result =
            prepared.operation?.operation === "subscription"
              ? yield* options.executor.subscribe(input).pipe(Stream.runHead)
              : Option.some(yield* options.executor.execute(input))
          if (Option.isNone(result)) {
            // Registration sources are finite and empty: running the Stream opens lazy
            // Effect resolvers, collects their topics, and closes every source scope.
            active = yield* storage.activate(id, message.id, generation, [
              ...topics,
            ])
          } else if (yield* storage.stop(id, message.id, generation)) {
            yield* send(connection, {
              type: "data",
              id: message.id,
              payload: result.value,
            })
            yield* send(connection, { type: "complete", id: message.id })
          }
        }).pipe(
          Effect.catch((error) =>
            Effect.gen(function* () {
              if (yield* storage.stop(id, message.id, generation))
                yield* send(connection, {
                  type: "error",
                  id: message.id,
                  payload: [format(toGraphQLError(error))],
                })
            }),
          ),
          Effect.ensuring(
            Effect.gen(function* () {
              if (!active) yield* storage.stop(id, message.id, generation)
            }).pipe(
              Effect.catchCause((cause) =>
                Effect.logError("GraphQL pending-start cleanup failed", cause),
              ),
            ),
          ),
        )
      }).pipe(
        Effect.catch((error) =>
          send(connection, {
            type: "error",
            id: message.id,
            payload: [format(toGraphQLError(error))],
          }),
        ),
      )
      return { statusCode: 200, body: "" }
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.gen(function* () {
          if (Cause.hasInterrupts(cause)) return yield* Effect.interrupt
          const error = Cause.hasDies(cause)
            ? new ExecutionError({ cause: Cause.squash(cause) })
            : Cause.squash(cause)
          yield* Effect.logError("GraphQL WebSocket request failed", cause)
          if (routeKey === "$connect") {
            // A failed upgrade has no future disconnect callback to remove its row.
            yield* storage
              .disconnect(id)
              .pipe(
                Effect.catchCause((cleanupCause) =>
                  Effect.logError(
                    "GraphQL failed-upgrade cleanup failed",
                    cleanupCause,
                  ),
                ),
              )
          }
          return {
            statusCode: error instanceof TransportError ? error.status : 500,
            body: JSON.stringify({ errors: [format(toGraphQLError(error))] }),
          }
        }),
      ),
      Effect.map((response): ProxyResponse => response),
    )
  })
}

/** Invoke from an external scheduler for known active connection IDs. No Lambda timers. */
export const keepAlive = Effect.fn("GraphQL.keepAlive")(function* (
  connectionId: ConnectionId,
) {
  const storage = yield* Storage
  const connection = yield* storage.liveConnection(connectionId)
  if (connection.initialized) yield* send(connection, { type: "ka" })
})

/** Same validation, context factory, subscribe guards and field authorization as registration. */
export const deliverOperation = <C, L, E, R>(
  options: WebSocketOptions<C, L, E, R>,
  connection: Connection,
  operation: SubscriptionOperation,
  event: { readonly topic: Topic; readonly payload: Schema.Json },
  record: StreamRecord | DirectEvent,
  lambdaContext: L,
) =>
  Effect.gen(function* () {
    const format = options.formatError ?? defaultFormatError
    const result = yield* Effect.gen(function* () {
      const prepared = yield* prepare(
        options.executor,
        operation.request,
        options,
      )
      if (prepared.errors.length) return { errors: prepared.errors.map(format) }
      const { pubSub } = operationPubSub(event)
      const context = yield* options.context({
        event: record,
        lambdaContext,
        connection,
        persisted: connection.context,
        pubSub,
        response: { headers: {}, cookies: [] },
      })
      const result = yield* options.executor
        .subscribe({
          document: prepared.document,
          request: operation.request,
          context,
        })
        .pipe(Stream.runHead)
      return Option.getOrUndefined(result)
    }).pipe(
      Effect.catch((error) =>
        Effect.succeed({ errors: [format(toGraphQLError(error))] }),
      ),
      Effect.catchCause((cause) =>
        Effect.gen(function* () {
          if (Cause.hasInterrupts(cause)) return yield* Effect.interrupt
          yield* Effect.logError("GraphQL subscriber execution failed", cause)
          // Do not invoke application formatters again: they can be the defect's source.
          return {
            errors: [
              {
                message: "Internal server error",
                extensions: { code: "INTERNAL_SERVER_ERROR" },
              },
            ],
          }
        }),
      ),
    )
    // Infrastructure failures still reach the stream retry policy. Only execution failures
    // become GraphQL results; retrying a permission denial cannot make it succeed.
    if (result)
      yield* send(connection, {
        type: "data",
        id: operation.operationId,
        payload: result,
      })
  })
