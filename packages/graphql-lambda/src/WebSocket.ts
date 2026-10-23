import { Cause, Clock, Effect, Option, Schema, Stream } from "effect"
import {
  badRequest,
  ConnectionRejected,
  ExecutionError,
  GatewayError,
  StorageError,
  ProtocolClose,
  MessageTooLarge,
  formatError as defaultFormatError,
  toGraphQLError,
  TransportError,
  type ErrorFormatter,
} from "./Errors.ts"
import { type Executor } from "./Executor.ts"
import type { Gateway } from "./Gateway.ts"
import { ConnectionId, OperationId } from "./Identifiers.ts"
import { decodePersisted, encodePersisted } from "./internal/persisted.ts"
import { close, send } from "./internal/protocol.ts"
import { header, parseJson, prepare } from "./internal/request.ts"
import type { WebSocketRequirements } from "./internal/requirements.ts"
import { operationPubSub } from "./internal/source.ts"
import { Connection, JsonObject, OperationRequest } from "./Models.ts"
import type {
  ContextRequest,
  ProxyResponse,
  ValidationOptions,
  WebSocketEvent,
} from "./Request.ts"
import { Storage } from "./Storage.ts"

const Message = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("connection_init"),
    payload: Schema.optionalKey(Schema.NullOr(JsonObject)),
  }),
  Schema.Struct({
    type: Schema.Literal("subscribe"),
    id: OperationId,
    payload: OperationRequest,
  }),
  Schema.Struct({ type: Schema.Literal("complete"), id: OperationId }),
  Schema.Struct({
    type: Schema.Literal("ping"),
    payload: Schema.optionalKey(Schema.NullOr(JsonObject)),
  }),
  Schema.Struct({
    type: Schema.Literal("pong"),
    payload: Schema.optionalKey(Schema.NullOr(JsonObject)),
  }),
])

export interface WebSocketOptions<C, L, P, E, R> extends ValidationOptions {
  readonly executor: Executor<C, E, R>
  readonly persistedContext: Schema.Codec<P, Schema.Json, R, R>
  readonly context: (
    request: ContextRequest<L, NoInfer<P>>,
  ) => Effect.Effect<C, E, R>
  readonly onConnect?: (
    connection: Connection,
    event: WebSocketEvent,
    lambdaContext: L,
  ) => Effect.Effect<NoInfer<P> | void | false, E, R>
  readonly onInit?: (
    payload: JsonObject,
    persisted: NoInfer<P> | undefined,
    connection: Connection,
    event: WebSocketEvent,
    lambdaContext: L,
  ) => Effect.Effect<NoInfer<P> | false, E, R>
  readonly formatError?: ErrorFormatter
  readonly ttlSeconds?: number
  readonly maxOperations?: number
  readonly acknowledgementPayload?: JsonObject
}

const authenticate = <P, E, R>(run: () => Effect.Effect<P | false, E, R>) =>
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
  P,
  const O extends WebSocketOptions<C, L, P, unknown, unknown>,
>(
  options: O & WebSocketOptions<C, L, P, unknown, unknown>,
): (
  event: WebSocketEvent,
  lambdaContext: L,
) => Effect.Effect<
  ProxyResponse,
  never,
  WebSocketRequirements<O> | Storage | Gateway
>
export function createWebSocketHandler<C, L, P>(
  options: WebSocketOptions<C, L, P, unknown, unknown>,
) {
  const format = options.formatError ?? defaultFormatError
  return Effect.fn("GraphQL.websocket")(function* (
    event: WebSocketEvent,
    lambdaContext: L,
  ) {
    const storage = yield* Storage
    const { connectionId, routeKey, domainName, stage } = event.requestContext
    const id = ConnectionId.make(connectionId)
    const socket = { id, endpoint: `https://${domainName}/${stage}` }
    return yield* Effect.gen(function* () {
      if (routeKey === "$disconnect") {
        yield* storage.disconnect(id)
        return { statusCode: 200, body: "" }
      }
      if (routeKey === "$connect") {
        const protocols =
          header(event, "sec-websocket-protocol")
            ?.split(",")
            .map((value) => value.trim()) ?? []
        if (!protocols.includes("graphql-transport-ws"))
          return yield* new ProtocolClose({ reason: "Subprotocol" })
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
            initialized: false,
            initReceived: false,
            createdAt: now,
            ttl: Math.floor(now / 1000) + ttl,
          }),
        )
        if (options.onConnect) {
          const hook = options.onConnect
          const context = yield* authenticate(() =>
            hook(connection, event, lambdaContext),
          )
          if (context === false) {
            return yield* new ProtocolClose({ reason: "Forbidden" })
          }
          if (context !== undefined)
            connection = yield* storage.update(connection, {
              context: yield* encodePersisted(
                options.persistedContext,
                context,
              ),
            })
        }
        return {
          statusCode: 200,
          headers: { "Sec-WebSocket-Protocol": "graphql-transport-ws" },
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
      // Heartbeats and cancellation must not depend on application context decoding.
      if (message.type === "ping") {
        yield* send(connection, {
          type: "pong",
          ...(message.payload === undefined
            ? {}
            : { payload: message.payload }),
        })
        return { statusCode: 200, body: "" }
      }
      if (message.type === "pong") return { statusCode: 200, body: "" }
      if (message.type !== "connection_init") {
        if (!message.id)
          return yield* new ProtocolClose({ reason: "InvalidMessage" })
        if (message.type === "complete") {
          yield* storage.stop(id, message.id)
          return { statusCode: 200, body: "" }
        }
        if (!connection.initialized || connection.context === undefined)
          return yield* new ProtocolClose({ reason: "Unauthorized" })
      } else {
        connection = yield* storage.beginInit(connection)
      }
      const persisted =
        connection.context === undefined
          ? Option.none<P>()
          : Option.some(
              yield* decodePersisted(
                options.persistedContext,
                connection.context,
              ),
            )
      if (message.type === "connection_init") {
        const hook = options.onInit
        const context = hook
          ? yield* authenticate(() =>
              hook(
                message.payload ?? {},
                Option.getOrUndefined(persisted),
                connection,
                event,
                lambdaContext,
              ),
            )
          : Option.isNone(persisted)
            ? yield* decodePersisted(options.persistedContext, {})
            : persisted.value
        if (context === false) {
          return yield* new ProtocolClose({ reason: "Forbidden" })
        }
        connection = yield* storage.update(connection, {
          initialized: true,
          initParams: message.payload ?? {},
          context: yield* encodePersisted(options.persistedContext, context),
        })
        yield* send(connection, {
          type: "connection_ack",
          ...(options.acknowledgementPayload === undefined
            ? {}
            : { payload: options.acknowledgementPayload }),
        })
        return { statusCode: 200, body: "" }
      }
      if (Option.isNone(persisted))
        return yield* new ProtocolClose({ reason: "Unauthorized" })
      yield* Effect.gen(function* () {
        const request = message.payload
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
            persisted: persisted.value,
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
            if (
              result.value.data === undefined &&
              result.value.errors?.length
            ) {
              yield* send(connection, {
                type: "error",
                id: message.id,
                payload: result.value.errors,
              })
            } else {
              yield* send(connection, {
                type: "next",
                id: message.id,
                payload: result.value,
              })
              yield* send(connection, { type: "complete", id: message.id })
            }
          }
        }).pipe(
          Effect.catch((error) =>
            error instanceof StorageError ||
            error instanceof GatewayError ||
            error instanceof MessageTooLarge
              ? Effect.fail(error)
              : Effect.gen(function* () {
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
                Effect.logError(
                  "GraphQL pending operation cleanup failed",
                  cause,
                ),
              ),
            ),
          ),
        )
      }).pipe(
        Effect.catch((error) =>
          error instanceof ProtocolClose || !(error instanceof TransportError)
            ? Effect.fail(error)
            : send(connection, {
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
          const reason =
            error instanceof ProtocolClose
              ? error.reason
              : error instanceof TransportError
                ? error.status === 401
                  ? "Unauthorized"
                  : "InvalidMessage"
                : "Internal"
          // Client protocol violations are logged as warnings by close().
          if (reason === "Internal")
            yield* Effect.logError("GraphQL WebSocket request failed", cause)
          yield* Effect.gen(function* () {
            const cleanup = yield* Effect.exit(storage.disconnect(id))
            yield* close(socket, reason)
            yield* cleanup
          }).pipe(
            Effect.catchCause((cleanupCause) =>
              Effect.logError(
                "GraphQL connection cleanup failed",
                cleanupCause,
              ),
            ),
          )
          return {
            statusCode:
              reason === "Internal"
                ? 500
                : reason === "Forbidden" || reason === "Unauthorized"
                  ? 401
                  : 400,
            body: "",
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
  if (connection.initialized) yield* send(connection, { type: "ping" })
})
