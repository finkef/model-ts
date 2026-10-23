import {
  Cause,
  Duration,
  Effect,
  Layer,
  Option,
  Schedule,
  Stream,
} from "effect"
import {
  Cookies,
  HttpServer,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/http"
import { Socket } from "effect/socket"
import { GatewayError } from "./Errors.ts"
import { Gateway } from "./Gateway.ts"
import { ConnectionId } from "./Identifiers.ts"
import {
  invocationContext,
  type Invocation,
  type ContextOptions,
  invocation,
  type LambdaContext,
} from "./internal/local/context.ts"
import {
  Delivery,
  makeDelivery,
  type StreamHandler,
} from "./internal/local/delivery.ts"
import type {
  HttpEvent,
  HttpResponse,
  ProxyResponse,
  WebSocketEvent,
} from "./Request.ts"
import { Sandbox } from "./Sandbox.ts"
import { Storage } from "./Storage.ts"
import { keepAlive } from "./WebSocket.ts"

export type { Invocation, LambdaContext } from "./internal/local/context.ts"
export {
  Delivery,
  DeliveryStoppedError,
  delivery,
  type DeliveryOptions,
  type StreamHandler,
} from "./internal/local/delivery.ts"

export type HttpHandler<L = LambdaContext, E = never, R = never> = (
  event: HttpEvent,
  context: L,
) => Effect.Effect<HttpResponse, E, R>
export type WebSocketHandler<L = LambdaContext, E = never, R = never> = (
  event: WebSocketEvent,
  context: L,
) => Effect.Effect<ProxyResponse, E, R>

export interface Options<
  L = LambdaContext,
  E = never,
  RH = never,
  RW = never,
  RS = never,
  K extends Duration.Input | false = false,
  RL = never,
> {
  readonly path?: `/${string}`
  readonly stage?: string
  readonly http?: HttpHandler<L, E, RH>
  readonly webSocket?: WebSocketHandler<L, E, RW>
  readonly stream?: StreamHandler<L, E, RS>
  readonly batchSize?: number
  /** Disabled by default. Sends production keepAlive pings on this interval. */
  readonly keepAlive?: K
  /**
   * Delays HTTP requests before their handler runs, to exercise loading
   * states. A function decides per request, e.g. from a `Ref` the app
   * changes at runtime. WebSocket messages and stream delivery are not
   * delayed. Disabled by default.
   */
  readonly latency?: Latency<RL>
}

/**
 * A fixed delay, or an Effect that decides the delay for each HTTP request.
 */
export type Latency<R = never> =
  | Duration.Input
  | ((event: HttpEvent) => Effect.Effect<Duration.Input, never, R>)
/** Runs production Lambda handlers on the application's Effect HTTP server. */
export const layer = <
  L = LambdaContext,
  E = never,
  RH = never,
  RW = never,
  RS = never,
  S extends StreamHandler<L, E, RS> | undefined = undefined,
  K extends Duration.Input | false = false,
  RL = never,
>(
  options: Options<L, E, RH, RW, RS, K, RL> & {
    readonly stream?: S
  } & ContextOptions<L>,
): Layer.Layer<
  Delivery,
  never,
  | Exclude<RH | RW | RS, Gateway>
  | RL
  | HttpServer.HttpServer
  | ([S] extends [undefined] ? never : Sandbox)
  | (K extends false ? never : Storage)
> => serverLayer(options, (call) => invocationContext(options, call))

const response = (result: ProxyResponse & Partial<HttpResponse>) => {
  const options = {
    status: result.statusCode,
    headers: result.headers,
    // Keep same-name cookies for different paths as separate Set-Cookie fields.
    cookies: Cookies.fromReadonlyRecord(
      Object.fromEntries(
        (result.cookies ?? []).flatMap((header, index) =>
          Object.values(Cookies.fromSetCookie(header).cookies).map((cookie) => [
            String(index),
            cookie,
          ]),
        ),
      ),
    ),
  }
  return result.isBase64Encoded
    ? HttpServerResponse.uint8Array(Buffer.from(result.body, "base64"), options)
    : HttpServerResponse.text(result.body, options)
}

/** A latency as a sleepable duration; anything else is a configuration defect. */
const latencyDuration = (input: Duration.Input) =>
  Effect.suspend(() => {
    const duration = Duration.fromInputUnsafe(input)
    const millis = Duration.toMillis(duration)
    return Number.isFinite(millis) && millis >= 0
      ? Effect.succeed(duration)
      : Effect.die(new Error("latency must be a finite non-negative duration"))
  })

const serverLayer = <L, E, RH, RW, RS, K extends Duration.Input | false, RL>(
  options: Options<L, E, RH, RW, RS, K, RL>,
  context: (invocation: Invocation) => L,
) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const path = options.path ?? "/graphql"
      const interval =
        options.keepAlive === undefined || options.keepAlive === false
          ? undefined
          : Duration.fromInputUnsafe(options.keepAlive)
      if (!path.startsWith("/") || path.includes("?") || path.includes("#"))
        return yield* Effect.die(new Error("path must be an absolute URL path"))
      if (
        interval &&
        (!Number.isFinite(Duration.toMillis(interval)) ||
          Duration.toMillis(interval) <= 0)
      )
        return yield* Effect.die(
          new Error("keepAlive must be a finite positive duration"),
        )
      // Conditional services are checked here; the public signature requires them
      // only when the corresponding transport or timer is enabled.
      const sandbox = options.stream
        ? yield* Effect.serviceOption(Sandbox)
        : Option.none()
      if (options.stream && Option.isNone(sandbox))
        return yield* Effect.die(
          new Error("Local stream delivery requires Sandbox"),
        )
      const keepAliveStorage = interval
        ? yield* Effect.serviceOption(Storage)
        : Option.none()
      if (interval && Option.isNone(keepAliveStorage))
        return yield* Effect.die(
          new Error("Local keepAlive requires PubSub.Storage"),
        )
      const latency = options.latency
      const delay =
        latency === undefined
          ? () => Effect.void
          : typeof latency === "function"
            ? (event: HttpEvent) =>
                latency(event).pipe(
                  Effect.flatMap(latencyDuration),
                  Effect.flatMap(Effect.sleep),
                )
            : yield* latencyDuration(latency).pipe(
                Effect.map((duration) => () => Effect.sleep(duration)),
              )
      const sockets = new Map<ConnectionId, Socket.Writer>()
      const write = Effect.fn("GraphQL.Local.gateway")(function* (
        operation: GatewayError["operation"],
        id: ConnectionId,
        data: string | Socket.CloseEvent,
      ) {
        const writer = sockets.get(id)
        if (!writer)
          return yield* new GatewayError({
            operation,
            gone: true,
            cause: new Error("Connection is not open"),
          })
        if (data instanceof Socket.CloseEvent) sockets.delete(id)
        yield* writer.write(data).pipe(
          Effect.mapError(
            (cause) =>
              new GatewayError({
                operation,
                gone: cause.reason._tag === "SocketCloseError",
                cause,
              }),
          ),
        )
      })
      const gateway = Layer.succeed(
        Gateway,
        Gateway.of({
          post: (_endpoint, id, data) => write("PostToConnection", id, data),
          // DeleteConnection accepts only a connection ID, not a close code:
          // https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/ApiGatewayManagementApi.html#deleteConnection-property
          // AWS does not guarantee its wire close code in that contract; use
          // normal closure (1000), never a graphql-transport-ws error code.
          close: (_endpoint, id) =>
            write("DeleteConnection", id, new Socket.CloseEvent(1000)),
        }),
      )
      const websocket = options.webSocket
      const http = options.http
      const stream = options.stream
      const app = Effect.fn("GraphQL.Local.request")(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest
        const url = new URL(request.url, "http://localhost")
        if (url.pathname !== path)
          return HttpServerResponse.empty({ status: 404 })
        const headers = { ...request.headers }
        const sourceIp = Option.getOrElse(
          request.remoteAddress,
          () => "127.0.0.1",
        )
        const upgrading =
          request.method === "GET" &&
          headers.upgrade?.toLowerCase() === "websocket"
        if (upgrading) {
          if (!websocket) return HttpServerResponse.empty({ status: 404 })
          const id = ConnectionId.make(crypto.randomUUID())
          const invoke = Effect.fn("GraphQL.Local.websocket")(function* (
            routeKey: string,
            body?: string,
          ) {
            // WebSocket proxy $connect forwards the original request headers,
            // including Cookie (unlike HTTP API payload 2.0's cookies field):
            // https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-websocket-api-integration-requests.html
            const event: WebSocketEvent = {
              ...(routeKey === "$connect" ? { headers } : {}),
              body,
              isBase64Encoded: false,
              requestContext: {
                routeKey,
                connectionId: id,
                domainName: headers.host ?? "localhost",
                stage: options.stage ?? "local",
                requestId: crypto.randomUUID(),
                identity: { sourceIp },
              },
            }
            return yield* websocket(event, context(invocation(event)))
          })
          const connected = yield* invoke("$connect")
          if (connected.statusCode < 200 || connected.statusCode >= 300)
            return response(connected)
          return yield* Effect.scoped(
            Effect.gen(function* () {
              yield* Effect.addFinalizer(() =>
                Effect.gen(function* () {
                  sockets.delete(id)
                  yield* invoke("$disconnect").pipe(
                    Effect.catchCause((cause) =>
                      Effect.logWarning(
                        "GraphQL local disconnect failed",
                        cause,
                      ),
                    ),
                  )
                }),
              )
              const socket = yield* request.upgrade
              // Acquiring the reader performs the upgrade and installs frame listeners.
              const reader = yield* socket.reader
              const writer = yield* socket.writer
              sockets.set(id, writer)
              if (interval && Option.isSome(keepAliveStorage))
                yield* keepAlive(id).pipe(
                  Effect.provideService(Storage, keepAliveStorage.value),
                  Effect.catchCause((cause) =>
                    Cause.hasInterrupts(cause)
                      ? Effect.interrupt
                      : Effect.logWarning(
                          "GraphQL local keep-alive failed",
                          cause,
                        ),
                  ),
                  Effect.repeat(Schedule.spaced(interval)),
                  Effect.delay(interval),
                  Effect.forkScoped,
                )
              yield* Stream.fromEffectRepeat(reader.pull).pipe(
                Stream.flattenIterable,
                Stream.runForEachWhile(
                  (
                    frame,
                  ): Effect.Effect<boolean, E | Socket.SocketError, RW> =>
                    typeof frame === "string"
                      ? invoke("$default", frame).pipe(Effect.as(true))
                      : writer
                          .write(new Socket.CloseEvent(1003))
                          .pipe(Effect.as(false)),
                ),
                Effect.catch((error) =>
                  Socket.isSocketError(error) &&
                  error.reason._tag === "SocketCloseError"
                    ? Effect.void
                    : Effect.fail(error),
                ),
              )
              return HttpServerResponse.empty()
            }),
          )
        }
        if (!http) return HttpServerResponse.empty({ status: 404 })
        const cookie = headers.cookie
        delete headers.cookie
        const params = new Map<string, string>()
        for (const [name, value] of url.searchParams)
          params.set(
            name,
            params.has(name) ? `${params.get(name)},${value}` : value,
          )
        const event: HttpEvent = {
          version: "2.0",
          routeKey: "$default",
          rawPath: url.pathname,
          rawQueryString: url.search.slice(1),
          headers,
          ...(cookie
            ? { cookies: cookie.split(";").map((value) => value.trim()) }
            : {}),
          queryStringParameters: Object.fromEntries(params),
          body:
            request.method === "GET" || request.method === "HEAD"
              ? undefined
              : yield* request.text,
          isBase64Encoded: false,
          requestContext: {
            requestId: crypto.randomUUID(),
            http: {
              method: request.method,
              path: url.pathname,
              sourceIp,
              userAgent: headers["user-agent"] ?? "",
            },
          },
        }
        yield* delay(event)
        return response(yield* http(event, context(invocation(event))))
      })().pipe(
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            if (Cause.hasInterrupts(cause)) return yield* Effect.interrupt
            yield* Effect.logWarning("GraphQL local request failed", cause)
            return HttpServerResponse.empty({ status: 500 })
          }),
        ),
      )
      return Layer.merge(
        Layer.effectDiscard(
          Effect.gen(function* () {
            yield* HttpServer.serveEffect(app)
            // Run before the platform's graceful HTTP shutdown waits for open sockets.
            yield* Effect.addFinalizer(() =>
              Effect.gen(function* () {
                const open = Array.from(sockets.values())
                sockets.clear()
                yield* Effect.forEach(
                  open,
                  (writer) =>
                    writer
                      .write(new Socket.CloseEvent(1000))
                      .pipe(
                        Effect.catchCause((cause) =>
                          Effect.logWarning(
                            "GraphQL local socket shutdown failed",
                            cause,
                          ),
                        ),
                      ),
                  { discard: true },
                )
              }),
            )
          }),
        ),
        stream && Option.isSome(sandbox)
          ? makeDelivery(
              (event) => stream(event, context(invocation(event))),
              options,
            ).pipe(Layer.provide(Layer.succeed(Sandbox, sandbox.value)))
          : Layer.succeed(Delivery, { settled: Effect.void }),
      ).pipe(Layer.provide(gateway))
    }),
  )
