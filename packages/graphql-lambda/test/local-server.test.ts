import { expect, it } from "@effect/vitest"
import { Deferred, Duration, Effect, Layer, Ref, Schema } from "effect"
import { TestClock } from "effect/testing"
import { describe, inject } from "vitest"
import {
  Gateway,
  ConnectionId,
  PubSub,
  Topic,
  createHttpHandler,
  createWebSocketHandler,
  type ContextRequest,
  type HttpEvent,
  type WebSocketEvent,
} from "../src/index.ts"
import * as Local from "../src/Local.ts"
import { GatewayMessages, Sandbox } from "../src/Sandbox.ts"
import { options, type LambdaContext } from "./fixtures.ts"
import {
  openSocket,
  origin,
  rejectedHandshake,
  server,
} from "./local-fixtures.ts"

const context = () => ({})
const storage = Sandbox.layerMemory()

describe.skipIf(inject("graphqlDynamoDB") !== "memory")("local server", () => {
  it.effect(
    "HTTP queries, mutations, request metadata, cookies and GraphiQL round trip",
    () => {
      const requests: Array<HttpEvent> = []
      const http = createHttpHandler({
        ...options,
        context: (request: ContextRequest<Local.LambdaContext>) => {
          if ("version" in request.event) {
            requests.push(request.event)
            expect(request.lambdaContext.awsRequestId).toBe(
              request.event.requestContext.requestId,
            )
          }
          return options.context({
            ...request,
            lambdaContext: { user: "local" },
          })
        },
        graphiql: { subscriptionUrl: "ws://localhost/graphql" },
      })
      return Effect.gen(function* () {
        const url = `${yield* origin}/graphql`
        const result = yield* Effect.promise(() =>
          fetch(`${url}?tag=one&tag=two`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Cookie: "session=old; theme=dark",
              "X-Trace": "one,two",
            },
            body: JSON.stringify({ query: "{ me }" }),
          }),
        )
        expect(result.status).toBe(200)
        expect(yield* Effect.promise(() => result.json())).toEqual({
          data: { me: "local" },
        })
        expect(result.headers.getSetCookie()).toEqual([
          "session=rotated; HttpOnly",
          "refresh=rotated; Secure",
        ])
        expect(requests[0]).toMatchObject({
          version: "2.0",
          rawPath: "/graphql",
          rawQueryString: "tag=one&tag=two",
          queryStringParameters: { tag: "one,two" },
          cookies: ["session=old", "theme=dark"],
          headers: { "x-trace": "one,two" },
          requestContext: {
            http: { method: "POST", path: "/graphql", sourceIp: "127.0.0.1" },
          },
        })
        expect(requests[0].headers.cookie).toBeUndefined()
        const mutation = yield* Effect.promise(() =>
          fetch(url, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ query: "mutation { update }" }),
          }),
        )
        expect(yield* Effect.promise(() => mutation.json())).toEqual({
          data: { update: "updated" },
        })
        const html = yield* Effect.promise(() =>
          fetch(url, { headers: { accept: "text/html" } }),
        )
        expect(html.headers.get("content-type")).toBe(
          "text/html; charset=utf-8",
        )
        expect(yield* Effect.promise(() => html.text())).toContain(
          "ws://localhost/graphql",
        )
        const missing = yield* Effect.promise(() => fetch(`${url}/missing`))
        expect(missing.status).toBe(404)
      }).pipe(
        Effect.provide(Local.layer({ http }).pipe(Layer.provideMerge(server))),
      )
    },
  )

  it.effect("$connect receives cookies and can reject before upgrade", () => {
    let handshake: WebSocketEvent | undefined
    const webSocket = createWebSocketHandler({
      ...options,
      onConnect: (_connection, event: WebSocketEvent) => {
        handshake = event
        return Effect.succeed(false as const)
      },
    })
    return Effect.gen(function* () {
      expect(yield* rejectedHandshake()).toEqual({
        status: 401,
        upgraded: false,
      })
      expect(handshake?.headers?.cookie).toBe("session=handshake")
      expect(handshake?.requestContext).toMatchObject({
        routeKey: "$connect",
        stage: "local",
        identity: { sourceIp: "127.0.0.1" },
      })
      expect(handshake?.requestContext.domainName).toBe(
        (yield* origin).replace("http://", ""),
      )
      expect(Object.values(yield* (yield* Sandbox).snapshot)).toEqual([])
    }).pipe(
      Effect.provide(
        Local.layer({ webSocket, lambdaContext: context }).pipe(
          Layer.provideMerge(Layer.merge(server, storage)),
        ),
      ),
    )
  })

  it.effect(
    "real socket ack, filtered publication, next, complete, client disconnect and server close",
    () =>
      Effect.gen(function* () {
        const disconnected = yield* Deferred.make<void>()
        const production = createWebSocketHandler(options)
        const webSocket = (event: WebSocketEvent, lambda: LambdaContext) =>
          production(event, lambda).pipe(
            Effect.tap(() =>
              event.requestContext.routeKey === "$disconnect"
                ? Deferred.succeed(disconnected, undefined)
                : Effect.void,
            ),
          )
        yield* Effect.gen(function* () {
          const client = yield* openSocket()
          expect(client.protocol).toBe("graphql-transport-ws")
          yield* client.send({
            type: "connection_init",
            payload: { token: "valid" },
          })
          expect(yield* client.next).toEqual({ type: "connection_ack" })
          yield* client.send({
            type: "subscribe",
            id: "1",
            payload: { query: 'subscription { message(domain: "a") }' },
          })
          // Per-connection frames execute serially. Pong is a registration barrier.
          yield* client.send({ type: "ping" })
          expect(yield* client.next).toEqual({ type: "pong" })
          yield* PubSub.publish(Topic.make("user"), {
            domainId: "b",
            message: "skip",
          })
          yield* PubSub.publish(Topic.make("user"), {
            domainId: "a",
            message: "delivered",
          })
          yield* (yield* Local.Delivery).settled
          expect(yield* client.next).toEqual({
            type: "next",
            id: "1",
            payload: { data: { message: "delivered" } },
          })
          yield* client.send({ type: "complete", id: "1" })
          yield* client.send({ type: "ping" })
          expect(yield* client.next).toEqual({ type: "pong" })
          yield* PubSub.publish(Topic.make("user"), {
            domainId: "a",
            message: "cancelled",
          })
          yield* (yield* Local.Delivery).settled
          yield* client.send({ type: "ping" })
          expect(yield* client.next).toEqual({ type: "pong" })
          client.close()
          yield* client.closed
          yield* Deferred.await(disconnected)
          const sandbox = yield* Sandbox
          expect(
            Object.values(yield* sandbox.snapshot).filter(
              (row) =>
                row._tag === "GraphQLConnection" ||
                row._tag === "GraphQLSubscriptionOperation",
            ),
          ).toEqual([])
          const second = yield* openSocket()
          yield* second.send({
            type: "connection_init",
            payload: { token: "valid" },
          })
          expect(yield* second.next).toEqual({ type: "connection_ack" })
          // Repeated init exercises the production DeleteConnection path.
          yield* second.send({ type: "connection_init" })
          expect(yield* second.closed).toBe(1000)
          const third = yield* openSocket()
          yield* third.send({
            type: "connection_init",
            payload: { token: "valid" },
          })
          expect(yield* third.next).toEqual({ type: "connection_ack" })
        }).pipe(
          Effect.provide(
            Local.layer({
              webSocket,
              stream: PubSub.DynamoDB.streamHandler(options),
              lambdaContext: context,
            }).pipe(Layer.provideMerge(Layer.merge(server, storage))),
          ),
        )
      }),
  )
  it.effect(
    "base64 responses preserve multiple same-name cookies; failed requests stay isolated",
    () => {
      const http = (event: HttpEvent) =>
        event.rawQueryString === "fail"
          ? Effect.fail(new Error("request failed"))
          : Effect.succeed({
              statusCode: 201,
              headers: {
                "content-type": "application/octet-stream",
                "x-response": "local",
              },
              cookies: ["session=one; Path=/", "session=two; Path=/graphql"],
              body: Buffer.from([0, 255, 42]).toString("base64"),
              isBase64Encoded: true,
            })
      return Effect.gen(function* () {
        const url = `${yield* origin}/custom`
        const failed = yield* Effect.promise(() => fetch(`${url}?fail`))
        expect(failed.status).toBe(500)
        const result = yield* Effect.promise(() => fetch(url))
        expect(result.status).toBe(201)
        expect(
          Array.from(
            new Uint8Array(yield* Effect.promise(() => result.arrayBuffer())),
          ),
        ).toEqual([0, 255, 42])
        expect(result.headers.getSetCookie()).toEqual([
          "session=one; Path=/",
          "session=two; Path=/graphql",
        ])
        yield* (yield* Local.Delivery).settled
      }).pipe(
        Effect.provide(
          Local.layer({ path: "/custom", http }).pipe(
            Layer.provideMerge(server),
          ),
        ),
      )
    },
  )

  it.effect(
    "keep-alive runs the production ping and persisted codecs survive real delivery",
    () => {
      const persisted = Schema.Struct({
        user: Schema.String,
        since: Schema.DateFromString,
      })
      let deliveries = 0
      const settings = {
        ...options,
        persistedContext: persisted,
        onInit: () =>
          Effect.succeed({
            user: "codec",
            since: new Date("2026-01-01T00:00:00Z"),
          }),
        context: (
          request: ContextRequest<Local.LambdaContext, typeof persisted.Type>,
        ) =>
          Effect.sync(() => {
            expect(request.persisted.since).toBeInstanceOf(Date)
            if (!("requestContext" in request.event)) deliveries++
            return {
              user: request.persisted.user,
              allowed: true,
              pubSub: request.pubSub,
            }
          }),
      }
      return Effect.gen(function* () {
        const client = yield* openSocket()
        yield* client.send({ type: "connection_init" })
        expect(yield* client.next).toEqual({ type: "connection_ack" })
        yield* TestClock.adjust("5 seconds")
        expect(yield* client.next).toEqual({ type: "ping" })
        yield* client.send({
          type: "subscribe",
          id: "codec",
          payload: { query: 'subscription { message(domain: "a") }' },
        })
        yield* client.send({ type: "ping" })
        expect(yield* client.next).toEqual({ type: "pong" })
        yield* PubSub.publish(Topic.make("user"), {
          domainId: "a",
          message: "decoded",
        })
        yield* (yield* Local.Delivery).settled
        expect(yield* client.next).toEqual({
          type: "next",
          id: "codec",
          payload: { data: { message: "decoded" } },
        })
        expect(deliveries).toBe(1)
      }).pipe(
        Effect.provide(
          Local.layer({
            webSocket: createWebSocketHandler(settings),
            stream: PubSub.DynamoDB.streamHandler(settings),
            keepAlive: "5 seconds",
          }).pipe(Layer.provideMerge(Layer.merge(server, storage))),
        ),
      )
    },
  )
  it.effect(
    "the socket Gateway is private and reports gone connections to handlers",
    () => {
      const http = () =>
        Effect.gen(function* () {
          const gateway = yield* Gateway
          const missing = ConnectionId.make("missing")
          const post = yield* gateway
            .post("ignored", missing, "frame")
            .pipe(Effect.flip)
          const close = yield* gateway
            .close("ignored", missing)
            .pipe(Effect.flip)
          expect(post.gone).toBe(true)
          expect(close.gone).toBe(true)
          return { statusCode: 200, body: "ok", isBase64Encoded: false }
        })
      return Effect.gen(function* () {
        const url = yield* origin
        const response = yield* Effect.promise(() => fetch(`${url}/graphql`))
        expect(response.status).toBe(200)
        yield* (yield* Gateway).post("app", ConnectionId.make("app"), "outside")
        expect(yield* (yield* GatewayMessages).all).toEqual([
          { endpoint: "app", connectionId: "app", data: "outside" },
        ])
      }).pipe(
        Effect.provide(
          Local.layer({ http }).pipe(
            Layer.provideMerge(Layer.merge(server, Sandbox.layerGateway)),
          ),
        ),
      )
    },
  )
})

describe.skipIf(inject("graphqlDynamoDB") !== "memory")("local latency", () => {
  const echo = (event: HttpEvent) =>
    Effect.succeed({
      statusCode: 200,
      headers: {},
      body: event.rawQueryString,
      isBase64Encoded: false,
    })
  const timed = (query: string) =>
    Effect.gen(function* () {
      const url = `${yield* origin}/graphql?${query}`
      const started = Date.now()
      const body = yield* Effect.promise(() =>
        fetch(url).then((response) => response.text()),
      )
      return { body, elapsed: Date.now() - started }
    })

  it.live("delays every HTTP request by a fixed latency", () =>
    Effect.gen(function* () {
      const { body, elapsed } = yield* timed("fixed")
      expect(body).toBe("fixed")
      expect(elapsed).toBeGreaterThanOrEqual(200)
    }).pipe(
      Effect.provide(
        Local.layer({ http: echo, latency: "200 millis" }).pipe(
          Layer.provideMerge(server),
        ),
      ),
    ),
  )

  it.live("decides the latency per request and can change it at runtime", () =>
    Effect.gen(function* () {
      const latency = yield* Ref.make<Duration.Input>(Duration.zero)
      const layer = Local.layer({
        http: echo,
        latency: (event) =>
          event.rawQueryString === "control"
            ? Effect.succeed(Duration.zero)
            : Ref.get(latency),
      }).pipe(Layer.provideMerge(server))
      yield* Effect.gen(function* () {
        expect((yield* timed("before")).elapsed).toBeLessThan(250)
        yield* Ref.set(latency, "300 millis")
        expect((yield* timed("slow")).elapsed).toBeGreaterThanOrEqual(300)
        expect((yield* timed("control")).elapsed).toBeLessThan(250)
      }).pipe(Effect.provide(layer))
    }),
  )
})
