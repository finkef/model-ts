import { expect, it } from "@effect/vitest"
import { Cause, Effect, Logger, Schema } from "effect"
import {
  ConnectionId,
  Gateway,
  GatewayError,
  OperationId,
  PubSub,
  Topic,
  createWebSocketHandler,
  type ContextRequest,
} from "../src/index.ts"
import { decodePersisted, encodePersisted } from "../src/internal/persisted.ts"
import { GatewayMessages, Sandbox } from "../src/Sandbox.ts"
import {
  options,
  publish,
  streamEvent,
  testLayer,
  wsEvent,
  type LambdaContext,
} from "./fixtures.ts"

const Persisted = Schema.Struct({
  user: Schema.String,
  since: Schema.DateFromString,
  handshakeOnly: Schema.optionalKey(Schema.String),
})
const since = new Date("2026-01-01T00:00:00.000Z")
const settings = {
  ...options,
  persistedContext: Persisted,
  onConnect: () => Effect.succeed({ user: "Ada", since, handshakeOnly: "yes" }),
  onInit: (
    _payload: Schema.Json,
    previous: typeof Persisted.Type | undefined,
  ) =>
    Effect.sync(() => {
      expect(previous?.since).toBeInstanceOf(Date)
      expect(previous?.handshakeOnly).toBe("yes")
      return { user: "Grace", since }
    }),
  context: (request: ContextRequest<LambdaContext, typeof Persisted.Type>) =>
    Effect.sync(() => {
      expect(request.persisted.since).toBeInstanceOf(Date)
      expect(request.persisted.since.toISOString()).toBe(since.toISOString())
      return {
        pubSub: request.pubSub,
        user: request.persisted.user,
        allowed: true,
      }
    }),
}
const handler = createWebSocketHandler(settings)
const initialize = (id: string) =>
  Effect.gen(function* () {
    yield* handler(wsEvent(id, "$connect"), {})
    yield* handler(wsEvent(id, "$default", { type: "connection_init" }), {})
  })
const subscribe = (id: string) =>
  handler(
    wsEvent(id, "$default", {
      type: "subscribe",
      id: "s",
      payload: { query: 'subscription { message(domain:"a") }' },
    }),
    {},
  )

it.effect(
  "encodes hook values, decodes each request and replaces handshake context at init",
  () =>
    Effect.gen(function* () {
      const storage = yield* PubSub.Storage
      yield* handler(wsEvent("c1", "$connect"), {})
      expect(
        (yield* storage.liveConnection(ConnectionId.make("c1"))).context,
      ).toEqual({
        user: "Ada",
        since: since.toISOString(),
        handshakeOnly: "yes",
      })
      yield* handler(wsEvent("c1", "$default", { type: "connection_init" }), {})
      expect(
        (yield* storage.liveConnection(ConnectionId.make("c1"))).context,
      ).toEqual({
        user: "Grace",
        since: since.toISOString(),
      })
      yield* handler(
        wsEvent("c1", "$default", {
          type: "subscribe",
          id: "q",
          payload: { query: "{ me }" },
        }),
        {},
      )
      expect(JSON.parse((yield* (yield* GatewayMessages).all)[1].data)).toEqual(
        { type: "next", id: "q", payload: { data: { me: "Grace" } } },
      )
      yield* subscribe("c1")
      const event = yield* publish(Topic.make("user"), {
        domainId: "a",
        message: "delivered",
      })
      expect(
        yield* PubSub.DynamoDB.streamHandler(settings)(streamEvent(event), {}),
      ).toEqual({ batchItemFailures: [] })
      expect(
        JSON.parse((yield* (yield* GatewayMessages).all).at(-1)!.data).payload
          .data,
      ).toEqual({ message: "delivered" })
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "a stale encoded context is logged and cleaned up without retrying or blocking subscribers",
  () =>
    Effect.gen(function* () {
      for (const id of ["a-stale", "z-valid"]) {
        yield* initialize(id)
        yield* subscribe(id)
      }
      const storage = yield* PubSub.Storage
      yield* storage.update(
        yield* storage.liveConnection(ConnectionId.make("a-stale")),
        {
          context: { user: "Ada", since: "not-a-date" },
        },
      )
      const event = yield* publish(Topic.make("user"), {
        domainId: "a",
        message: "delivered",
      })
      const logs: Array<string> = []
      const result = yield* PubSub.DynamoDB.streamHandler(settings)(
        streamEvent(event),
        {},
      ).pipe(
        Effect.provide(
          Logger.layer([
            Logger.make(({ message }) => {
              logs.push(String(message))
            }),
          ]),
        ),
      )
      expect(result).toEqual({ batchItemFailures: [] })
      expect(yield* (yield* GatewayMessages).closed).toEqual(["a-stale"])
      expect(
        logs.some((line) =>
          line.includes("persisted context no longer decodes"),
        ),
      ).toBe(true)
      expect(
        (yield* storage
          .liveConnection(ConnectionId.make("a-stale"))
          .pipe(Effect.flip))._tag,
      ).toBe("TransportError")
      expect(
        Object.values(yield* (yield* Sandbox).snapshot).some(
          (row) => row.connectionId === "a-stale",
        ),
      ).toBe(false)
      expect(
        (yield* (yield* GatewayMessages).all)
          .slice(2)
          .map((frame) => frame.connectionId),
      ).toEqual(["z-valid"])
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "schema failures stay typed and invalid persisted context closes a WebSocket request",
  () =>
    Effect.gen(function* () {
      const exit = yield* decodePersisted(Persisted, {
        user: "Ada",
        since: "invalid",
      }).pipe(Effect.exit)
      expect(exit._tag).toBe("Failure")
      if (exit._tag === "Failure") {
        expect(Cause.hasDies(exit.cause)).toBe(false)
        expect(Cause.squash(exit.cause)).toMatchObject({
          _tag: "PersistedContextError",
          operation: "decode",
        })
      }
      expect(
        yield* encodePersisted(Persisted, {
          user: "Ada",
          since: new Date(NaN),
        }).pipe(Effect.flip),
      ).toMatchObject({ _tag: "PersistedContextError", operation: "encode" })
      yield* initialize("c1")
      const storage = yield* PubSub.Storage
      yield* storage.update(
        yield* storage.liveConnection(ConnectionId.make("c1")),
        { context: {} },
      )
      expect((yield* subscribe("c1")).statusCode).toBe(500)
      expect(yield* (yield* Sandbox).snapshot).toEqual({})
      expect(yield* (yield* GatewayMessages).closed).toEqual(["c1"])
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "a handshake can accept without context and init supplies the final decoded value",
  () =>
    Effect.gen(function* () {
      const accepted = createWebSocketHandler({
        ...settings,
        onConnect: () => Effect.void,
        onInit: (_payload, previous) => {
          expect(previous).toBeUndefined()
          return Effect.succeed({ user: "Grace", since })
        },
      })
      yield* accepted(wsEvent("c1", "$connect"), {})
      expect(
        (yield* (yield* PubSub.Storage).liveConnection(ConnectionId.make("c1")))
          .context,
      ).toBeUndefined()
      yield* accepted(
        wsEvent("c1", "$default", { type: "connection_init" }),
        {},
      )
      expect(
        (yield* (yield* PubSub.Storage).liveConnection(ConnectionId.make("c1")))
          .context,
      ).toEqual({ user: "Grace", since: since.toISOString() })
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "no hooks initialize an empty public schema, while missing required context cannot subscribe",
  () =>
    Effect.gen(function* () {
      const publicHandler = createWebSocketHandler({
        ...options,
        persistedContext: Schema.Struct({}),
        onInit: undefined,
      })
      yield* publicHandler(wsEvent("public", "$connect"), {})
      yield* publicHandler(
        wsEvent("public", "$default", { type: "connection_init" }),
        {},
      )
      expect(
        (yield* (yield* PubSub.Storage).liveConnection(
          ConnectionId.make("public"),
        )).context,
      ).toEqual({})
      const required = createWebSocketHandler({
        ...settings,
        onConnect: undefined,
        onInit: undefined,
      })
      yield* required(wsEvent("missing", "$connect"), {})
      expect(
        (yield* required(
          wsEvent("missing", "$default", {
            type: "subscribe",
            id: "q",
            payload: { query: "{ me }" },
          }),
          {},
        )).statusCode,
      ).toBe(401)
      yield* required(wsEvent("no-default", "$connect"), {})
      expect(
        (yield* required(
          wsEvent("no-default", "$default", { type: "connection_init" }),
          {},
        )).statusCode,
      ).toBe(500)
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "stale context closure retries DeleteConnection before removing its recovery state",
  () =>
    Effect.gen(function* () {
      yield* initialize("c1")
      yield* subscribe("c1")
      const storage = yield* PubSub.Storage
      const id = ConnectionId.make("c1")
      yield* storage.update(yield* storage.liveConnection(id), { context: {} })
      const event = yield* publish(Topic.make("user"), {
        domainId: "a",
        message: "event",
      })
      const deliver = PubSub.DynamoDB.streamHandler(settings)(
        streamEvent(event, "42"),
        {},
      )
      const gateway = yield* Gateway
      expect(
        yield* deliver.pipe(
          Effect.provideService(Gateway, {
            ...gateway,
            close: () =>
              Effect.fail(
                new GatewayError({
                  operation: "DeleteConnection",
                  gone: false,
                  cause: new Error("Throttled"),
                }),
              ),
          }),
        ),
      ).toEqual({ batchItemFailures: [{ itemIdentifier: "42" }] })
      expect((yield* storage.operation(id, OperationId.make("s")))?.state).toBe(
        "active",
      )
      expect(yield* deliver).toEqual({ batchItemFailures: [] })
      expect(yield* (yield* GatewayMessages).closed).toEqual(["c1"])
      expect(
        yield* storage.operation(id, OperationId.make("s")),
      ).toBeUndefined()
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "heartbeats and client completion do not decode persisted context",
  () =>
    Effect.gen(function* () {
      yield* initialize("c1")
      yield* subscribe("c1")
      const storage = yield* PubSub.Storage
      const id = ConnectionId.make("c1")
      yield* storage.update(yield* storage.liveConnection(id), { context: {} })
      for (const message of [
        { type: "ping", payload: { nonce: 1 } },
        { type: "pong" },
        { type: "complete", id: "s" },
      ])
        expect(
          (yield* handler(wsEvent("c1", "$default", message), {})).statusCode,
        ).toBe(200)
      expect(yield* (yield* GatewayMessages).closed).toEqual([])
      expect(
        (yield* (yield* GatewayMessages).all).map((frame) =>
          JSON.parse(frame.data),
        ),
      ).toEqual([
        { type: "connection_ack" },
        { type: "pong", payload: { nonce: 1 } },
      ])
      expect(
        yield* storage.operation(id, OperationId.make("s")),
      ).toBeUndefined()
    }).pipe(Effect.provide(testLayer())),
)
