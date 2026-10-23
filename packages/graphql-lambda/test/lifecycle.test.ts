import { it, expect } from "@effect/vitest"
import { Sandbox } from "@model-ts/dynamodb/Sandbox"
import { Effect } from "effect"
import {
  ConnectionId,
  OperationId,
  EventId,
  Topic,
} from "../src/Identifiers.ts"
import {
  Connection,
  PublishedEvent,
  GatewayMessages,
  SubscriptionOperation,
  createWebSocketHandler,
  publish,
  keepAlive,
} from "../src/index.ts"
import {
  connect,
  options,
  start,
  stream,
  streamEvent,
  testLayer,
  websocket,
  wsEvent,
} from "./fixtures.ts"

it.effect(
  "connect → authenticated init → multi-topic subscription → filtered delivery → gone cleanup → disconnect",
  () =>
    Effect.gen(function* () {
      const messages = yield* GatewayMessages
      const sandbox = yield* Sandbox
      const upgrade = yield* websocket(wsEvent("c1", "$connect"), {})
      expect(upgrade.headers).toEqual({
        "Sec-WebSocket-Protocol": "graphql-ws",
      })
      yield* websocket(
        wsEvent("c1", "$default", {
          type: "connection_init",
          payload: { token: "valid" },
        }),
        {},
      )
      yield* start("c1")
      expect(
        (yield* messages.all).map((message) => JSON.parse(message.data)),
      ).toEqual([{ type: "connection_ack" }])
      expect(
        (yield* SubscriptionOperation.get(
          SubscriptionOperation.key(
            ConnectionId.make("c1"),
            OperationId.make("1"),
          ),
        )).topics,
      ).toEqual(["user", "organization"])
      const skipped = yield* publish(Topic.make("organization"), {
        domainId: "b",
        message: "skip",
      })
      expect(yield* stream(streamEvent(skipped), {})).toEqual({
        batchItemFailures: [],
      })
      expect(yield* messages.all).toHaveLength(1)
      const sent = yield* publish(Topic.make("user"), {
        domainId: "a",
        message: "delivered",
        date: new Date("2026-01-01T00:00:00Z"),
      })
      expect(sent.payload).toContain('"date":"2026-01-01T00:00:00.000Z"')
      yield* stream(streamEvent(sent), {})
      expect(JSON.parse((yield* messages.all)[1].data)).toEqual({
        type: "data",
        id: "1",
        payload: { data: { message: "delivered" } },
      })
      yield* messages.markGone(ConnectionId.make("c1"))
      yield* stream(streamEvent(sent), {})
      const rows = Object.values(yield* sandbox.snapshot)
      expect(rows.map((row) => row._tag)).toEqual([
        "GraphQLPublishedEvent",
        "GraphQLPublishedEvent",
      ])
      expect(
        (yield* websocket(wsEvent("c1", "$disconnect"), {})).statusCode,
      ).toBe(200)
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "documents four-role single-table storage and aligned TTL without indexes",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      yield* start("c1")
      const sandbox = yield* Sandbox
      yield* sandbox.seed(
        new PublishedEvent({
          id: EventId.make("event-1"),
          topic: Topic.make("user"),
          payload: '{"message":"example"}',
          ttl: 7200,
        }),
      )
      const snapshot = yield* sandbox.snapshot
      expect(
        Object.values(snapshot)
          .map((row) => ({
            PK: row.PK,
            SK: row.SK,
            tag: row._tag,
            ttl: row.ttl,
          }))
          .toSorted((a, b) => `${a.PK}${a.SK}`.localeCompare(`${b.PK}${b.SK}`)),
      ).toMatchSnapshot()
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "rejects init authentication, closes the socket and removes all state",
  () =>
    Effect.gen(function* () {
      const messages = yield* GatewayMessages
      const sandbox = yield* Sandbox
      yield* websocket(wsEvent("c1", "$connect"), {})
      const response = yield* websocket(
        wsEvent("c1", "$default", {
          type: "connection_init",
          payload: { token: "invalid" },
        }),
        {},
      )
      expect(response.statusCode).toBe(401)
      expect(yield* messages.closed).toEqual(["c1"])
      expect(yield* sandbox.snapshot).toEqual({})
      expect(JSON.parse((yield* messages.all)[0].data)).toMatchObject({
        type: "error",
        payload: { extensions: { code: "UNAUTHORIZED" } },
      })
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "upgrade authentication can persist context or reject before initialization",
  () =>
    Effect.gen(function* () {
      const handler = createWebSocketHandler({
        ...options,
        onWebsocketConnect: () => Effect.succeed(false as const),
      })
      expect((yield* handler(wsEvent("c1", "$connect"), {})).statusCode).toBe(
        401,
      )
      expect(yield* (yield* Sandbox).snapshot).toEqual({})
      const accepted = createWebSocketHandler({
        ...options,
        onWebsocketConnect: () => Effect.succeed({ origin: "trusted" }),
      })
      yield* accepted(wsEvent("c2", "$connect"), {})
      yield* accepted(
        wsEvent("c2", "$default", {
          type: "connection_init",
          payload: { token: "valid" },
        }),
        {},
      )
      expect(
        (yield* Connection.get(Connection.key(ConnectionId.make("c2"))))
          .context,
      ).toEqual({
        origin: "trusted",
        user: "ada",
        allowed: true,
      })
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "rebuilds context and reauthorizes delivery after access is revoked",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      yield* start("c1")
      const connection = yield* Connection.get(
        Connection.key(ConnectionId.make("c1")),
      )
      yield* connection.update({
        context: { ...connection.context, allowed: false },
      })
      yield* stream(
        streamEvent(
          yield* publish(Topic.make("user"), {
            domainId: "a",
            message: "private",
          }),
        ),
        {},
      )
      const messages = yield* (yield* GatewayMessages).all
      const frame = JSON.parse(messages[1].data)
      expect(frame.payload.errors[0].extensions.code).toBe("FORBIDDEN")
      expect(frame.payload.data).toEqual({ message: null })
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "repeat start is idempotent, stop removes every topic, and WS queries complete",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      yield* start("c1")
      yield* start("c1")
      expect(Object.values(yield* (yield* Sandbox).snapshot)).toHaveLength(4)
      yield* websocket(wsEvent("c1", "$default", { type: "stop", id: "1" }), {})
      expect(Object.values(yield* (yield* Sandbox).snapshot)).toHaveLength(1)
      yield* websocket(
        wsEvent("c1", "$default", {
          type: "start",
          id: "q",
          payload: { query: "{ me }" },
        }),
        {},
      )
      expect(
        (yield* (yield* GatewayMessages).all)
          .slice(-2)
          .map((message) => JSON.parse(message.data)),
      ).toEqual([
        { type: "data", id: "q", payload: { data: { me: "ada" } } },
        { type: "complete", id: "q" },
      ])
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "enforces operation limit and exposes scheduler-driven keepalive",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      yield* start("c1")
      const limited = createWebSocketHandler({ ...options, maxOperations: 1 })
      yield* limited(
        wsEvent("c1", "$default", {
          type: "start",
          id: "2",
          payload: { query: 'subscription { message(domain: "a") }' },
        }),
        {},
      )
      const messages = yield* GatewayMessages
      expect(JSON.parse((yield* messages.all).at(-1)!.data)).toMatchObject({
        type: "error",
        id: "2",
      })
      yield* keepAlive(ConnectionId.make("c1"))
      expect(JSON.parse((yield* messages.all).at(-1)!.data)).toEqual({
        type: "ka",
      })
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "malformed messages are rejected and uninitialized clients are closed",
  () =>
    Effect.gen(function* () {
      yield* websocket(wsEvent("c1", "$connect"), {})
      for (const message of [
        null,
        {},
        { type: "subscribe" },
        { type: "start" },
      ])
        expect(
          (yield* websocket(wsEvent("c1", "$default", message), {})).statusCode,
        ).toBe(400)
      const handler = createWebSocketHandler({ ...options, initRetries: 0 })
      expect(
        (yield* handler(
          wsEvent("c1", "$default", {
            type: "start",
            id: "1",
            payload: { query: "{ me }" },
          }),
          {},
        )).statusCode,
      ).toBe(401)
      expect(yield* (yield* GatewayMessages).closed).toEqual(["c1"])
    }).pipe(Effect.provide(testLayer())),
)
