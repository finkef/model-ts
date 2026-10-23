import { expect, it } from "@effect/vitest"
import { Deferred, Effect, Fiber, Tracer } from "effect"
import { Topic, createWebSocketHandler } from "../src/index.ts"
import { GatewayMessages, Sandbox } from "../src/Sandbox.ts"
import {
  publish,
  stream,
  streamEvent,
  connect,
  options,
  start,
  testLayer,
  websocket,
  wsEvent,
} from "./fixtures.ts"

it.effect(
  "requires the selected subprotocol and records the intended close code",
  () =>
    Effect.gen(function* () {
      const spans: Tracer.NativeSpan[] = []
      const tracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options)
          spans.push(span)
          return span
        },
      })
      for (const protocol of [undefined, "other", "GRAPHQL-TRANSPORT-WS"]) {
        const event = {
          ...wsEvent("handshake", "$connect"),
          headers: { "Sec-WebSocket-Protocol": protocol },
        }
        expect(
          (yield* websocket(event, {}).pipe(Effect.withTracer(tracer)))
            .statusCode,
        ).toBe(400)
        expect(yield* (yield* Sandbox).snapshot).toEqual({})
      }
      expect(yield* (yield* GatewayMessages).closed).toEqual([
        "handshake",
        "handshake",
        "handshake",
      ])
      expect(
        spans
          .filter((span) => span.name === "GraphQL.close")
          .map((span) => span.attributes.get("graphql.close.code")),
      ).toEqual([4406, 4406, 4406])
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "echoes ping payloads before init, ignores pong, and supports an ack payload",
  () =>
    Effect.gen(function* () {
      const handler = createWebSocketHandler({
        ...options,
        acknowledgementPayload: { ready: true },
      })
      yield* handler(wsEvent("c1", "$connect"), {})
      for (const payload of [undefined, null, { nonce: 12 }]) {
        yield* handler(
          wsEvent("c1", "$default", {
            type: "ping",
            ...(payload === undefined ? {} : { payload }),
          }),
          {},
        )
        yield* handler(wsEvent("c1", "$default", { type: "pong", payload }), {})
      }
      yield* handler(
        wsEvent("c1", "$default", {
          type: "connection_init",
          payload: { token: "valid" },
        }),
        {},
      )
      expect(
        (yield* (yield* GatewayMessages).all).map((frame) =>
          JSON.parse(frame.data),
        ),
      ).toEqual([
        { type: "pong" },
        { type: "pong", payload: null },
        { type: "pong", payload: { nonce: 12 } },
        { type: "connection_ack", payload: { ready: true } },
      ])
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "a repeated init closes an initialized connection without acknowledging again",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      yield* start("c1")
      yield* websocket(
        wsEvent("c1", "$default", { type: "connection_init" }),
        {},
      )
      expect(yield* (yield* Sandbox).snapshot).toEqual({})
      expect(yield* (yield* GatewayMessages).closed).toEqual(["c1"])
      expect(
        (yield* (yield* GatewayMessages).all).map((frame) =>
          JSON.parse(frame.data),
        ),
      ).toEqual([{ type: "connection_ack" }])
    }).pipe(Effect.provide(testLayer())),
)

for (const second of ["connection_init", "subscribe"] as const) {
  it.effect(
    `${second} during init closes without waiting and pending auth cannot resurrect state`,
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        let calls = 0
        const handler = createWebSocketHandler({
          ...options,
          onInit: () =>
            Effect.gen(function* () {
              calls++
              yield* Deferred.succeed(entered, undefined)
              yield* Deferred.await(release)
              return { user: "Ada" }
            }),
        })
        yield* handler(wsEvent("c1", "$connect"), {})
        const pending = yield* handler(
          wsEvent("c1", "$default", { type: "connection_init" }),
          {},
        ).pipe(Effect.forkChild)
        yield* Deferred.await(entered)
        yield* handler(
          wsEvent("c1", "$default", {
            type: second,
            id: "1",
            payload: { query: "{ me }" },
          }),
          {},
        )
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(pending)
        expect(calls).toBe(1)
        expect(yield* (yield* Sandbox).snapshot).toEqual({})
        expect(yield* (yield* GatewayMessages).all).toEqual([])
        expect(yield* (yield* GatewayMessages).closed).toContain("c1")
      }).pipe(Effect.provide(testLayer())),
  )
}

it.effect("duplicate subscription IDs close and remove all operations", () =>
  Effect.gen(function* () {
    yield* connect("c1")
    yield* start("c1")
    yield* start("c1", "2")
    yield* start("c1")
    expect(yield* (yield* Sandbox).snapshot).toEqual({})
    expect(yield* (yield* GatewayMessages).closed).toEqual(["c1"])
    expect(
      (yield* (yield* GatewayMessages).all).map((frame) =>
        JSON.parse(frame.data),
      ),
    ).toEqual([{ type: "connection_ack" }])
  }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "request errors terminate with arrays and client completion is silent for unknown IDs",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      for (const query of [
        "{",
        "{ missing }",
        'subscription { a:message(domain:"a") b:message(domain:"a") }',
      ]) {
        yield* websocket(
          wsEvent("c1", "$default", {
            type: "subscribe",
            id: "1",
            payload: { query },
          }),
          {},
        )
        const frame = JSON.parse(
          (yield* (yield* GatewayMessages).all).at(-1)!.data,
        )
        expect(frame.type).toBe("error")
        expect(frame.id).toBe("1")
        expect(Array.isArray(frame.payload)).toBe(true)
      }
      yield* websocket(
        wsEvent("c1", "$default", { type: "complete", id: "missing" }),
        {},
      )
      expect(yield* (yield* GatewayMessages).all).toHaveLength(4)
      expect(yield* (yield* GatewayMessages).closed).toEqual([])
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "duplicate IDs close a pending query and suppress its eventual result",
  () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const handler = createWebSocketHandler({
        ...options,
        executor: {
          ...options.executor,
          execute: () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined)
              yield* Deferred.await(release)
              return { data: { me: "Ada" } }
            }),
        },
      })
      yield* connect("c1")
      const request = wsEvent("c1", "$default", {
        type: "subscribe",
        id: "query",
        payload: { query: "{ me }" },
      })
      const pending = yield* handler(request, {}).pipe(Effect.forkChild)
      yield* Deferred.await(entered)
      yield* handler(request, {})
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(pending)
      expect(yield* (yield* Sandbox).snapshot).toEqual({})
      expect(yield* (yield* GatewayMessages).closed).toEqual(["c1"])
      expect(
        (yield* (yield* GatewayMessages).all).map((frame) =>
          JSON.parse(frame.data),
        ),
      ).toEqual([{ type: "connection_ack" }])
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "long operation IDs survive storage, delivery, completion and reuse",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      const ids = [
        "x".repeat(129),
        "é\u0000".repeat(600),
        "é\u0000".repeat(600) + "different",
      ]
      for (const id of ids) yield* start("c1", id)
      const rows = Object.values(yield* (yield* Sandbox).snapshot)
      expect(
        rows.filter((row) => row._tag === "GraphQLSubscriptionOperation"),
      ).toHaveLength(3)
      expect(
        rows.every(
          (row) =>
            typeof row.SK === "string" && Buffer.byteLength(row.SK) <= 1024,
        ),
      ).toBe(true)
      const event = yield* publish(Topic.make("user"), {
        domainId: "a",
        message: "hello",
      })
      expect(yield* stream(streamEvent(event), {})).toEqual({
        batchItemFailures: [],
      })
      const frames = (yield* (yield* GatewayMessages).all)
        .slice(1)
        .map((frame) => JSON.parse(frame.data))
      expect(frames.map((frame) => frame.id).toSorted()).toEqual(ids.toSorted())
      expect(
        frames.every(
          (frame) =>
            frame.type === "next" && frame.payload.data.message === "hello",
        ),
      ).toBe(true)
      for (const id of ids)
        yield* websocket(
          wsEvent("c1", "$default", { type: "complete", id }),
          {},
        )
      for (const id of ids) yield* start("c1", id)
      expect(yield* (yield* GatewayMessages).closed).toEqual([])
      yield* websocket(wsEvent("c1", "$disconnect"), {})
      expect(
        Object.values(yield* (yield* Sandbox).snapshot).map((row) => row._tag),
      ).toEqual(["GraphQLPublishedEvent"])
    }).pipe(Effect.provide(testLayer())),
)
