import { expect, it } from "@effect/vitest"
import { Sandbox } from "@model-ts/dynamodb/Sandbox"
import { Effect, Stream } from "effect"
import {
  ConnectionId,
  type ContextRequest,
  DeliveryError,
  Gateway,
  GatewayError,
  GatewayMessages,
  Storage,
  StorageError,
  Topic,
  createDirectPublisher,
  publish,
} from "../src/index.ts"
import {
  type LambdaContext,
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
  "delivers inline with filtering and serialization parity, then removes Gone state without ever writing an event",
  () =>
    Effect.gen(function* () {
      const messages = yield* GatewayMessages
      const sandbox = yield* Sandbox
      const direct = createDirectPublisher({
        ...options,
        context: (request: ContextRequest<LambdaContext>) => {
          expect("delivery" in request.event && request.event.delivery).toBe(
            "direct",
          )
          expect(request.lambdaContext.user).toBe("projector")
          return options.context(request)
        },
      })
      yield* connect("c1")
      yield* start("c1")
      const before = yield* sandbox.snapshot
      yield* direct(
        Topic.make("organization"),
        { domainId: "b", message: "skip" },
        { user: "projector" },
      )
      expect(yield* messages.all).toHaveLength(1)
      const event = yield* direct(
        Topic.make("user"),
        {
          domainId: "a",
          message: "delivered",
          date: new Date("2026-01-01T00:00:00Z"),
          omitted: undefined,
        },
        { user: "projector" },
      )
      expect(JSON.parse(event.payload)).toEqual({
        domainId: "a",
        message: "delivered",
        date: "2026-01-01T00:00:00.000Z",
      })
      expect(JSON.parse((yield* messages.all)[1].data)).toEqual({
        type: "data",
        id: "1",
        payload: { data: { message: "delivered" } },
      })
      expect(yield* sandbox.snapshot).toEqual(before)
      yield* messages.markGone(ConnectionId.make("c1"))
      yield* direct(
        Topic.make("user"),
        { domainId: "a", message: "gone" },
        { user: "projector" },
      )
      expect(yield* sandbox.snapshot).toEqual({})
      expect(
        (yield* websocket(wsEvent("c1", "$disconnect"), {})).statusCode,
      ).toBe(200)
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "inline and stream delivery produce identical results after authorization is revoked",
  () =>
    Effect.gen(function* () {
      const direct = createDirectPublisher(options)
      const storage = yield* Storage
      yield* connect("c1")
      yield* start("c1")
      const connection = yield* storage.liveConnection(ConnectionId.make("c1"))
      yield* storage.update(connection, {
        context: { ...connection.context, allowed: false },
      })
      const payload = { domainId: "a", message: "private" }
      yield* direct(Topic.make("user"), payload, {})
      const queued = yield* publish(Topic.make("user"), payload)
      yield* stream(streamEvent(queued), {})
      const frames = (yield* (yield* GatewayMessages).all)
        .slice(1)
        .map((message) => JSON.parse(message.data))
      expect(frames).toHaveLength(2)
      expect(frames[0]).toEqual(frames[1])
      expect(frames[0].payload.errors[0].extensions.code).toBe("FORBIDDEN")
      expect(frames[0].payload.data).toEqual({ message: null })
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "inline failures identify failed subscribers, continue fan-out and retain state for a retry",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      yield* connect("c2")
      yield* start("c1")
      yield* start("c2")
      const direct = createDirectPublisher(options)
      const gateway = yield* Gateway
      const messages = yield* GatewayMessages
      const sandbox = yield* Sandbox
      const before = yield* sandbox.snapshot
      const failure = new GatewayError({
        operation: "PostToConnection",
        gone: false,
        cause: new Error("Throttled"),
      })
      const payload = { domainId: "a", message: "retry" }
      const result = yield* direct(Topic.make("user"), payload, {}).pipe(
        Effect.provideService(Gateway, {
          ...gateway,
          post: (endpoint, id, data) =>
            id === "c1"
              ? Effect.fail(failure)
              : gateway.post(endpoint, id, data),
        }),
        Effect.flip,
      )
      expect(result).toBeInstanceOf(DeliveryError)
      if (!(result instanceof DeliveryError))
        throw new Error("Expected a delivery failure")
      expect(result.failures).toEqual([
        {
          connectionId: "c1",
          operationId: "1",
          cause: failure,
        },
      ])
      expect(
        (yield* messages.all).slice(2).map((message) => message.connectionId),
      ).toEqual(["c2"])
      expect(yield* sandbox.snapshot).toEqual(before)
      yield* direct(Topic.make("user"), payload, {})
      expect(
        (yield* messages.all).slice(2).map((message) => message.connectionId),
      ).toEqual(["c2", "c1", "c2"])
      expect(yield* sandbox.snapshot).toEqual(before)
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "delivery and handlers honor the storage service and preserve backend failures",
  () =>
    Effect.gen(function* () {
      const storage = yield* Storage
      const direct = createDirectPublisher(options)
      const failure = new StorageError({
        operation: "subscribers",
        cause: new Error("store offline"),
      })
      let queries = 0
      const unavailable = {
        ...storage,
        subscribers: () => {
          queries++
          return Stream.fail(failure)
        },
      }
      const invalid = yield* direct(Topic.make("user"), 1n, {}).pipe(
        Effect.provideService(Storage, unavailable),
        Effect.flip,
      )
      expect(invalid._tag).toBe("TransportError")
      expect(queries).toBe(0)
      expect(
        yield* direct(Topic.make("user"), {}, {}).pipe(
          Effect.provideService(Storage, unavailable),
          Effect.flip,
        ),
      ).toBe(failure)
      expect(queries).toBe(1)
      const response = yield* websocket(
        wsEvent("unavailable", "$connect"),
        {},
      ).pipe(
        Effect.provideService(Storage, {
          ...storage,
          create: () => Effect.fail(failure),
        }),
      )
      expect(response.statusCode).toBe(500)
      expect(response.body).not.toContain("store offline")
      expect(yield* (yield* Sandbox).snapshot).toEqual({})
      expect(yield* (yield* GatewayMessages).all).toEqual([])
    }).pipe(Effect.provide(testLayer())),
)
