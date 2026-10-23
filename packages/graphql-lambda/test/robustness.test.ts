import { marshall } from "@aws-sdk/util-dynamodb"
import { it, expect } from "@effect/vitest"
import { Cause, Effect, Layer, Stream } from "effect"
import { TestClock } from "effect/testing"
import { GraphQLError } from "graphql"
import {
  ConnectionId,
  OperationId,
  OperationGeneration,
  EventId,
  Topic,
} from "../src/Identifiers.ts"
import {
  Gateway,
  GatewayError,
  GatewayMessages,
  PublishedEvent,
  Subscription,
  SubscriptionOperation,
  TransportError,
  createWebSocketHandler,
} from "../src/index.ts"
import { operationPubSub } from "../src/internal/source.ts"
import { withFilter } from "../src/PubSub.ts"
import { Sandbox } from "../src/PubSub/DynamoDB.ts"
import { streamHandler } from "../src/PubSub/DynamoDB.ts"
import { Storage } from "../src/Storage.ts"
import { loadOperation } from "./fixtures.ts"
import { publish } from "./fixtures.ts"
import {
  connect,
  register,
  options,
  start,
  stream,
  streamEvent,
  testLayer,
  websocket,
  wsEvent,
} from "./fixtures.ts"

it.effect("concurrent starts cannot exceed the operation limit", () =>
  Effect.gen(function* () {
    yield* connect("race")
    const handler = createWebSocketHandler({ ...options, maxOperations: 1 })
    yield* Effect.forEach(
      ["one", "two"],
      (id) =>
        handler(
          wsEvent("race", "$default", {
            type: "subscribe",
            id,
            payload: { query: 'subscription { message(domain: "a") }' },
          }),
          {},
        ),
      { concurrency: 2 },
    )
    const rows = Object.values(yield* (yield* Sandbox).snapshot)
    expect(
      rows.filter((row) => row._tag === "GraphQLSubscriptionOperation"),
    ).toHaveLength(1)
    expect(
      rows.filter((row) => row._tag === "GraphQLSubscription"),
    ).toHaveLength(2)
    expect(
      (yield* (yield* GatewayMessages).all)
        .map((message) => JSON.parse(message.data).type)
        .toSorted(),
    ).toEqual(["connection_ack", "error"])
  }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "completed IDs can be reused with different topics and keys cannot collide",
  () =>
    Effect.gen(function* () {
      yield* connect("a:b")
      yield* connect("a")
      const request = { query: 'subscription { message(domain: "a") }' }
      yield* register(
        ConnectionId.make("a:b"),
        OperationId.make("c"),
        request,
        ["first", "second"].map((name) => Topic.make(name)),
        10,
      )
      yield* register(
        ConnectionId.make("a"),
        OperationId.make("b:c"),
        request,
        ["first"].map((name) => Topic.make(name)),
        10,
      )
      yield* (yield* Storage).stop(
        ConnectionId.make("a:b"),
        OperationId.make("c"),
      )
      yield* register(
        ConnectionId.make("a:b"),
        OperationId.make("c"),
        request,
        ["third"].map((name) => Topic.make(name)),
        10,
      )
      expect(
        yield* (yield* Storage)
          .subscribers(Topic.make("second"), 0)
          .pipe(Stream.runCollect),
      ).toEqual([])
      expect(
        (yield* (yield* Storage)
          .subscribers(Topic.make("first"), 0)
          .pipe(Stream.runCollect)).map((sub) => sub.connectionId),
      ).toEqual(["a"])
      yield* (yield* Storage).stop(
        ConnectionId.make("a:b"),
        OperationId.make("c"),
      )
      expect(
        (yield* loadOperation(ConnectionId.make("a"), OperationId.make("b:c")))
          .topics,
      ).toEqual(["first"])
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "disconnect cleans up a missing/expired connection and never resurrects it",
  () =>
    Effect.gen(function* () {
      yield* (yield* Sandbox).seed(
        new SubscriptionOperation({
          connectionId: ConnectionId.make("expired"),
          operationId: OperationId.make("1"),
          generation: OperationGeneration.make("orphan"),
          request: { query: "{ me }" },
          state: "active",
          topics: [Topic.make("user")],
          ttl: 7200,
        }),
        new Subscription({
          connectionId: ConnectionId.make("expired"),
          operationId: OperationId.make("1"),
          topic: Topic.make("user"),
          ttl: 7200,
        }),
      )
      yield* (yield* Storage).disconnect(ConnectionId.make("expired"))
      expect(yield* (yield* Sandbox).snapshot).toEqual({})
      const attempt = yield* register(
        ConnectionId.make("expired"),
        OperationId.make("1"),
        { query: "{ me }" },
        ["user"].map((name) => Topic.make(name)),
        10,
      ).pipe(Effect.result)
      expect(attempt._tag).toBe("Failure")
      expect(yield* (yield* Sandbox).snapshot).toEqual({})
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "hides expired subscriptions before DynamoDB TTL deletion and ignores expired events",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      yield* TestClock.adjust("1 hour")
      yield* start("c1")
      expect(
        (yield* loadOperation(ConnectionId.make("c1"), OperationId.make("1")))
          .ttl,
      ).toBe(7200)
      const event = yield* publish(
        Topic.make("user"),
        { domainId: "a", message: "old" },
        1,
      )
      yield* TestClock.adjust("1 second")
      yield* stream(streamEvent(event), {})
      expect(yield* (yield* GatewayMessages).all).toHaveLength(1)
      yield* TestClock.adjust("1 hour")
      expect(
        yield* (yield* Storage)
          .subscribers(Topic.make("user"), 7_201_000)
          .pipe(Stream.runCollect),
      ).toEqual([])
      expect(
        (yield* websocket(wsEvent("c1", "$disconnect"), {})).statusCode,
      ).toBe(200)
      expect(
        Object.values(yield* (yield* Sandbox).snapshot).map((row) => row._tag),
      ).toEqual(["GraphQLPublishedEvent"])
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "subscriber pagination traverses expired pages and more than fifty live memberships",
  () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      const rows = Array.from(
        { length: 103 },
        (_, i) =>
          new Subscription({
            topic: Topic.make("page"),
            connectionId: ConnectionId.make(String(i).padStart(3, "0")),
            operationId: OperationId.make("1"),
            ttl: i < 51 ? 1 : 7200,
          }),
      )
      yield* sandbox.seed(...rows)
      const found = yield* (yield* Storage)
        .subscribers(Topic.make("page"), 1000)
        .pipe(Stream.runCollect)
      expect(found).toHaveLength(52)
      expect(found[0].connectionId).toBe("051")
      expect(found.at(-1)?.connectionId).toBe("102")
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "non-Gone send failure returns a stream retry identifier and preserves subscriptions",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      yield* start("c1")
      const event = yield* publish(Topic.make("user"), {
        domainId: "a",
        message: "retry",
      })
      const failureLayer = Layer.succeed(
        Gateway,
        Gateway.of({
          post: () =>
            Effect.fail(
              new GatewayError({
                operation: "PostToConnection",
                gone: false,
                cause: new Error("Throttled"),
              }),
            ),
          close: () => Effect.void,
        }),
      )
      expect(
        yield* stream(streamEvent(event, "42"), {}).pipe(
          Effect.provide(failureLayer),
        ),
      ).toEqual({ batchItemFailures: [{ itemIdentifier: "42" }] })
      expect(
        (yield* loadOperation(ConnectionId.make("c1"), OperationId.make("1")))
          .topics,
      ).toHaveLength(2)
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "stream ignores non-events, reports corrupt event records, and continues the batch",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      yield* start("c1")
      const event = yield* publish(Topic.make("user"), {
        domainId: "a",
        message: "valid",
      })
      const records = [
        { eventName: "REMOVE", dynamodb: { SequenceNumber: "1" } },
        {
          eventName: "INSERT",
          dynamodb: {
            NewImage: marshall({ _tag: "OtherModel" }),
            SequenceNumber: "2",
          },
        },
        {
          eventName: "INSERT",
          dynamodb: {
            NewImage: marshall({ _tag: "GraphQLPublishedEvent" }),
            SequenceNumber: "3",
          },
        },
        ...streamEvent(event, "4").Records,
      ]
      expect(yield* stream({ Records: records }, {})).toEqual({
        batchItemFailures: [{ itemIdentifier: "3" }],
      })
      expect(yield* (yield* GatewayMessages).all).toHaveLength(2)
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "delivery formats typed context failures and uses the same validation rules",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      yield* start("c1")
      const handler = streamHandler({
        ...options,
        context: () =>
          Effect.fail(
            new TransportError({
              message: "Session revoked",
              code: "UNAUTHORIZED",
              status: 401,
            }),
          ),
      })
      const event = yield* publish(Topic.make("user"), {
        domainId: "a",
        message: "private",
      })
      expect(yield* handler(streamEvent(event), {})).toEqual({
        batchItemFailures: [],
      })
      expect(
        JSON.parse((yield* (yield* GatewayMessages).all).at(-1)!.data)
          .payload[0],
      ).toEqual({
        message: "Session revoked",
        extensions: { code: "UNAUTHORIZED" },
      })
    }).pipe(Effect.provide(testLayer())),
)

it.effect("publish refuses invalid JSON and empty topics before writing", () =>
  Effect.gen(function* () {
    const circular = { self: {} }
    circular.self = circular
    for (const payload of [undefined, 1n, circular])
      expect(
        // @ts-expect-error Runtime callers can bypass the JSON payload boundary.
        (yield* publish(Topic.make("topic"), payload).pipe(Effect.result))._tag,
      ).toBe("Failure")
    expect((yield* publish(Topic.make(""), {}).pipe(Effect.result))._tag).toBe(
      "Failure",
    )
    expect(yield* (yield* Sandbox).snapshot).toEqual({})
  }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "Effect subscription sources collect topics and finite filtering closes the source",
  () =>
    Effect.gen(function* () {
      const source = operationPubSub({
        topic: Topic.make("topic"),
        payload: "hello",
      })
      expect(
        yield* source.pubSub
          .stream(["topic", "other"].map((name) => Topic.make(name)))
          .pipe(Stream.runCollect),
      ).toEqual(["hello"])
      expect([...source.topics]).toEqual(["topic", "other"])
      let closed = false
      async function* values() {
        try {
          yield 1
          yield 2
        } finally {
          closed = true
        }
      }
      const iterator = withFilter(values(), (value) => value === 2)
      expect((yield* Effect.promise(() => iterator.next())).value).toBe(2)
      yield* Effect.promise(() => iterator.return(undefined))
      expect(closed).toBe(true)
    }),
)

it.effect(
  "invalid message types close immediately and expired event inserts are ignored",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      yield* start("c1")
      yield* websocket(wsEvent("c1", "$default", { type: "unsupported" }), {})
      expect(yield* (yield* Sandbox).snapshot).toEqual({})
      expect(yield* (yield* GatewayMessages).closed).toEqual(["c1"])
      const expired = new PublishedEvent({
        id: EventId.make("expired"),
        topic: Topic.make("user"),
        payload: "{}",
        ttl: 0,
      })
      expect(yield* stream(streamEvent(expired), {})).toEqual({
        batchItemFailures: [],
      })
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "delivery applies custom validation rules before rebuilding application context",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      yield* start("c1")
      const handler = streamHandler({
        ...options,
        validationRules: [
          (context) => ({
            Field(node) {
              if (node.name.value === "message")
                context.reportError(
                  new GraphQLError("Field disabled", {
                    nodes: [node],
                    originalError: new TransportError({
                      message: "Field disabled",
                      code: "FORBIDDEN",
                      status: 403,
                    }),
                  }),
                )
            },
          }),
        ],
        context: () => Effect.die("validation must run first"),
      })
      const event = yield* publish(Topic.make("user"), {
        domainId: "a",
        message: "private",
      })
      expect(yield* handler(streamEvent(event), {})).toEqual({
        batchItemFailures: [],
      })
      expect(
        JSON.parse((yield* (yield* GatewayMessages).all).at(-1)!.data)
          .payload[0].message,
      ).toBe("Field disabled")
    }).pipe(Effect.provide(testLayer())),
)

it.effect("disconnect racing registration leaves no orphaned operations", () =>
  Effect.gen(function* () {
    yield* connect("race")
    yield* Effect.all(
      [
        register(
          ConnectionId.make("race"),
          OperationId.make("1"),
          { query: "{ me }" },
          ["topic"].map((name) => Topic.make(name)),
          10,
        ).pipe(Effect.result),
        (yield* Storage).disconnect(ConnectionId.make("race")),
      ],
      { concurrency: 2 },
    )
    expect(yield* (yield* Sandbox).snapshot).toEqual({})
  }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "record defects report their sequence and do not abort later records",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      yield* start("c1")
      const event = yield* publish(Topic.make("user"), {
        domainId: "a",
        message: "hello",
      })
      const storage = yield* Storage
      const defect = new Error("subscriber lookup defect")
      let calls = 0
      const result = yield* stream(
        {
          Records: [
            ...streamEvent(event, "41").Records,
            ...streamEvent(event, "42").Records,
          ],
        },
        {},
      ).pipe(
        Effect.provideService(Storage, {
          ...storage,
          subscribers: (...args) =>
            ++calls === 1 ? Stream.die(defect) : storage.subscribers(...args),
        }),
      )
      expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: "41" }] })
      expect(
        (yield* (yield* GatewayMessages).all).map((frame) =>
          JSON.parse(frame.data),
        ),
      ).toEqual([
        { type: "connection_ack" },
        { type: "next", id: "1", payload: { data: { message: "hello" } } },
      ])
      const exit = yield* stream(
        {
          Records: [
            {
              ...streamEvent(event).Records[0],
              dynamodb: {
                NewImage: streamEvent(event).Records[0].dynamodb?.NewImage,
              },
            },
          ],
        },
        {},
      ).pipe(
        Effect.provideService(Storage, {
          ...storage,
          subscribers: () => Stream.die(defect),
        }),
        Effect.exit,
      )
      expect(exit._tag).toBe("Failure")
      if (exit._tag === "Failure") expect(Cause.squash(exit.cause)).toBe(defect)
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "delivery closes IDs too large for even a terminal frame without poisoning retries",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      yield* start("c1", "x".repeat(33 * 1024))
      const event = yield* publish(Topic.make("user"), {
        domainId: "a",
        message: "hello",
      })
      expect(yield* stream(streamEvent(event), {})).toEqual({
        batchItemFailures: [],
      })
      expect(yield* (yield* GatewayMessages).closed).toEqual(["c1"])
      expect(
        Object.values(yield* (yield* Sandbox).snapshot).map((row) => row._tag),
      ).toEqual(["GraphQLPublishedEvent"])
    }).pipe(Effect.provide(testLayer())),
)
