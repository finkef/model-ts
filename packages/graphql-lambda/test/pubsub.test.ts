import { expect, it } from "@effect/vitest"
import { Model } from "@model-ts/core"
import { DynamoDB } from "@model-ts/dynamodb"
import { Sandbox } from "@model-ts/dynamodb/Sandbox"
import { Context, Effect, Layer, Schema } from "effect"
import {
  ConnectionId,
  EventId,
  Gateway,
  GatewayMessages,
  PubSub,
  Topic,
  createWebSocketHandler,
} from "../src/index.ts"
import { connect, options, publish, streamEvent, wsEvent } from "./fixtures.ts"

class Note extends Model.Class<Note>()("Note", DynamoDB.provider, {
  text: Schema.String,
}) {
  get PK() {
    return "NOTE"
  }
  get SK() {
    return "NOTE"
  }
}

for (const order of ["app-first", "pubsub-first"] as const) {
  it.effect(`merged sandboxes isolate app and PubSub tables (${order})`, () =>
    Effect.gen(function* () {
      const app = yield* Sandbox
      const transport = yield* PubSub.DynamoDB.Sandbox
      expect(app.tableName).not.toBe(transport.tableName)
      expect((yield* DynamoDB).tableName).toBe(app.tableName)
      yield* new Note({ text: "application data" }).put()
      yield* connect("isolated")
      let contexts = 0
      const settings = {
        ...options,
        context: (request: Parameters<typeof options.context>[0]) =>
          Effect.gen(function* () {
            contexts++
            expect((yield* Note.get({ PK: "NOTE", SK: "NOTE" })).text).toBe(
              "application data",
            )
            return yield* options.context(request)
          }),
      }
      const websocket = createWebSocketHandler(settings)
      yield* websocket(
        wsEvent("isolated", "$default", {
          type: "subscribe",
          id: "s",
          payload: { query: 'subscription { message(domain: "a") }' },
        }),
        {},
      )
      const event = yield* publish(Topic.make("user"), {
        domainId: "a",
        message: "announcement",
      })
      expect(
        yield* PubSub.DynamoDB.streamHandler(settings)(streamEvent(event), {}),
      ).toEqual({ batchItemFailures: [] })
      expect(contexts).toBe(2)
      expect(
        JSON.parse((yield* (yield* GatewayMessages).all).at(-1)!.data),
      ).toEqual({
        type: "next",
        id: "s",
        payload: { data: { message: "announcement" } },
      })
      yield* websocket(
        wsEvent("isolated", "$default", { type: "complete", id: "s" }),
        {},
      )
      const connection = yield* (yield* PubSub.Storage).liveConnection(
        ConnectionId.make("isolated"),
      )
      expect(connection.initialized).toBe(true)
      expect((yield* Note.get({ PK: "NOTE", SK: "NOTE" })).text).toBe(
        "application data",
      )
      expect(Object.values(yield* app.snapshot).map((row) => row._tag)).toEqual(
        ["Note"],
      )
      expect(
        Object.values(yield* transport.snapshot)
          .map((row) => row._tag)
          .toSorted(),
      ).toEqual(["GraphQLConnection", "GraphQLPublishedEvent"])
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          ...(order === "app-first"
            ? ([Sandbox.layerMemory(), PubSub.DynamoDB.layerMemory()] as const)
            : ([
                PubSub.DynamoDB.layerMemory(),
                Sandbox.layerMemory(),
              ] as const)),
          Gateway.layerMemory,
        ),
      ),
    ),
  )
}

it.effect("layerFrom does not export any table-layer services", () =>
  Effect.gen(function* () {
    const transport = yield* Layer.build(
      PubSub.DynamoDB.layerFrom(Sandbox.layerMemory()),
    )
    expect(Context.getOption(transport, DynamoDB)._tag).toBe("None")
    expect(Context.getOption(transport, Sandbox)._tag).toBe("None")
    expect(Context.getOption(transport, PubSub.Publisher)._tag).toBe("Some")
    expect(Context.getOption(transport, PubSub.Storage)._tag).toBe("Some")
  }),
)

it.effect(
  "publish delegates to only the backend-agnostic Publisher service",
  () =>
    Effect.gen(function* () {
      const calls: Array<{
        topic: Topic
        payload: Schema.Json
        ttlSeconds?: number
      }> = []
      const id = EventId.make("receipt")
      const result = yield* PubSub.publish(
        Topic.make("notes"),
        { text: "hello" },
        { ttlSeconds: 30 },
      ).pipe(
        Effect.provideService(PubSub.Publisher, {
          publish: (topic, payload, options) =>
            Effect.sync(() => {
              calls.push({ topic, payload, ttlSeconds: options?.ttlSeconds })
              return id
            }),
        }),
      )
      expect(result).toBe(id)
      expect(calls).toEqual([
        { topic: "notes", payload: { text: "hello" }, ttlSeconds: 30 },
      ])
    }),
)
