import { it, expect } from "@effect/vitest"
import { Context, Deferred, Effect, Fiber, Layer, Stream } from "effect"
import { Topic } from "../src/Identifiers.ts"
import { createWebSocketHandler } from "../src/index.ts"
import { streamHandler } from "../src/PubSub/DynamoDB.ts"
import { GatewayMessages, Sandbox } from "../src/Sandbox.ts"
import { publish } from "./fixtures.ts"
import {
  connect,
  options,
  streamEvent,
  testLayer,
  wsEvent,
} from "./fixtures.ts"

class ResolverService extends Context.Service<
  ResolverService,
  { value: string }
>()("TransportTest/ResolverService") {}

it.effect(
  "consumes a native lazy Effect executor, preserving services and closing registration/delivery scopes",
  () =>
    Effect.gen(function* () {
      let acquired = 0
      let released = 0
      const executor = {
        ...options.executor,
        subscribe: (input: Parameters<typeof options.executor.subscribe>[0]) =>
          Stream.unwrap(
            Effect.gen(function* () {
              const service = yield* ResolverService
              yield* Effect.acquireRelease(
                Effect.sync(() => {
                  acquired++
                }),
                () =>
                  Effect.sync(() => {
                    released++
                  }),
              )
              return input.context.pubSub
                .stream(Topic.make("native"))
                .pipe(Stream.map(() => ({ data: { message: service.value } })))
            }),
          ),
      }
      const handler = createWebSocketHandler({ ...options, executor })
      const delivery = streamHandler({ ...options, executor })
      yield* connect("c1")
      yield* handler(
        wsEvent("c1", "$default", {
          type: "subscribe",
          id: "1",
          payload: { query: 'subscription { message(domain: "a") }' },
        }),
        {},
      )
      expect([acquired, released]).toEqual([1, 1])
      expect(
        Object.values(yield* (yield* Sandbox).snapshot).filter(
          (row) => row._tag === "GraphQLSubscription",
        ),
      ).toHaveLength(1)
      yield* delivery(streamEvent(yield* publish(Topic.make("native"), {})), {})
      expect([acquired, released]).toEqual([2, 2])
      expect(
        JSON.parse((yield* (yield* GatewayMessages).all).at(-1)!.data),
      ).toEqual({
        type: "next",
        id: "1",
        payload: { data: { message: "from service" } },
      })
    }).pipe(
      Effect.provide(
        Layer.merge(
          testLayer(),
          Layer.succeed(ResolverService, { value: "from service" }),
        ),
      ),
    ),
)

it.effect(
  "interrupting an invocation closes its subscription Stream scope without storing an operation",
  () =>
    Effect.gen(function* () {
      let closed = false
      const opened = yield* Deferred.make<void>()
      const executor = {
        ...options.executor,
        subscribe: () =>
          Stream.unwrap(
            Effect.gen(function* () {
              yield* Effect.acquireRelease(
                Deferred.succeed(opened, undefined),
                () =>
                  Effect.sync(() => {
                    closed = true
                  }),
              )
              return Stream.never
            }),
          ),
      }
      yield* connect("c1")
      const handler = createWebSocketHandler({ ...options, executor })
      const running = yield* handler(
        wsEvent("c1", "$default", {
          type: "subscribe",
          id: "1",
          payload: { query: 'subscription { message(domain: "a") }' },
        }),
        {},
      ).pipe(Effect.forkChild)
      yield* Deferred.await(opened)
      yield* Fiber.interrupt(running)
      expect(closed).toBe(true)
      expect(
        Object.values(yield* (yield* Sandbox).snapshot).map((row) => row._tag),
      ).toEqual(["GraphQLConnection"])
    }).pipe(Effect.provide(testLayer())),
)
