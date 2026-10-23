import { expect, it } from "@effect/vitest"
import { Sandbox } from "@model-ts/dynamodb/Sandbox"
import { Deferred, Effect, Fiber, Stream } from "effect"
import {
  ConnectionId,
  OperationId,
  Storage,
  Topic,
  GatewayMessages,
  createWebSocketHandler,
  type ContextRequest,
} from "../src/index.ts"
import {
  connect,
  options,
  testLayer,
  wsEvent,
  type LambdaContext,
} from "./fixtures.ts"

for (const phase of ["context", "subscribe"] as const) {
  for (const action of ["stop", "replace"] as const) {
    it.effect(
      `a pending ${phase} cannot resurrect an operation after ${action}`,
      () =>
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          let calls = 0
          const gate = Effect.gen(function* () {
            if (++calls === 1) {
              yield* Deferred.succeed(entered, undefined)
              yield* Deferred.await(release)
            }
          })
          const handler = createWebSocketHandler({
            ...options,
            context: (request: ContextRequest<LambdaContext>) =>
              (phase === "context" ? gate : Effect.void).pipe(
                Effect.andThen(options.context(request)),
              ),
            executor: {
              ...options.executor,
              subscribe: (
                input: Parameters<typeof options.executor.subscribe>[0],
              ) =>
                Stream.unwrap(
                  (phase === "subscribe" ? gate : Effect.void).pipe(
                    Effect.map(() =>
                      input.context.pubSub
                        .stream(
                          Topic.make(String(input.request.variables?.domain)),
                        )
                        .pipe(
                          Stream.map(() => ({
                            data: { message: "delivered" },
                          })),
                        ),
                    ),
                  ),
                ),
            },
          })
          const start = (domain: string) =>
            handler(
              wsEvent("c1", "$default", {
                type: "start",
                id: "1",
                payload: {
                  query:
                    "subscription ($domain: String!) { message(domain: $domain) }",
                  variables: { domain },
                },
              }),
              {},
            )
          yield* connect("c1")
          const pending = yield* start("old-topic").pipe(Effect.forkChild)
          yield* Deferred.await(entered)
          const storage = yield* Storage
          const pendingOperation = yield* storage.operation(
            ConnectionId.make("c1"),
            OperationId.make("1"),
          )
          expect(pendingOperation?.state).toBe("pending")
          expect(pendingOperation?.topics).toEqual([])
          if (action === "stop") {
            yield* handler(
              wsEvent("c1", "$default", { type: "stop", id: "1" }),
              {},
            )
          } else {
            yield* start("new-topic")
          }
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(pending)
          const operation = yield* storage.operation(
            ConnectionId.make("c1"),
            OperationId.make("1"),
          )
          expect(
            yield* storage
              .subscribers(Topic.make("old-topic"), 0)
              .pipe(Stream.runCollect),
          ).toEqual([])
          const frames = (yield* (yield* GatewayMessages).all).map((frame) =>
            JSON.parse(frame.data),
          )
          if (action === "stop") {
            expect(operation).toBeUndefined()
            expect(
              Object.values(yield* (yield* Sandbox).snapshot),
            ).toHaveLength(1)
            expect(frames).toEqual([
              { type: "connection_ack" },
              { type: "complete", id: "1" },
            ])
          } else {
            expect(operation?.state).toBe("active")
            expect(operation?.topics).toEqual(["new-topic"])
            expect(operation?.generation).not.toBe(pendingOperation?.generation)
            expect(
              yield* storage
                .subscribers(Topic.make("new-topic"), 0)
                .pipe(Stream.runCollect),
            ).toHaveLength(1)
            expect(frames).toEqual([{ type: "connection_ack" }])
          }
        }).pipe(Effect.provide(testLayer())),
    )
  }
}

it.effect(
  "replacement pauses delivery and changes topic membership only at conditional activation",
  () =>
    Effect.gen(function* () {
      yield* connect("c1")
      const storage = yield* Storage
      const id = ConnectionId.make("c1")
      const operation = OperationId.make("1")
      const request = { query: 'subscription { message(domain: "a") }' }
      const first = yield* storage.reserve(id, operation, request, 10)
      yield* storage.activate(id, operation, first, [Topic.make("old")])
      const replacement = yield* storage.reserve(id, operation, request, 10)
      expect((yield* storage.operation(id, operation))?.state).toBe("pending")
      expect(
        yield* storage
          .subscribers(Topic.make("old"), 0)
          .pipe(Stream.runCollect),
      ).toHaveLength(1)
      expect(
        yield* storage
          .subscribers(Topic.make("new"), 0)
          .pipe(Stream.runCollect),
      ).toEqual([])
      expect(
        yield* storage.activate(id, operation, first, [Topic.make("stale")]),
      ).toBe(false)
      expect(
        yield* storage.activate(id, operation, replacement, [
          Topic.make("new"),
        ]),
      ).toBe(true)
      expect(
        yield* storage
          .subscribers(Topic.make("old"), 0)
          .pipe(Stream.runCollect),
      ).toEqual([])
      expect(
        yield* storage
          .subscribers(Topic.make("new"), 0)
          .pipe(Stream.runCollect),
      ).toHaveLength(1)
    }).pipe(Effect.provide(testLayer())),
)
