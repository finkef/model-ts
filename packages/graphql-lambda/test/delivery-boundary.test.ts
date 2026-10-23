import { expect, it } from "@effect/vitest"
import { Cause, Deferred, Effect, Fiber, Logger } from "effect"
import {
  Gateway,
  GatewayError,
  ConnectionId,
  OperationId,
  StorageError,
  TransportError,
  createWebSocketHandler,
} from "../src/index.ts"
import { streamHandler } from "../src/PubSub/DynamoDB.ts"
import { GatewayMessages } from "../src/Sandbox.ts"
import { Storage } from "../src/Storage.ts"
import { Greeting, Identity, builderOptions, builderTopic } from "./builder.ts"
import { publish } from "./fixtures.ts"
import { streamEvent, testLayer, wsEvent } from "./fixtures.ts"

const invocation = { requestId: "delivery-boundary" }
const register = Effect.gen(function* () {
  const websocket = createWebSocketHandler(builderOptions)
  for (const id of ["a-bad", "z-good"]) {
    yield* websocket(wsEvent(id, "$connect"), invocation)
    yield* websocket(
      wsEvent(id, "$default", { type: "connection_init" }),
      invocation,
    )
    yield* websocket(
      wsEvent(id, "$default", {
        type: "subscribe",
        id: "s",
        payload: { query: "subscription { message }" },
      }),
      invocation,
    )
  }
})
const payload = { user: "Ada", message: "event" }

{
  for (const fault of ["throw", "die", "formatter"] as const)
    it.effect(
      `stream terminates on ${fault}, reports the cause and delivers to the next subscriber`,
      () =>
        Effect.gen(function* () {
          yield* register
          const messages = yield* GatewayMessages
          const causes: Cause.Cause<unknown>[] = []
          const secret = new Error("private context defect")
          const formatterSecret = new Error("private formatter defect")
          {
            const options = {
              ...builderOptions,
              context: (
                request: Parameters<typeof builderOptions.context>[0],
              ) => {
                if (request.connection?.id === "a-bad") {
                  if (fault === "throw") throw secret
                  if (fault === "die") return Effect.die(secret)
                  return Effect.fail(
                    new TransportError({
                      message: "Explicit rejection",
                      code: "FORBIDDEN",
                      status: 403,
                    }),
                  )
                }
                return builderOptions.context(request)
              },
              formatError: () => {
                throw formatterSecret
              },
            }
            const deliver = Effect.gen(function* () {
              const event = yield* publish(builderTopic, payload)
              expect(
                yield* streamHandler(options)(streamEvent(event), invocation),
              ).toEqual({
                batchItemFailures: [],
              })
            })
            yield* deliver.pipe(
              Effect.provide(
                Logger.layer([
                  Logger.make(({ cause }) => {
                    causes.push(cause)
                  }),
                ]),
              ),
            )
            const frames = (yield* messages.all).slice(-2).map((message) => ({
              id: message.connectionId,
              frame: JSON.parse(message.data),
            }))
            expect(frames).toEqual([
              {
                id: "a-bad",
                frame: {
                  type: "error",
                  id: "s",
                  payload: [
                    {
                      message: "Internal server error",
                      extensions: { code: "INTERNAL_SERVER_ERROR" },
                    },
                  ],
                },
              },
              {
                id: "z-good",
                frame: {
                  type: "next",
                  id: "s",
                  payload: { data: { message: "Hello event" } },
                },
              },
            ])
            expect(
              causes.some(
                (cause) =>
                  Cause.squash(cause) ===
                  (fault === "formatter" ? formatterSecret : secret),
              ),
            ).toBe(true)
          }
          expect(causes).toHaveLength(1)
          const storage = yield* Storage
          expect(
            yield* storage.operation(
              ConnectionId.make("a-bad"),
              OperationId.make("s"),
            ),
          ).toBeUndefined()
          const before = (yield* messages.all).length
          const next = yield* publish(builderTopic, payload)
          yield* streamHandler(builderOptions)(streamEvent(next), invocation)
          expect(
            (yield* messages.all)
              .slice(before)
              .map((frame) => frame.connectionId),
          ).toEqual(["z-good"])
        }).pipe(
          Effect.provideService(Greeting, { prefix: "Hello " }),
          Effect.provideService(Identity, { user: "Ada" }),
          Effect.provide(testLayer()),
        ),
    )

  for (const fault of ["failure", "defect"] as const)
    it.effect(
      `stream retains storage ${fault} retry semantics while delivering to other subscribers`,
      () =>
        Effect.gen(function* () {
          yield* register
          const storage = yield* Storage
          const unavailable = new StorageError({
            operation: "operation",
            cause: new Error("database unavailable"),
          })
          const layer = {
            ...storage,
            operation: (...args: Parameters<typeof storage.operation>) =>
              args[0] === "a-bad"
                ? fault === "failure"
                  ? Effect.fail(unavailable)
                  : Effect.die(unavailable)
                : storage.operation(...args),
          }
          const event = yield* publish(builderTopic, payload)
          expect(
            yield* streamHandler(builderOptions)(
              streamEvent(event, "42"),
              invocation,
            ).pipe(Effect.provideService(Storage, layer)),
          ).toEqual({ batchItemFailures: [{ itemIdentifier: "42" }] })

          const frames = (yield* (yield* GatewayMessages).all).slice(2)
          expect(frames).toHaveLength(1)
          expect(frames[0].connectionId).toBe("z-good")
          expect(JSON.parse(frames[0].data).payload.data).toEqual({
            message: "Hello event",
          })
        }).pipe(
          Effect.provideService(Greeting, { prefix: "Hello " }),
          Effect.provideService(Identity, { user: "Ada" }),
          Effect.provide(testLayer()),
        ),
    )

  it.effect(
    `stream preserves interruption and awaits pending context cleanup`,
    () =>
      Effect.gen(function* () {
        yield* register
        const entered = yield* Deferred.make<void>()
        const finalized = yield* Deferred.make<void>()
        const options = {
          ...builderOptions,
          context: () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined)
              return yield* Effect.never
            }).pipe(Effect.ensuring(Deferred.succeed(finalized, undefined))),
        }
        const deliver = Effect.gen(function* () {
          const event = yield* publish(builderTopic, payload)
          yield* streamHandler(options)(streamEvent(event), invocation)
        })
        const fiber = yield* deliver.pipe(Effect.forkChild)
        yield* Deferred.await(entered)
        yield* Fiber.interrupt(fiber)
        yield* Deferred.await(finalized)
        const exit = yield* Fiber.await(fiber)
        expect(exit._tag).toBe("Failure")
        if (exit._tag === "Failure")
          expect(Cause.hasInterrupts(exit.cause)).toBe(true)
        expect(yield* (yield* GatewayMessages).all).toHaveLength(2)
      }).pipe(
        Effect.provideService(Greeting, { prefix: "Hello " }),
        Effect.provideService(Identity, { user: "Ada" }),
        Effect.provide(testLayer()),
      ),
  )
}

for (const failure of ["context", "oversize"] as const) {
  it.effect(
    `terminal ${failure} errors retain retry state until the error frame is sent`,
    () =>
      Effect.gen(function* () {
        yield* register
        const gateway = yield* Gateway
        const settings = {
          ...builderOptions,
          context: (request: Parameters<typeof builderOptions.context>[0]) =>
            request.connection?.id === "a-bad" && failure === "context"
              ? Effect.fail(
                  new TransportError({
                    message: "Session revoked",
                    code: "UNAUTHORIZED",
                    status: 401,
                  }),
                )
              : builderOptions.context(request),
        }
        const event = yield* publish(builderTopic, {
          ...payload,
          message: failure === "oversize" ? "x".repeat(40 * 1024) : "event",
        })
        const deliver = streamHandler(settings)(
          streamEvent(event, "42"),
          invocation,
        )
        expect(
          yield* deliver.pipe(
            Effect.provideService(Gateway, {
              ...gateway,
              post: (endpoint, id, data) =>
                id === "a-bad"
                  ? Effect.fail(
                      new GatewayError({
                        operation: "PostToConnection",
                        gone: false,
                        cause: new Error("Throttled"),
                      }),
                    )
                  : gateway.post(endpoint, id, data),
            }),
          ),
        ).toEqual({ batchItemFailures: [{ itemIdentifier: "42" }] })
        const storage = yield* Storage
        expect(
          (yield* storage.operation(
            ConnectionId.make("a-bad"),
            OperationId.make("s"),
          ))?.state,
        ).toBe("active")
        expect(yield* deliver).toEqual({ batchItemFailures: [] })
        expect(
          yield* storage.operation(
            ConnectionId.make("a-bad"),
            OperationId.make("s"),
          ),
        ).toBeUndefined()
        const frames = (yield* (yield* GatewayMessages).all)
          .filter((frame) => frame.connectionId === "a-bad")
          .slice(1)
          .map((frame) => JSON.parse(frame.data))
        expect(frames).toEqual([
          {
            type: "error",
            id: "s",
            payload: [
              {
                message:
                  failure === "context"
                    ? "Session revoked"
                    : "WebSocket result exceeds the 32 KiB single-frame limit",
                extensions: {
                  code:
                    failure === "context"
                      ? "UNAUTHORIZED"
                      : "MESSAGE_TOO_LARGE",
                },
              },
            ],
          },
        ])
      }).pipe(
        Effect.provideService(Greeting, { prefix: "Hello " }),
        Effect.provideService(Identity, { user: "Ada" }),
        Effect.provide(testLayer()),
      ),
  )
}
