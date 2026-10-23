import { expect, it } from "@effect/vitest"
import { Cause, Effect, Logger } from "effect"
import {
  ConnectionId,
  ConnectionRejected,
  TransportError,
  createWebSocketHandler,
  type JsonObject,
} from "../src/index.ts"
import { GatewayMessages, Sandbox } from "../src/Sandbox.ts"
import { loadConnection } from "./fixtures.ts"
import { options, testLayer, websocket, wsEvent } from "./fixtures.ts"

it.effect(
  "only false and ConnectionRejected are authentication rejections",
  () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      const gateway = yield* GatewayMessages
      const rejected: ReadonlyArray<
        Effect.Effect<false | JsonObject, ConnectionRejected>
      > = [Effect.succeed(false), Effect.fail(new ConnectionRejected({}))]
      for (const [index, decision] of rejected.entries()) {
        const upgradeId = `upgrade-${index}`
        const upgrade = createWebSocketHandler({
          ...options,
          onConnect: () => decision,
        })
        expect(
          (yield* upgrade(wsEvent(upgradeId, "$connect"), {})).statusCode,
        ).toBe(401)
        expect(yield* sandbox.snapshot).toEqual({})
        const initId = `init-${index}`
        const init = createWebSocketHandler({
          ...options,
          onInit: () => decision,
        })
        yield* websocket(wsEvent(initId, "$connect"), {})
        expect(
          (yield* init(
            wsEvent(initId, "$default", { type: "connection_init" }),
            {},
          )).statusCode,
        ).toBe(401)
        expect(yield* sandbox.snapshot).toEqual({})
        expect(yield* gateway.closed).toContain(initId)
        expect(yield* gateway.all).toEqual([])
      }
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "auth infrastructure errors and defects return 500 with their causes logged, and failed upgrades are cleaned up",
  () =>
    Effect.gen(function* () {
      const logCauses: Array<string> = []
      const logger = Logger.make(({ cause }) => {
        if (cause.reasons.length) logCauses.push(Cause.pretty(cause))
      })
      const failure = new Error("credential database offline")
      const failures: ReadonlyArray<
        Effect.Effect<never, Error | TransportError>
      > = [
        Effect.fail(failure),
        Effect.fail(
          new TransportError({
            message: "credential service unavailable",
            code: "UNAUTHORIZED",
            status: 401,
          }),
        ),
        Effect.die(new Error("auth decoder defect")),
        Effect.die(
          new TransportError({
            message: "unexpected auth defect",
            code: "UNAUTHORIZED",
            status: 401,
          }),
        ),
      ]
      for (const [index, failed] of failures.entries()) {
        const upgrade = createWebSocketHandler({
          ...options,
          onConnect: () => failed,
        })
        const response = yield* upgrade(
          wsEvent(`upgrade-${index}`, "$connect"),
          {},
        ).pipe(Effect.provide(Logger.layer([logger])))
        expect(response.statusCode).toBe(500)
        expect(response.body).toBe("")
        expect(yield* (yield* Sandbox).snapshot).toEqual({})
      }
      expect(logCauses).toHaveLength(4)
      expect(logCauses[3]).toContain("unexpected auth defect")
      expect(logCauses[0]).toContain("credential database offline")
      expect(logCauses[1]).toContain("credential service unavailable")
      expect(logCauses[2]).toContain("auth decoder defect")
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "init infrastructure failures close and clean up without protocol frames",
  () =>
    Effect.gen(function* () {
      for (const [index, failed] of [
        Effect.fail(new Error("JWT key fetch failed")),
        Effect.die(new Error("JWT implementation defect")),
      ].entries()) {
        const id = `init-failure-${index}`
        yield* websocket(wsEvent(id, "$connect"), {})
        const handler = createWebSocketHandler({
          ...options,
          onInit: () => failed,
        })
        expect(
          (yield* handler(
            wsEvent(id, "$default", { type: "connection_init" }),
            {},
          )).statusCode,
        ).toBe(500)
        expect(yield* (yield* Sandbox).snapshot).toEqual({})
        expect(yield* (yield* GatewayMessages).closed).toContain(id)
        expect(yield* (yield* GatewayMessages).all).toEqual([])
      }
    }).pipe(Effect.provide(testLayer())),
)

it.effect("synchronously thrown auth defects are server errors too", () =>
  Effect.gen(function* () {
    const upgrade = createWebSocketHandler({
      ...options,
      onConnect: () => {
        throw new Error("hook threw before returning its Effect")
      },
    })
    const response = yield* upgrade(wsEvent("thrown", "$connect"), {})
    expect(response.statusCode).toBe(500)
    expect(response.body).not.toContain("hook threw")
    expect(yield* (yield* Sandbox).snapshot).toEqual({})
  }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "untrusted init parameters cannot replace upgrade identity without an explicit mapping",
  () =>
    Effect.gen(function* () {
      const handler = createWebSocketHandler({
        ...options,
        onInit: undefined,
        onConnect: () =>
          Effect.succeed({ user: "authenticated", role: "member" }),
      })
      const id = ConnectionId.make("upgrade-only")
      yield* handler(wsEvent(id, "$connect"), {})
      yield* handler(
        wsEvent(id, "$default", {
          type: "connection_init",
          payload: { user: "victim", role: "admin" },
        }),
        {},
      )
      yield* handler(
        wsEvent(id, "$default", {
          type: "subscribe",
          id: "query",
          payload: { query: "{ me }" },
        }),
        {},
      )
      const connection = yield* loadConnection(id)
      expect(connection.context).toEqual({
        user: "authenticated",
        role: "member",
      })
      expect(connection.initParams).toEqual({ user: "victim", role: "admin" })
      const frames = (yield* (yield* GatewayMessages).all).map((frame) =>
        JSON.parse(frame.data),
      )
      expect(frames[1].payload.data.me).toBe("authenticated")
      const anonymous = createWebSocketHandler({
        ...options,
        onInit: undefined,
      })
      yield* anonymous(wsEvent("anonymous", "$connect"), {})
      yield* anonymous(
        wsEvent("anonymous", "$default", {
          type: "connection_init",
          payload: { user: "victim", role: "admin" },
        }),
        {},
      )
      expect(
        (yield* loadConnection(ConnectionId.make("anonymous"))).context,
      ).toEqual({})
    }).pipe(Effect.provide(testLayer())),
)
