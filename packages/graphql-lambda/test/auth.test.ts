import { expect, it } from "@effect/vitest"
import { Sandbox } from "@model-ts/dynamodb/Sandbox"
import { Cause, Effect, Logger } from "effect"
import {
  Connection,
  ConnectionId,
  ConnectionRejected,
  GatewayMessages,
  TransportError,
  createWebSocketHandler,
  type JsonObject,
} from "../src/index.ts"
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
          onWebsocketConnect: () => decision,
        })
        expect(
          (yield* upgrade(wsEvent(upgradeId, "$connect"), {})).statusCode,
        ).toBe(401)
        expect(yield* sandbox.snapshot).toEqual({})
        const initId = `init-${index}`
        const init = createWebSocketHandler({
          ...options,
          onConnect: () => decision,
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
        expect(JSON.parse((yield* gateway.all).at(-1)!.data)).toMatchObject({
          type: "error",
          payload: { extensions: { code: "UNAUTHORIZED" } },
        })
      }
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "auth infrastructure errors and defects return 500 with their causes logged, and failed upgrades are cleaned up",
  () =>
    Effect.gen(function* () {
      const logCauses: Array<string> = []
      const logger = Logger.make(({ cause }) => {
        logCauses.push(Cause.pretty(cause))
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
          onWebsocketConnect: () => failed,
        })
        const response = yield* upgrade(
          wsEvent(`upgrade-${index}`, "$connect"),
          {},
        ).pipe(Effect.provide(Logger.layer([logger])))
        expect(response.statusCode).toBe(500)
        expect(JSON.parse(response.body)).toEqual({
          errors: [
            {
              message: "Internal server error",
              extensions: { code: "INTERNAL_SERVER_ERROR" },
            },
          ],
        })
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
  "init failures retain uninitialized state for a retry and never send an unauthorized frame",
  () =>
    Effect.gen(function* () {
      const id = ConnectionId.make("retry")
      yield* websocket(wsEvent(id, "$connect"), {})
      const logs: Array<string> = []
      const logger = Logger.make(({ cause }) => {
        logs.push(Cause.pretty(cause))
      })
      for (const failed of [
        Effect.fail(new Error("JWT key fetch failed")),
        Effect.die(new Error("JWT implementation defect")),
      ]) {
        const handler = createWebSocketHandler({
          ...options,
          onConnect: () => failed,
        })
        expect(
          (yield* handler(
            wsEvent(id, "$default", { type: "connection_init" }),
            {},
          ).pipe(Effect.provide(Logger.layer([logger])))).statusCode,
        ).toBe(500)
        const persisted = yield* Connection.get(Connection.key(id))
        expect(persisted.initialized).toBe(false)
        expect(persisted.context).toEqual({})
        expect(yield* (yield* GatewayMessages).all).toEqual([])
        expect(yield* (yield* GatewayMessages).closed).toEqual([])
      }
      expect(logs[0]).toContain("JWT key fetch failed")
      expect(logs[1]).toContain("JWT implementation defect")
      expect(
        (yield* websocket(
          wsEvent(id, "$default", {
            type: "connection_init",
            payload: { token: "valid" },
          }),
          {},
        )).statusCode,
      ).toBe(200)
      expect((yield* Connection.get(Connection.key(id))).initialized).toBe(true)
    }).pipe(Effect.provide(testLayer())),
)

it.effect("synchronously thrown auth defects are server errors too", () =>
  Effect.gen(function* () {
    const upgrade = createWebSocketHandler({
      ...options,
      onWebsocketConnect: () => {
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
        onConnect: undefined,
        onWebsocketConnect: () =>
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
          type: "start",
          id: "query",
          payload: { query: "{ me }" },
        }),
        {},
      )
      const connection = yield* Connection.get(Connection.key(id))
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
        onConnect: undefined,
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
        (yield* Connection.get(Connection.key(ConnectionId.make("anonymous"))))
          .context,
      ).toEqual({})
    }).pipe(Effect.provide(testLayer())),
)
