import { it, expect } from "@effect/vitest"
import { Effect, Tracer } from "effect"
import { ConnectionId } from "../src/Identifiers.ts"
import { Gateway } from "../src/index.ts"

it.effect(
  "SDK layer routes per endpoint, detects SDK v3 410 and traces every call without AWS",
  () =>
    Effect.gen(function* () {
      const calls: Array<{ hostname: string; method: string }> = []
      const spans: Array<string> = []
      let destroyed = 0
      const layer = Gateway.layer({
        region: "us-east-1",
        credentials: { accessKeyId: "test", secretAccessKey: "test" },
        maxAttempts: 1,
        requestHandler: {
          handle: async (request: {
            hostname: string
            method: string
            path: string
          }) => {
            calls.push({ hostname: request.hostname, method: request.method })
            return {
              response: {
                statusCode: request.path.endsWith("gone") ? 410 : 200,
                headers: { "content-type": "application/json" },
                body: Buffer.from(
                  request.path.endsWith("gone")
                    ? '{"message":"Gone","__type":"GoneException"}'
                    : "{}",
                ),
              },
            }
          },
          destroy: () => {
            destroyed++
          },
        },
      })
      const tracer = Tracer.make({
        span(options) {
          spans.push(options.name)
          return new Tracer.NativeSpan(options)
        },
      })
      yield* Effect.gen(function* () {
        const gateway = yield* Gateway
        yield* gateway.post(
          "https://one.execute-api.local/ws",
          ConnectionId.make("ok"),
          "{}",
        )
        yield* gateway.post(
          "https://two.execute-api.local/ws",
          ConnectionId.make("ok"),
          "{}",
        )
        yield* gateway.close(
          "https://one.execute-api.local/ws",
          ConnectionId.make("ok"),
        )
        const failure = yield* gateway
          .post(
            "https://two.execute-api.local/ws",
            ConnectionId.make("gone"),
            "{}",
          )
          .pipe(Effect.flip)
        expect(failure.gone).toBe(true)
        expect(failure.operation).toBe("PostToConnection")
      }).pipe(Effect.provide(layer), Effect.withTracer(tracer))
      expect(calls).toEqual([
        { hostname: "one.execute-api.local", method: "POST" },
        { hostname: "two.execute-api.local", method: "POST" },
        { hostname: "one.execute-api.local", method: "DELETE" },
        { hostname: "two.execute-api.local", method: "POST" },
      ])
      expect(
        spans.filter((name) => name.startsWith("ApiGatewayManagementApi.")),
      ).toEqual([
        "ApiGatewayManagementApi.PostToConnection",
        "ApiGatewayManagementApi.PostToConnection",
        "ApiGatewayManagementApi.DeleteConnection",
        "ApiGatewayManagementApi.PostToConnection",
      ])
      expect(destroyed).toBe(2)
    }),
)
