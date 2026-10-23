import { expect, it } from "@effect/vitest"
import { ErrorReporter, type ErrorReport } from "@model-ts/graphql"
import { Cause, Effect } from "effect"
import {
  GatewayMessages,
  createHttpHandler,
  createWebSocketHandler,
} from "../src/index.ts"
import { streamHandler } from "../src/PubSub/DynamoDB.ts"
import {
  Greeting,
  Identity,
  builderOptions,
  builderTopic,
  privateFailure,
} from "./builder.ts"
import { publish } from "./fixtures.ts"
import { httpEvent, streamEvent, testLayer, wsEvent } from "./fixtures.ts"

const invocation = { requestId: "builder-test" }
const http = createHttpHandler(builderOptions)
const websocket = createWebSocketHandler(builderOptions)
const delivery = streamHandler(builderOptions)
const query = "{ hello publicFailure forbidden internal }"

for (const transport of ["HTTP", "WebSocket"] as const) {
  it.effect(
    `${transport} preserves builder public errors and locations, masks internal failures and reports their cause`,
    () =>
      Effect.gen(function* () {
        const reports: ErrorReport[] = []
        const run = Effect.gen(function* () {
          if (transport === "HTTP") {
            const response = yield* http(httpEvent({ query }), invocation)
            expect(response.statusCode).toBe(200)
            expect(response.isBase64Encoded).toBe(false)
            expect(response.headers?.["content-type"]).toBe("application/json")
            return response.body
          }
          yield* websocket(wsEvent("builder", "$connect"), invocation)
          yield* websocket(
            wsEvent("builder", "$default", { type: "connection_init" }),
            invocation,
          )
          yield* websocket(
            wsEvent("builder", "$default", {
              type: "subscribe",
              id: "q",
              payload: { query },
            }),
            invocation,
          )
          const frames = (yield* (yield* GatewayMessages).all).map((message) =>
            JSON.parse(message.data),
          )
          expect(frames.map((frame) => frame.type)).toEqual([
            "connection_ack",
            "next",
            "complete",
          ])
          return JSON.stringify(frames[1].payload)
        })
        const body = yield* run.pipe(
          Effect.provideService(ErrorReporter, {
            report: (report) =>
              Effect.sync(() => {
                reports.push(report)
              }),
          }),
        )
        expect(JSON.parse(body)).toEqual({
          data: {
            hello: "Hello Ada",
            publicFailure: null,
            forbidden: null,
            internal: null,
          },
          errors: [
            {
              message: "This action is unavailable",
              locations: [{ line: 1, column: 9 }],
              path: ["publicFailure"],
              extensions: { code: "INVALID_STATE" },
            },
            {
              message: 'You don\'t have permission to access field "forbidden"',
              locations: [{ line: 1, column: 23 }],
              path: ["forbidden"],
              extensions: { code: "FORBIDDEN" },
            },
            {
              message: "Internal server error",
              locations: [{ line: 1, column: 33 }],
              path: ["internal"],
              extensions: { code: "INTERNAL_SERVER_ERROR" },
            },
          ],
        })
        expect(body).not.toContain(privateFailure.message)
        expect(reports).toHaveLength(1)
        expect(reports[0].coordinate).toBe("Query.internal")
        expect(Cause.squash(reports[0].cause)).toBe(privateFailure)
      }).pipe(
        Effect.provideService(Greeting, { prefix: "Hello " }),
        Effect.provideService(Identity, { user: "Ada" }),
        Effect.provide(testLayer()),
      ),
  )
}

it.effect(
  "delivers a builder subscription through published events with filtering",
  () =>
    Effect.gen(function* () {
      yield* websocket(wsEvent("subscriber", "$connect"), invocation)
      yield* websocket(
        wsEvent("subscriber", "$default", { type: "connection_init" }),
        invocation,
      )
      yield* websocket(
        wsEvent("subscriber", "$default", {
          type: "subscribe",
          id: "s",
          payload: { query: "subscription { message }" },
        }),
        invocation,
      )
      const messages = yield* GatewayMessages
      expect(
        (yield* messages.all).map((message) => JSON.parse(message.data)),
      ).toEqual([{ type: "connection_ack" }])
      for (const user of ["Other", "Ada"]) {
        const payload = { user, message: "event" }
        const event = yield* publish(builderTopic, payload)
        expect(yield* delivery(streamEvent(event), invocation)).toEqual({
          batchItemFailures: [],
        })
      }
      const frames = (yield* messages.all)
        .slice(1)
        .map((message) => JSON.parse(message.data))
      expect(frames).toEqual([
        {
          type: "next",
          id: "s",
          payload: { data: { message: "Hello event" } },
        },
      ])
      yield* websocket(wsEvent("subscriber", "$disconnect"), invocation)
      yield* delivery(
        streamEvent(
          yield* publish(builderTopic, {
            user: "Ada",
            message: "after disconnect",
          }),
        ),
        invocation,
      )
      expect(yield* messages.all).toHaveLength(2)
    }).pipe(
      Effect.provideService(Greeting, { prefix: "Hello " }),
      Effect.provideService(Identity, { user: "Ada" }),
      Effect.provide(testLayer()),
    ),
)
