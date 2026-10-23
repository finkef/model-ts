import { expect, it } from "@effect/vitest"
import { Cause, Deferred, Effect, Fiber, Logger } from "effect"
import {
  createHttpHandler,
  type ContextRequest,
  type Result,
} from "../src/index.ts"
import { httpEvent, options, type LambdaContext } from "./fixtures.ts"

it.effect(
  "rejects simple requests before context or execution, and grants no CORS access by default",
  () =>
    Effect.gen(function* () {
      let contexts = 0
      let executions = 0
      const handler = createHttpHandler({
        ...options,
        context: (request: ContextRequest<LambdaContext>) => {
          contexts++
          return options.context(request)
        },
        executor: {
          ...options.executor,
          execute: () =>
            Effect.sync(() => {
              executions++
              return { data: {} }
            }),
        },
      })
      for (const contentType of [
        undefined,
        "text/plain",
        "application/x-www-form-urlencoded",
        "multipart/form-data",
      ]) {
        const response = yield* handler(
          httpEvent(
            { query: "mutation { update }" },
            {
              cookies: ["session=valid"],
              headers: {
                origin: "https://attacker.example",
                ...(contentType ? { "content-type": contentType } : {}),
              },
            },
          ),
          {},
        )
        expect(response.statusCode).toBe(400)
        expect(
          response.headers?.["access-control-allow-origin"],
        ).toBeUndefined()
        expect(
          response.headers?.["access-control-allow-credentials"],
        ).toBeUndefined()
      }
      expect(contexts).toBe(0)
      expect(executions).toBe(0)
      const allowed = yield* handler(
        httpEvent(
          { query: "{ me }" },
          {
            headers: {
              origin: "https://attacker.example",
              "content-type": "application/json; charset=utf-8",
            },
          },
        ),
        {},
      )
      expect(allowed.statusCode).toBe(200)
      expect(allowed.headers?.["access-control-allow-origin"]).toBeUndefined()
    }),
)

it.effect(
  "requires explicit origins for credentials and a preflight header for headerless GETs",
  () =>
    Effect.gen(function* () {
      let contexts = 0
      const handler = createHttpHandler({
        ...options,
        cors: { origin: ["https://app.example"], credentials: true },
        csrfHeaders: ["x-graphql-csrf"],
        context: (request: ContextRequest<LambdaContext>) => {
          contexts++
          return options.context(request)
        },
      })
      for (const origin of [
        "https://app.example",
        "https://attacker.example",
      ]) {
        const response = yield* handler(
          httpEvent(
            { query: "{ me }" },
            {
              headers: { origin: origin, "content-type": "application/json" },
            },
          ),
          {},
        )
        expect(response.headers?.["access-control-allow-origin"]).toBe(
          origin === "https://app.example" ? origin : undefined,
        )
        expect(response.headers?.["access-control-allow-credentials"]).toBe(
          origin === "https://app.example" ? "true" : undefined,
        )
      }
      contexts = 0
      for (const value of [undefined, "", " "]) {
        const response = yield* handler(
          httpEvent(null, {
            requestContext: { http: { method: "GET" } },
            queryStringParameters: { query: "{ me }" },
            headers: { "x-graphql-csrf": value },
          }),
          {},
        )
        expect(response.statusCode).toBe(400)
      }
      expect(contexts).toBe(0)
      const mutation = yield* handler(
        httpEvent(null, {
          requestContext: { http: { method: "GET" } },
          queryStringParameters: { query: "mutation { update }" },
          headers: { "x-graphql-csrf": "true" },
        }),
        {},
      )
      expect(mutation.statusCode).toBe(405)
      expect(contexts).toBe(0)
      const query = yield* handler(
        httpEvent(null, {
          requestContext: { http: { method: "GET" } },
          queryStringParameters: { query: "{ me }" },
          headers: { "x-graphql-csrf": "true" },
        }),
        {},
      )
      expect(query.statusCode).toBe(200)
      expect(contexts).toBe(1)
      const invalidPolicy = createHttpHandler({
        ...options,
        cors: { origin: "*", credentials: true },
      })
      const refused = yield* invalidPolicy(httpEvent({ query: "{ me }" }), {})
      expect(refused.statusCode).toBe(500)
      expect(refused.headers?.["access-control-allow-origin"]).toBeUndefined()
    }),
)

it.effect(
  "HTTP defects and throwing formatters return a fixed safe 500 and log their cause",
  () =>
    Effect.gen(function* () {
      const causes: Array<string> = []
      const logger = Logger.layer([
        Logger.make(({ cause }) => {
          causes.push(Cause.pretty(cause))
        }),
      ])
      const cycle: Record<string, unknown> = {}
      cycle.self = cycle
      // Deliberately malformed third-party executor results exercise the runtime serialization boundary.
      const malformed = [cycle, { value: 1n }].map(
        (data) => ({ data }) as Result,
      )
      const handlers = [
        createHttpHandler({
          ...options,
          context: () => Effect.die(new Error("private-context")),
        }),
        createHttpHandler({
          ...options,
          context: () => {
            throw new Error("private-sync-context")
          },
        }),
        createHttpHandler({
          ...options,
          executor: {
            ...options.executor,
            execute: () => Effect.die(new Error("private-executor")),
          },
        }),
        ...malformed.map((result) =>
          createHttpHandler({
            ...options,
            executor: {
              ...options.executor,
              execute: () => Effect.succeed(result),
            },
          }),
        ),
        createHttpHandler({
          ...options,
          formatError: () => {
            throw new Error("private-formatter")
          },
          context: () => Effect.fail(new Error("private-context-failure")),
        }),
      ]
      for (const handler of handlers) {
        const response = yield* handler(
          httpEvent({ query: "{ me }" }),
          {},
        ).pipe(Effect.provide(logger))
        expect(response.statusCode).toBe(500)
        expect(JSON.parse(response.body)).toEqual({
          errors: [
            {
              message: "Internal server error",
              extensions: { code: "INTERNAL_SERVER_ERROR" },
            },
          ],
        })
        expect(response.headers?.["content-type"]).toBe("application/json")
        expect(response.body).not.toContain("private")
      }
      for (const marker of [
        "private-context",
        "private-sync-context",
        "private-executor",
        "private-formatter",
      ])
        expect(causes.some((cause) => cause.includes(marker))).toBe(true)
      expect(causes.length).toBeGreaterThanOrEqual(handlers.length)
    }),
)

it.effect("HTTP preserves interruption and releases a pending context", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>()
    let finalized = false
    const handler = createHttpHandler({
      ...options,
      context: () =>
        Effect.gen(function* () {
          yield* Deferred.succeed(entered, undefined)
          return yield* Effect.never
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              finalized = true
            }),
          ),
        ),
    })
    const fiber = yield* handler(httpEvent({ query: "{ me }" }), {}).pipe(
      Effect.forkChild,
    )
    yield* Deferred.await(entered)
    yield* Fiber.interrupt(fiber)
    expect(finalized).toBe(true)
    const exit = fiber.pollUnsafe()
    expect(exit?._tag).toBe("Failure")
    if (exit?._tag === "Failure")
      expect(Cause.hasInterrupts(exit.cause)).toBe(true)
  }),
)
