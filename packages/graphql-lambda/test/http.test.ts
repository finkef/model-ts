import { it, expect } from "@effect/vitest"
import { Effect } from "effect"
import {
  createHttpHandler,
  graphqlExecutor,
  formatError,
  TransportError,
} from "../src/index.ts"
import {
  http,
  httpEvent,
  options,
  schema,
  type AppContext,
} from "./fixtures.ts"

it.effect(
  "batches queries with middleware context, reflected CORS and multiple rotated cookies",
  () =>
    Effect.gen(function* () {
      const response = yield* http(
        httpEvent([{ query: "{ me }" }, { query: '{ echo(text: "hi") }' }], {
          headers: {
            "Content-Type": "application/json",
            oRiGiN: "https://app.example",
            cOoKiE: "session=old",
          },
        }),
        { user: "middleware-user" },
      )
      expect(response.statusCode).toBe(200)
      expect(JSON.parse(response.body)).toEqual([
        { data: { me: "middleware-user" } },
        { data: { echo: "hi" } },
      ])
      expect(response.headers?.["access-control-allow-origin"]).toBe(
        "https://app.example",
      )
      expect(response.headers?.["access-control-allow-credentials"]).toBe(
        "true",
      )
      expect(response.multiValueHeaders?.["Set-Cookie"]).toEqual([
        "session=rotated; HttpOnly",
        "refresh=rotated; Secure",
      ])
    }),
)
it.effect("GET variables work; GET mutations never execute", () =>
  Effect.gen(function* () {
    const query = yield* http(
      httpEvent(null, {
        httpMethod: "GET",
        queryStringParameters: {
          query: "query ($text: String!) { echo(text: $text) }",
          variables: '{"text":"hello"}',
        },
      }),
      {},
    )
    expect(JSON.parse(query.body)).toEqual({ data: { echo: "hello" } })
    const mutation = yield* http(
      httpEvent(null, {
        httpMethod: "GET",
        queryStringParameters: { query: "mutation { update }" },
      }),
      {},
    )
    expect(mutation.statusCode).toBe(405)
    expect(mutation.headers?.allow).toBe("POST")
  }),
)
it.effect(
  "OPTIONS echoes requested headers without creating application context",
  () =>
    Effect.gen(function* () {
      const handler = createHttpHandler({
        ...options,
        context: () => Effect.die("must not run"),
        cors: { origin: ["https://app.example"], credentials: true },
      })
      const response = yield* handler(
        httpEvent(null, {
          httpMethod: "OPTIONS",
          headers: {
            "Access-Control-Request-Headers": "Authorization, X-App-Session-ID",
          },
        }),
        {},
      )
      expect(response.statusCode).toBe(204)
      expect(response.headers?.["access-control-allow-headers"]).toBe(
        "Authorization, X-App-Session-ID",
      )
    }),
)
it.effect(
  "masks resolver errors, preserves explicit public codes and reports originals to the formatter",
  () =>
    Effect.gen(function* () {
      const errors: Array<string> = []
      const handler = createHttpHandler({
        ...options,
        executor: graphqlExecutor<AppContext>(schema, {
          formatError: (error) => {
            errors.push(error.originalError?.message ?? error.message)
            return formatError(error)
          },
        }),
      })
      const response = yield* handler(
        httpEvent({ query: "{ secret publicError }" }),
        {},
      )
      expect(response.statusCode).toBe(200)
      expect(response.body).not.toContain("password")
      expect(response.body).toContain("INTERNAL_SERVER_ERROR")
      expect(response.body).toContain("FORBIDDEN")
      expect(errors).toContain("database password=secret")
    }),
)
it.effect("typed context failures become safe HTTP responses", () =>
  Effect.gen(function* () {
    const handler = createHttpHandler({
      ...options,
      context: () =>
        Effect.fail(
          new TransportError({
            message: "Sign in",
            code: "UNAUTHORIZED",
            status: 401,
          }),
        ),
    })
    const response = yield* handler(httpEvent({ query: "{ me }" }), {})
    expect(response.statusCode).toBe(401)
    expect(JSON.parse(response.body)).toEqual({
      errors: [{ message: "Sign in", extensions: { code: "UNAUTHORIZED" } }],
    })
  }),
)
it.effect(
  "rejects malformed JSON, invalid operations, variables and empty/oversized batches",
  () =>
    Effect.gen(function* () {
      for (const event of [
        httpEvent(null, { body: "{" }),
        httpEvent({ query: "{" }),
        httpEvent({ query: "{ missing }" }),
        httpEvent({
          query: "query ($text: String!) { echo(text: $text) }",
          variables: { text: 42 },
        }),
        httpEvent([]),
        httpEvent(Array.from({ length: 21 }, () => ({ query: "{ me }" }))),
      ]) {
        const response = yield* http(event, {})
        expect(response.statusCode).toBe(400)
      }
    }),
)
it.effect("isolates invalid batch entries and supports base64 payloads", () =>
  Effect.gen(function* () {
    const response = yield* http(
      httpEvent(null, {
        isBase64Encoded: true,
        body: Buffer.from(
          JSON.stringify([{ query: "{ me }" }, { query: "{ missing }" }]),
        ).toString("base64"),
      }),
      {},
    )
    expect(response.statusCode).toBe(200)
    const body = JSON.parse(response.body)
    expect(body[0]).toEqual({ data: { me: "guest" } })
    expect(body[1].errors).toHaveLength(1)
  }),
)
it.effect("controls introspection, explorer and optional health route", () =>
  Effect.gen(function* () {
    const handler = createHttpHandler({
      ...options,
      introspection: false,
      explorerHtml: "<html>GraphiQL</html>",
      healthCheck: Effect.void,
    })
    expect(
      (yield* handler(
        httpEvent({ query: "{ __schema { queryType { name } } }" }),
        {},
      )).statusCode,
    ).toBe(400)
    expect(
      (yield* http(
        httpEvent({ query: "{ __schema { queryType { name } } }" }),
        {},
      )).statusCode,
    ).toBe(200)
    const html = yield* handler(
      httpEvent(null, { httpMethod: "GET", headers: { accept: "text/html" } }),
      {},
    )
    expect(html.headers?.["content-type"]).toBe("text/html; charset=utf-8")
    expect(
      (yield* handler(
        httpEvent(null, { path: "/.well-known/apollo/server-health" }),
        {},
      )).body,
    ).toBe('{"status":"pass"}')
  }),
)

it.effect(
  "preserves Apollo parse, validation and variable error codes without exposing resolver details",
  () =>
    Effect.gen(function* () {
      for (const [request, code] of [
        [{ query: "{" }, "GRAPHQL_PARSE_FAILED"],
        [{ query: "{ absent }" }, "GRAPHQL_VALIDATION_FAILED"],
        [
          {
            query: "query ($text: String!) { echo(text: $text) }",
            variables: { text: 42 },
          },
          "BAD_USER_INPUT",
        ],
      ] as const) {
        const response = yield* http(httpEvent(request), {})
        expect(response.statusCode).toBe(400)
        expect(JSON.parse(response.body).errors[0].extensions.code).toBe(code)
      }
    }),
)
