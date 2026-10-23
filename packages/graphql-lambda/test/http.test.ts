import { it, expect } from "@effect/vitest"
import { Effect } from "effect"
import {
  createHttpHandler,
  graphqlExecutor,
  formatError,
  TransportError,
  type ContextRequest,
} from "../src/index.ts"
import { header } from "../src/internal/request.ts"
import {
  http,
  httpEvent,
  options,
  schema,
  type AppContext,
  type LambdaContext,
} from "./fixtures.ts"

it.effect(
  "batches queries with middleware context, reflected CORS and multiple rotated cookies",
  () =>
    Effect.gen(function* () {
      const response = yield* http(
        httpEvent([{ query: "{ me }" }, { query: '{ echo(text: "hi") }' }], {
          cookies: ["session=old", "theme=dark"],
          headers: {
            "content-type": "application/json",
            origin: "https://app.example",
          },
        }),
        { user: "middleware-user" },
      )
      expect(response.statusCode).toBe(200)
      expect(response.isBase64Encoded).toBe(false)
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
      expect(response.cookies).toEqual([
        "session=rotated; HttpOnly",
        "refresh=rotated; Secure",
      ])
    }),
)
it.effect(
  "passes the complete HTTP event and cookies to one batch context",
  () =>
    Effect.gen(function* () {
      const event = httpEvent([{ query: "{ me }" }, { query: "{ me }" }], {
        cookies: ["session=first", "session=second; theme=dark"],
        headers: {
          "content-type": "application/json",
          "x-request-tag": "first,second",
        },
      })
      let contexts = 0
      const handler = createHttpHandler({
        ...options,
        context: (request: ContextRequest<LambdaContext>) => {
          contexts++
          expect(request.event).toBe(event)
          if (!("version" in request.event))
            throw new Error("Expected HTTP event")
          expect(request.event.cookies).toEqual(event.cookies)
          expect(header(request.event, "Cookie")).toBe(
            "session=first; session=second; theme=dark",
          )
          expect(header(request.event, "x-request-tag")).toBe("first,second")
          request.response.headers["x-context"] = "present"
          return options.context(request)
        },
      })
      const response = yield* handler(event, {})
      expect(contexts).toBe(1)
      expect(response).toEqual({
        statusCode: 200,
        headers: { "content-type": "application/json", "x-context": "present" },
        cookies: ["session=rotated; HttpOnly", "refresh=rotated; Secure"],
        body: JSON.stringify([
          { data: { me: "guest" } },
          { data: { me: "guest" } },
        ]),
        isBase64Encoded: false,
      })
    }),
)

it.effect("rejects unsupported payload versions before creating context", () =>
  Effect.gen(function* () {
    const handler = createHttpHandler({
      ...options,
      context: () => Effect.die("must not run"),
    })
    const event = { ...httpEvent({ query: "{ me }" }), version: "3.0" }
    // @ts-expect-error HTTP payload versions other than 2.0 are not accepted.
    const response = yield* handler(event, {})
    expect(response.statusCode).toBe(400)
    expect(response.isBase64Encoded).toBe(false)
    expect(response.body).toContain("HTTP payload format 2.0 is required")
  }),
)

it.effect("handles bodyless GETs and rejects unsupported methods", () =>
  Effect.gen(function* () {
    const response = yield* http(
      httpEvent(null, {
        body: undefined,
        requestContext: { http: { method: "GET" } },
        queryStringParameters: { query: "{ me }" },
      }),
      {},
    )
    expect(JSON.parse(response.body)).toEqual({ data: { me: "guest" } })
    expect(response.cookies).toBeUndefined()
    const rejected = yield* http(
      httpEvent(null, {
        requestContext: { http: { method: "PUT" } },
      }),
      {},
    )
    expect(rejected.statusCode).toBe(405)
    expect(rejected.headers?.allow).toBe("GET, POST, OPTIONS")
  }),
)

it.effect("enforces decoded UTF-8 body and configured batch limits", () =>
  Effect.gen(function* () {
    const body = JSON.stringify({ query: '{ echo(text: "é") }' })
    const handler = createHttpHandler({
      ...options,
      maxBodyBytes: Buffer.byteLength(body),
      maxBatchSize: 1,
    })
    for (const isBase64Encoded of [false, true]) {
      const event = httpEvent(null, {
        body: isBase64Encoded ? Buffer.from(body).toString("base64") : body,
        isBase64Encoded,
      })
      expect((yield* handler(event, {})).statusCode).toBe(200)
      const limited = createHttpHandler({
        ...options,
        maxBodyBytes: Buffer.byteLength(body) - 1,
      })
      expect((yield* limited(event, {})).statusCode).toBe(400)
    }
    const batchHandler = createHttpHandler({ ...options, maxBatchSize: 1 })
    const batch = yield* batchHandler(
      httpEvent([{ query: "{me}" }, { query: "{me}" }]),
      {},
    )
    expect(batch.statusCode).toBe(400)
    expect(batch.body).toContain("Invalid batch size")
  }),
)
it.effect("GET variables work; GET mutations never execute", () =>
  Effect.gen(function* () {
    const query = yield* http(
      httpEvent(null, {
        requestContext: { http: { method: "GET" } },
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
        requestContext: { http: { method: "GET" } },
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
          requestContext: { http: { method: "OPTIONS" } },
          headers: {
            "access-control-request-headers": "Authorization, X-App-Session-ID",
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
it.effect("controls introspection, GraphiQL and optional health route", () =>
  Effect.gen(function* () {
    const handler = createHttpHandler({
      ...options,
      introspection: false,
      graphiql: true,
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
      httpEvent(null, {
        requestContext: { http: { method: "GET" } },
        headers: { accept: "text/html" },
      }),
      {},
    )
    expect(html.headers?.["content-type"]).toBe("text/html; charset=utf-8")
    expect(
      (yield* handler(
        httpEvent(null, { rawPath: "/.well-known/apollo/server-health" }),
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
