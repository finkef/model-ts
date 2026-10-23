import { Cause, Effect } from "effect"
import {
  formatError as defaultFormatError,
  toGraphQLError,
  TransportError,
  badRequest,
  type ErrorFormatter,
} from "./Errors.ts"
import type { Executor } from "./Executor.ts"
import type { GraphiQLOptions } from "./GraphiQL.ts"
import { graphiqlHtml } from "./internal/graphiql.ts"
import {
  decodeRequest,
  header,
  parseJson,
  prepare,
} from "./internal/request.ts"
import { RequestValidationError } from "./internal/RequestValidationError.ts"
import type { HttpRequirements } from "./internal/requirements.ts"
import { operationPubSub } from "./internal/source.ts"
import type {
  ContextRequest,
  HttpEvent,
  HttpResponse,
  ResponseHeaders,
  ValidationOptions,
} from "./Request.ts"

export interface HttpOptions<C, L, E, R> extends ValidationOptions {
  readonly executor: Executor<C, E, R>
  readonly context: (request: ContextRequest<L>) => Effect.Effect<C, E, R>
  readonly formatError?: ErrorFormatter
  readonly cors?:
    | false
    | {
        readonly origin: true | string | ReadonlyArray<string>
        readonly credentials?: false
      }
    | {
        readonly origin: string | ReadonlyArray<string>
        readonly credentials: true
      }
  /** Non-empty header values that require browser preflight. JSON Content-Type also qualifies. */
  readonly csrfHeaders?: ReadonlyArray<
    `x-${string}` | "apollo-require-preflight"
  >
  readonly maxBatchSize?: number
  readonly maxBodyBytes?: number
  /** Serve GraphiQL for GET Accept: text/html. Disabled by default. */
  readonly graphiql?: boolean | GraphiQLOptions
  readonly healthCheck?: Effect.Effect<void, E, R>
}

/** Run the returned Effect with the application's shared ManagedRuntime. */
export function createHttpHandler<
  C,
  L,
  const O extends HttpOptions<C, L, unknown, unknown>,
>(
  options: O & HttpOptions<C, L, unknown, unknown>,
): (
  event: HttpEvent,
  lambdaContext: L,
) => Effect.Effect<HttpResponse, never, HttpRequirements<O>>
export function createHttpHandler<C, L>(
  options: HttpOptions<C, L, unknown, unknown>,
) {
  const format = options.formatError ?? defaultFormatError
  return Effect.fn("GraphQL.http")(function* (
    event: HttpEvent,
    lambdaContext: L,
  ) {
    const response: ResponseHeaders = {
      headers: { "content-type": "application/json" },
      cookies: [],
    }
    const reply = (statusCode: number, body: string): HttpResponse => ({
      statusCode,
      headers: response.headers,
      ...(response.cookies.length ? { cookies: response.cookies } : {}),
      body,
      isBase64Encoded: false,
    })
    const failure = (error: unknown) => ({
      errors: [format(toGraphQLError(error))],
    })
    return yield* Effect.gen(function* () {
      if (event.version !== "2.0")
        return yield* badRequest("HTTP payload format 2.0 is required")
      const method = event.requestContext.http.method
      const cors = options.cors ?? false
      if (cors) {
        if (
          cors.credentials &&
          ((typeof cors.origin !== "string" && !Array.isArray(cors.origin)) ||
            cors.origin === "*" ||
            (Array.isArray(cors.origin) && cors.origin.includes("*")))
        )
          throw new Error("Credentialed CORS requires explicit allowed origins")
        const origin = header(event, "origin")
        const allowed =
          cors.origin === "*"
            ? "*"
            : origin &&
                (cors.origin === true ||
                  cors.origin === origin ||
                  (Array.isArray(cors.origin) && cors.origin.includes(origin)))
              ? origin
              : undefined
        if (allowed) {
          response.headers["access-control-allow-origin"] = allowed
          if (cors.credentials)
            response.headers["access-control-allow-credentials"] = "true"
        }
        response.headers.vary = "Origin, Access-Control-Request-Headers"
        response.headers["access-control-allow-methods"] = "GET, POST, OPTIONS"
        const requested = header(event, "access-control-request-headers")
        if (requested)
          response.headers["access-control-allow-headers"] = requested
      }

      if (method === "OPTIONS") return reply(204, "")
      if (
        method === "GET" &&
        options.graphiql &&
        header(event, "accept")
          ?.split(",")
          .some((range) => {
            const [type, ...parameters] = range.trim().toLowerCase().split(";")
            return (
              type.trim() === "text/html" &&
              !parameters.some((parameter) =>
                /^q\s*=\s*0(?:\.0*)?$/.test(parameter.trim()),
              )
            )
          })
      ) {
        response.headers["content-type"] = "text/html; charset=utf-8"
        return reply(
          200,
          graphiqlHtml(
            options.graphiql === true ? {} : options.graphiql,
            event.rawPath,
          ),
        )
      }
      if (method !== "GET" && method !== "POST") {
        response.headers.allow = "GET, POST, OPTIONS"
        return reply(
          405,
          JSON.stringify(
            failure(badRequest("Only GET and POST are supported")),
          ),
        )
      }
      const contentType = header(event, "content-type")
        ?.split(";", 1)[0]
        .trim()
        .toLowerCase()
      const csrfHeaders = options.csrfHeaders ?? [
        "apollo-require-preflight",
        "x-apollo-operation-name",
      ]
      if (
        csrfHeaders.some(
          (name) =>
            name !== "apollo-require-preflight" &&
            !/^x-[a-z0-9-]+$/i.test(name),
        )
      )
        throw new Error("CSRF headers must be explicit non-simple headers")
      if (
        contentType !== "application/json" &&
        !csrfHeaders.some((name) => header(event, name)?.trim())
      )
        return yield* badRequest(
          "Send application/json or a non-empty Apollo-Require-Preflight header",
        )
      if (
        options.healthCheck &&
        event.rawPath === "/.well-known/apollo/server-health"
      ) {
        yield* options.healthCheck
        return reply(200, JSON.stringify({ status: "pass" }))
      }
      let input: unknown
      if (method === "GET") {
        const query = event.queryStringParameters ?? {}
        input = {
          query: query.query,
          ...(query.operationName !== undefined
            ? { operationName: query.operationName }
            : {}),
          ...(query.variables
            ? { variables: yield* parseJson(query.variables) }
            : {}),
          ...(query.extensions
            ? { extensions: yield* parseJson(query.extensions) }
            : {}),
        }
      } else {
        const body = event.isBase64Encoded
          ? Buffer.from(event.body ?? "", "base64").toString("utf8")
          : (event.body ?? "")
        if (Buffer.byteLength(body) > (options.maxBodyBytes ?? 1024 * 1024))
          return yield* badRequest("Request body is too large")
        input = yield* parseJson(body)
      }
      const batch = Array.isArray(input)
      const requests: ReadonlyArray<unknown> = Array.isArray(input)
        ? input
        : [input]
      if (
        requests.length === 0 ||
        requests.length > (options.maxBatchSize ?? 20)
      )
        return yield* badRequest("Invalid batch size")
      const context = yield* Effect.cached(
        Effect.suspend(() =>
          options.context({
            event,
            lambdaContext,
            persisted: {},
            pubSub: operationPubSub().pubSub,
            response,
          }),
        ),
      )
      const results = yield* Effect.forEach(requests, (input) =>
        Effect.gen(function* () {
          const request = yield* decodeRequest(input)
          const prepared = yield* prepare(options.executor, request, options)
          if (prepared.errors.length)
            return {
              status: 400,
              result: { errors: prepared.errors.map(format) },
            }
          if (method === "GET" && prepared.operation?.operation !== "query")
            return {
              status: 405,
              result: failure(badRequest("GET supports queries only")),
            }
          if (prepared.operation?.operation === "subscription")
            return {
              status: 400,
              result: failure(
                badRequest("Subscriptions require WebSocket transport"),
              ),
            }
          const result = yield* options.executor.execute({
            request,
            document: prepared.document,
            context: yield* context,
          })
          return {
            status: result.errors && result.data === undefined ? 400 : 200,
            result,
          }
        }).pipe(
          Effect.catch((error) =>
            Effect.gen(function* () {
              if (
                !(error instanceof TransportError) &&
                !(error instanceof RequestValidationError)
              )
                yield* Effect.logError("GraphQL HTTP operation failed", error)
              return {
                status:
                  error instanceof TransportError
                    ? error.status
                    : error instanceof Error && error.name === "GraphQLError"
                      ? 400
                      : 500,
                result: failure(error),
              }
            }),
          ),
        ),
      )
      const status = batch ? 200 : results[0].status
      if (status === 405) response.headers.allow = "POST"
      return reply(
        status,
        JSON.stringify(
          batch ? results.map(({ result }) => result) : results[0].result,
        ),
      )
    }).pipe(
      Effect.catch((error) =>
        Effect.sync(() =>
          reply(
            error instanceof TransportError ? error.status : 500,
            JSON.stringify(failure(error)),
          ),
        ),
      ),
      Effect.catchCause((cause) =>
        Effect.gen(function* () {
          if (Cause.hasInterrupts(cause)) return yield* Effect.interrupt
          yield* Effect.logError("GraphQL HTTP request failed", cause)
          // This fallback invokes no application formatter or serializer.
          response.headers["content-type"] = "application/json"
          return reply(
            500,
            '{"errors":[{"message":"Internal server error","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}',
          )
        }),
      ),
    )
  })
}
