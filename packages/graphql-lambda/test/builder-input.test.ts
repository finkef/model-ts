import { expect, it } from "@effect/vitest"
import {
  createBuilder,
  executor,
  ErrorReporter,
  type ErrorReport,
  type Register,
} from "@model-ts/graphql"
import { Cause, Effect, Schema } from "effect"
import { GraphQLError, GraphQLScalarType, parse } from "graphql"
import {
  GatewayMessages,
  TransportError,
  createHttpHandler,
  createWebSocketHandler,
} from "../src/index.ts"
import { JsonObject } from "../src/Models.ts"
import { httpEvent, testLayer, wsEvent } from "./fixtures.ts"

class PublicInput extends Schema.TaggedError<PublicInput>()("PublicInput", {
  message: Schema.String,
  field: Schema.String,
}) {}

const inputSchema = (reject: () => never) => {
  const codec = Schema.String
  const GraphQL = createBuilder<
    Register<typeof codec, "Checked">
  >().withErrorPolicy({
    public: [PublicInput],
    publicCodes: { PublicInput: "SAFE_INPUT" },
  })
  const scalar = GraphQL.scalar(
    "Checked",
    codec,
    new GraphQLScalarType({
      name: "Checked",
      serialize: (value) => value,
      parseValue: reject,
      parseLiteral: reject,
    }),
  )
  const echo = GraphQL.field("String", {
    args: { value: scalar.schema },
    resolve: () => "unexpected resolution",
  })
  return executor(
    GraphQL.schema({
      scalar,
      query: GraphQL.queryField("echo", echo),
      mutation: GraphQL.mutationField("echo", echo),
    }),
  )
}

for (const rules of [false, true])
  for (const transport of ["HTTP", "WebSocket"] as const) {
    it.effect(
      `${transport} preserves builder scalar policy with application rules ${rules} and reports private causes`,
      () =>
        Effect.gen(function* () {
          let publicInput = true
          let parses = 0
          let ruleCalls = 0
          const secret = new Error("private scalar backend")
          const executor = inputSchema(() => {
            parses++
            throw publicInput
              ? new PublicInput({
                  message: "Choose another value",
                  field: "input",
                })
              : secret
          })
          const options = {
            persistedContext: JsonObject,
            executor,
            context: () => Effect.succeed({}),
            validationRules: rules
              ? [
                  () => {
                    ruleCalls++
                    return {}
                  },
                ]
              : [],
            formatError: () => {
              throw new Error(
                "Executor errors must not reach the transport formatter",
              )
            },
          }
          const http = createHttpHandler(options)
          const websocket = createWebSocketHandler(options)
          if (transport === "WebSocket") {
            yield* websocket(wsEvent("input", "$connect"), {})
            yield* websocket(
              wsEvent("input", "$default", { type: "connection_init" }),
              {},
            )
          }
          const reports: ErrorReport[] = []
          const reporter = {
            report: (report: ErrorReport) =>
              Effect.sync(() => {
                reports.push(report)
              }),
          }
          for (const visible of [true, false]) {
            publicInput = visible
            for (const variable of [true, false]) {
              const request = variable
                ? {
                    query: "query($value:Checked!){echo(value:$value)}",
                    variables: { value: "bad" },
                  }
                : { query: '{echo(value:"bad")}' }
              const expected = yield* executor
                .execute({
                  document: parse(request.query),
                  request,
                  context: {},
                })
                .pipe(Effect.provideService(ErrorReporter, reporter))
              reports.length = 0
              parses = 0
              ruleCalls = 0
              const result = yield* Effect.gen(function* () {
                if (transport === "HTTP") {
                  const response = yield* http(httpEvent(request), {})
                  expect(response.statusCode).toBe(400)
                  return JSON.parse(response.body)
                }
                yield* websocket(
                  wsEvent("input", "$default", {
                    type: "subscribe",
                    id: "q",
                    payload: request,
                  }),
                  {},
                )
                const frame = JSON.parse(
                  (yield* (yield* GatewayMessages).all).at(-1)!.data,
                )
                expect(frame.type).toBe("error")
                return { errors: frame.payload }
              }).pipe(Effect.provideService(ErrorReporter, reporter))
              expect(result).toEqual(expected)
              expect(expected.errors?.[0].locations).toHaveLength(1)
              expect(expected.errors?.[0]).toMatchObject(
                visible
                  ? {
                      message: "Choose another value",
                      extensions: { code: "SAFE_INPUT", field: "input" },
                    }
                  : {
                      message: "Internal server error",
                      extensions: { code: "INTERNAL_SERVER_ERROR" },
                    },
              )
              // A configured application rule requires a standard-validation gate first.
              expect(parses).toBe(rules && !variable ? 2 : 1)
              expect(ruleCalls).toBe(rules && variable ? 1 : 0)
              expect(reports).toHaveLength(visible ? 0 : 1)
              if (!visible) expect(Cause.squash(reports[0].cause)).toBe(secret)
              expect(JSON.stringify(result)).not.toContain(secret.message)
            }
          }
        }).pipe(Effect.provide(testLayer())),
    )
  }

it.effect(
  "executor validation ownership retains HTTP and application request guards before scalar execution",
  () =>
    Effect.gen(function* () {
      let parsed = 0
      let contexts = 0
      const executor = inputSchema(() => {
        parsed++
        throw new Error("must not parse")
      })
      const options = {
        executor,
        context: () =>
          Effect.sync(() => {
            contexts++
            return {}
          }),
      }
      const http = createHttpHandler({ ...options, introspection: false })
      expect(
        (yield* http(
          httpEvent(
            {},
            {
              requestContext: { http: { method: "GET" } },
              queryStringParameters: { query: 'mutation {echo(value:"bad")}' },
            },
          ),
          {},
        )).statusCode,
      ).toBe(405)
      expect(
        (yield* http(httpEvent({ query: "{__schema{queryType{name}}}" }), {}))
          .statusCode,
      ).toBe(400)
      expect((yield* http(httpEvent({ query: "{" }), {})).statusCode).toBe(400)
      const limited = createHttpHandler({ ...options, maxQueryBytes: 1 })
      expect(
        (yield* limited(httpEvent({ query: "{echo}" }), {})).statusCode,
      ).toBe(400)
      const restricted = createHttpHandler({
        ...options,
        validationRules: [
          (context) => ({
            Field(node) {
              if (node.name.value === "echo")
                context.reportError(
                  new GraphQLError("Application rule rejected this field", {
                    nodes: node,
                    originalError: new TransportError({
                      message: "Application rule rejected this field",
                      code: "FIELD_DISABLED",
                      status: 400,
                    }),
                  }),
                )
            },
          }),
        ],
      })
      const response = yield* restricted(
        httpEvent({
          query: "query($value:Checked!){echo(value:$value)}",
          variables: { value: "bad" },
        }),
        {},
      )
      expect(response.statusCode).toBe(400)
      expect(JSON.parse(response.body).errors[0].extensions.code).toBe(
        "FIELD_DISABLED",
      )
      expect(parsed).toBe(0)
      expect(contexts).toBe(0)
    }),
)
