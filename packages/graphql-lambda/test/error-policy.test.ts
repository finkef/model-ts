import { expect, it } from "@effect/vitest"
import { Effect, Stream } from "effect"
import {
  GraphQLError,
  GraphQLInputObjectType,
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
  GraphQLScalarType,
  GraphQLSchema,
  GraphQLString,
  GraphQLUnionType,
  parse,
} from "graphql"
import {
  createHttpHandler,
  createWebSocketHandler,
  graphqlExecutor,
  formatError,
  Topic,
  type Result,
  TransportError,
} from "../src/index.ts"
import { streamHandler } from "../src/PubSub/DynamoDB.ts"
import { GatewayMessages } from "../src/Sandbox.ts"
import { publish } from "./fixtures.ts"
import {
  connect,
  httpEvent,
  options,
  testLayer,
  wsEvent,
  streamEvent,
} from "./fixtures.ts"

const privateError = () =>
  new GraphQLError("private-token", {
    path: ["leak"],
    extensions: { secret: "private-extension" },
  })

for (const kind of ["raw", "prelocated", "public"] as const) {
  it.effect(
    `custom scalar ${kind} errors follow the same policy for literals and variables`,
    () =>
      Effect.gen(function* () {
        const fail = () => {
          throw kind === "raw"
            ? new Error("db-password=secret")
            : kind === "prelocated"
              ? privateError()
              : new TransportError({
                  message: "Invalid public input",
                  code: "SAFE_INPUT",
                  status: 400,
                })
        }
        const scalar = new GraphQLScalarType({
          name: "LegacyScalar",
          serialize: (value) => value,
          parseValue: fail,
          parseLiteral: fail,
        })
        const input = new GraphQLInputObjectType({
          name: "NestedInput",
          fields: {
            values: { type: new GraphQLList(new GraphQLNonNull(scalar)) },
          },
        })
        const schema = new GraphQLSchema({
          query: new GraphQLObjectType({
            name: "Query",
            fields: {
              echo: {
                type: GraphQLString,
                args: { input: { type: input } },
                resolve: () => "ok",
              },
              builtin: {
                type: GraphQLString,
                args: { text: { type: GraphQLString } },
                resolve: () => "ok",
              },
            },
          }),
        })
        const originalParser = scalar.parseValue
        const executor = graphqlExecutor(schema)
        const handler = createHttpHandler({
          executor,
          context: () => Effect.succeed({}),
        })
        for (const request of [
          {
            query: "query ($input: NestedInput) { echo(input: $input) }",
            variables: { input: { values: ["secret-value"] } },
          },
          { query: '{ echo(input: { values: ["secret-value"] }) }' },
        ]) {
          const response = yield* handler(httpEvent(request), {})
          expect(response.statusCode).toBe(400)
          expect(response.body).not.toContain("db-password")
          expect(response.body).not.toContain("private-token")
          expect(response.body).not.toContain("private-extension")
          const error = JSON.parse(response.body).errors[0]
          expect(error.extensions.code).toBe(
            kind === "public" ? "SAFE_INPUT" : "INTERNAL_SERVER_ERROR",
          )
          expect(error.message).toBe(
            kind === "public"
              ? "Invalid public input"
              : "Internal server error",
          )
        }
        expect(scalar.parseValue).toBe(originalParser)
        expect(executor.schema).not.toBe(schema)
        const builtin = yield* handler(
          httpEvent({ query: "{ builtin(text: 42) }" }),
          {},
        )
        expect(JSON.parse(builtin.body).errors[0].message).toContain("String")
        expect(JSON.parse(builtin.body).errors[0].extensions.code).toBe(
          "GRAPHQL_VALIDATION_FAILED",
        )
      }),
  )
}

it.effect(
  "prelocated resolver, abstract-type and scalar-serialization errors are private",
  () =>
    Effect.gen(function* () {
      const object = new GraphQLObjectType({
        name: "Object",
        fields: { value: { type: GraphQLString } },
      })
      const union = new GraphQLUnionType({
        name: "Union",
        types: [object],
        resolveType: () => {
          throw privateError()
        },
      })
      const scalar = new GraphQLScalarType({
        name: "Output",
        serialize: () => {
          throw privateError()
        },
      })
      const schema = new GraphQLSchema({
        query: new GraphQLObjectType({
          name: "Query",
          fields: {
            resolver: {
              type: GraphQLString,
              resolve: () => {
                throw privateError()
              },
            },
            abstract: { type: union, resolve: () => ({ value: "private" }) },
            scalar: { type: scalar, resolve: () => "private" },
          },
        }),
        subscription: new GraphQLObjectType({
          name: "Subscription",
          fields: {
            source: {
              type: GraphQLString,
              subscribe: () => {
                throw privateError()
              },
            },
          },
        }),
      })
      const executor = graphqlExecutor(schema)
      const handler = createHttpHandler({
        executor,
        context: () => Effect.succeed({}),
      })
      const response = yield* handler(
        httpEvent({
          query: "{ resolver abstract { ... on Object { value } } scalar }",
        }),
        {},
      )
      expect(response.body).not.toContain("private")
      expect(
        JSON.parse(response.body).errors.map(
          (error: { extensions: { code: string } }) => error.extensions.code,
        ),
      ).toEqual([
        "INTERNAL_SERVER_ERROR",
        "INTERNAL_SERVER_ERROR",
        "INTERNAL_SERVER_ERROR",
      ])
      const query = "subscription { source }"
      const result = yield* executor
        .subscribe({ document: parse(query), request: { query }, context: {} })
        .pipe(Stream.runCollect)
      expect(JSON.stringify(result)).not.toContain("private")
      expect(result[0].errors?.[0].extensions?.code).toBe(
        "INTERNAL_SERVER_ERROR",
      )
      expect(formatError(privateError()).extensions?.code).toBe(
        "INTERNAL_SERVER_ERROR",
      )
    }),
)

it.effect(
  "executor-formatted public errors survive HTTP, WebSocket, registration and stream delivery",
  () =>
    Effect.gen(function* () {
      const publicResult: Result = {
        data: { message: null },
        errors: [
          {
            message: "Not allowed",
            path: ["message"],
            extensions: { code: "FORBIDDEN" },
          },
          { message: "Declared failure", extensions: { code: "SAFE_ERROR" } },
        ],
      }
      const executor = {
        ...options.executor,
        execute: () => Effect.succeed(publicResult),
        subscribe: (input: Parameters<typeof options.executor.subscribe>[0]) =>
          input.context.pubSub
            .stream(Topic.make("public"))
            .pipe(Stream.map(() => publicResult)),
      }
      const settings = {
        ...options,
        executor,
        formatError: () => {
          throw new Error("Must not reformat executor results")
        },
      }
      const http = createHttpHandler(settings)
      expect(
        JSON.parse((yield* http(httpEvent({ query: "{ me }" }), {})).body),
      ).toEqual(publicResult)
      yield* connect("c1")
      const ws = createWebSocketHandler(settings)
      yield* ws(
        wsEvent("c1", "$default", {
          type: "subscribe",
          id: "query",
          payload: { query: "{ me }" },
        }),
        {},
      )
      yield* ws(
        wsEvent("c1", "$default", {
          type: "subscribe",
          id: "live",
          payload: { query: 'subscription { message(domain: "a") }' },
        }),
        {},
      )
      yield* streamHandler(settings)(
        streamEvent(yield* publish(Topic.make("public"), {})),
        {},
      )
      const rejected = createWebSocketHandler({
        ...settings,
        executor: {
          ...executor,
          subscribe: () => Stream.succeed(publicResult),
        },
      })
      yield* rejected(
        wsEvent("c1", "$default", {
          type: "subscribe",
          id: "rejected",
          payload: { query: 'subscription { message(domain: "a") }' },
        }),
        {},
      )
      const data = (yield* (yield* GatewayMessages).all)
        .map((frame) => JSON.parse(frame.data))
        .filter((frame) => frame.type === "next")
      expect(data).toHaveLength(3)
      for (const frame of data) expect(frame.payload).toEqual(publicResult)
    }).pipe(Effect.provide(testLayer())),
)

it.effect(
  "the graphql-js adapter masks serialization failures and throwing result formatters",
  () =>
    Effect.gen(function* () {
      const cycle: Record<string, unknown> = {}
      cycle.self = cycle
      for (const value of [1n, cycle]) {
        const scalar = new GraphQLScalarType({
          name: "JSON",
          serialize: () => value,
        })
        const schema = new GraphQLSchema({
          query: new GraphQLObjectType({
            name: "Query",
            fields: { value: { type: scalar, resolve: () => value } },
          }),
        })
        const handler = createHttpHandler({
          executor: graphqlExecutor(schema),
          context: () => Effect.succeed({}),
        })
        const response = yield* handler(httpEvent({ query: "{ value }" }), {})
        expect(response.statusCode).toBe(500)
        expect(JSON.parse(response.body)).toEqual({
          errors: [
            {
              message: "Internal server error",
              extensions: { code: "INTERNAL_SERVER_ERROR" },
            },
          ],
        })
      }
      const handler = createHttpHandler({
        ...options,
        executor: graphqlExecutor(options.executor.schema, {
          formatError: () => {
            throw new Error("private-formatter")
          },
        }),
      })
      const response = yield* handler(httpEvent({ query: "{ secret }" }), {})
      expect(response.statusCode).toBe(500)
      expect(response.body).not.toContain("private-formatter")
    }),
)
