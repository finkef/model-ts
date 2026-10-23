import { expect, it } from "@effect/vitest"
import { createBuilder, executor } from "@model-ts/graphql"
import { Cause, Clock, Effect, Logger, Stream } from "effect"
import {
  GraphQLObjectType,
  GraphQLSchema,
  GraphQLString,
  Kind,
  SingleFieldSubscriptionsRule,
  parse,
  validate,
  type SelectionSetNode,
  type ValidationRule,
} from "graphql"
import {
  ConnectionId,
  OperationId,
  Topic,
  GatewayMessages,
  createHttpHandler,
  createWebSocketHandler,
  graphqlExecutor,
  type ContextRequest,
  type PubSub,
  type Executor,
  ExecutionError,
  type WebSocketEvent,
  type ProxyResponse,
  type Gateway,
} from "../src/index.ts"
import { JsonObject } from "../src/Models.ts"
import { Storage } from "../src/Storage.ts"
import { httpEvent, testLayer, wsEvent } from "./fixtures.ts"

const topic = Topic.make("validation/events")
type AppContext = { readonly pubSub: PubSub.Source }
const fixture = (kind: "builder" | "plain") => {
  const calls = { contexts: 0, sources: 0, resolvers: 0 }
  let adapter: Executor<AppContext, ExecutionError>
  if (kind === "builder") {
    const GraphQL = createBuilder<{}, AppContext>()
    adapter = executor(
      GraphQL.schema({
        ping: GraphQL.queryField("ping", "String", () => {
          calls.resolvers++
          return "pong"
        }),
        event: GraphQL.subscriptionField("event", "String", {
          subscribe: (_, __, context) => {
            calls.sources++
            return context.pubSub.stream(topic).pipe(Stream.map(String))
          },
        }),
      }),
    )
  } else {
    adapter = graphqlExecutor(
      new GraphQLSchema({
        query: new GraphQLObjectType({
          name: "Query",
          fields: {
            ping: {
              type: GraphQLString,
              resolve: () => {
                calls.resolvers++
                return "pong"
              },
            },
          },
        }),
        subscription: new GraphQLObjectType<unknown, AppContext>({
          name: "Subscription",
          fields: {
            event: {
              type: GraphQLString,
              subscribe: (_, __, context) => {
                calls.sources++
                return context.pubSub.subscribe(topic)
              },
              resolve: String,
            },
          },
        }),
      }),
    )
  }
  return {
    calls,
    options: {
      persistedContext: JsonObject,
      executor: adapter,
      context: (request: ContextRequest<{}>) => {
        calls.contexts++
        return Effect.succeed({ pubSub: request.pubSub })
      },
    },
  }
}
type Handler = (
  event: WebSocketEvent,
  context: {},
) => Effect.Effect<ProxyResponse, never, Storage | Gateway>
const start = (handler: Handler, query: string, operationName?: string) =>
  handler(
    wsEvent("validation", "$default", {
      type: "subscribe",
      id: "operation",
      payload: { query, operationName },
    }),
    {},
  )
const connect = (handler: Handler) =>
  Effect.gen(function* () {
    yield* handler(wsEvent("validation", "$connect"), {})
    yield* handler(
      wsEvent("validation", "$default", { type: "connection_init" }),
      {},
    )
  })
const noRegistration = Effect.gen(function* () {
  const storage = yield* Storage
  expect(
    yield* storage.operation(
      ConnectionId.make("validation"),
      OperationId.make("operation"),
    ),
  ).toBeUndefined()
  expect(
    yield* Stream.runCollect(
      storage.subscribers(topic, yield* Clock.currentTimeMillis),
    ),
  ).toHaveLength(0)
})

// This visitor requires fragment existence and acyclicity.
// Running it before standard validation would throw or exhaust the call stack.
const assumesValidFragments =
  (called: () => void): ValidationRule =>
  (context) => ({
    OperationDefinition(operation) {
      called()
      const walk = (set: SelectionSetNode): void => {
        for (const selection of set.selections) {
          if (selection.kind === Kind.FRAGMENT_SPREAD) {
            const fragment = context.getFragment(selection.name.value)
            if (!fragment)
              throw new Error("Depth visitor received an unknown fragment")
            walk(fragment.selectionSet)
          } else if (selection.selectionSet) walk(selection.selectionSet)
        }
      }
      walk(operation.selectionSet)
    },
  })

for (const kind of ["builder", "plain"] as const) {
  it.effect(
    `${kind} rejects fragment-hidden multiple roots and introspection before registration with GraphQL 16 diagnostics`,
    () =>
      Effect.gen(function* () {
        const { options, calls } = fixture(kind)
        const handler = createWebSocketHandler(options)
        yield* connect(handler)
        const cases = [
          {
            query:
              "subscription { ...Many }\nfragment Many on Subscription {\n  a:event\n  b:event\n}",
            errors: [
              {
                message:
                  "Anonymous Subscription must select only one top level field.",
                locations: [{ line: 4, column: 3 }],
              },
            ],
          },
          {
            query:
              "subscription Named { ... on Subscription {a:event ...More} }\nfragment More on Subscription {b:event}",
            errors: [
              {
                message:
                  'Subscription "Named" must select only one top level field.',
                locations: [{ line: 2, column: 32 }],
              },
            ],
          },
          {
            query: "subscription { __typename }",
            errors: [
              {
                message:
                  "Anonymous Subscription must not select an introspection top level field.",
                locations: [{ line: 1, column: 16 }],
              },
            ],
          },
          {
            query:
              "subscription Named { ...Type }\nfragment Type on Subscription {kind:__typename}",
            errors: [
              {
                message:
                  'Subscription "Named" must not select an introspection top level field.',
                locations: [{ line: 2, column: 32 }],
              },
            ],
          },
        ]
        for (const test of cases) {
          expect(
            validate(options.executor.schema, parse(test.query), [
              SingleFieldSubscriptionsRule,
            ]).map((error) => error.toJSON()),
          ).toEqual(test.errors)
          const response = yield* start(handler, test.query)
          expect(response.statusCode).toBe(200)
          const frames = yield* (yield* GatewayMessages).all
          expect(JSON.parse(frames[frames.length - 1].data)).toEqual({
            type: "error",
            id: "operation",
            payload: test.errors.map((error) => ({
              ...error,
              extensions: { code: "GRAPHQL_VALIDATION_FAILED" },
            })),
          })
          yield* noRegistration
        }
        expect(calls).toEqual({ contexts: 0, sources: 0, resolvers: 0 })

        // Same response key and repeated spreads still collect one root.
        yield* start(
          handler,
          "subscription One { ...Only } fragment Only on Subscription { event event ...Again ...Again } fragment Again on Subscription {event}",
          "One",
        )
        const storage = yield* Storage
        expect(
          (yield* storage.operation(
            ConnectionId.make("validation"),
            OperationId.make("operation"),
          ))?.state,
        ).toBe("active")
        expect(
          yield* Stream.runCollect(
            storage.subscribers(topic, yield* Clock.currentTimeMillis),
          ),
        ).toHaveLength(1)
        expect(calls.sources).toBe(1)
      }).pipe(Effect.provide(testLayer())),
  )

  for (const directive of ["skip", "include"]) {
    for (const value of ["$condition", "true", "false"]) {
      const declaration = value === "$condition" ? "($condition:Boolean!)" : ""
      const condition = `@${directive}(if:${value})`
      const selections = {
        field: `{event ${condition}}`,
        "field in fragment": `{...Root} fragment Root on Subscription {event ${condition}}`,
        spread: `{...Root ${condition}} fragment Root on Subscription {event}`,
        "inline fragment": `{... on Subscription ${condition} {event}}`,
        "inline fragment in fragment": `{...Root} fragment Root on Subscription {... on Subscription ${condition} {event}}`,
      }
      for (const [location, selection] of Object.entries(selections)) {
        for (const transport of ["HTTP", "WebSocket"]) {
          it.effect(
            `${kind} ${transport} rejects root ${condition} on ${location}`,
            () => {
              const { options, calls } = fixture(kind)
              const query = `subscription Conditional${declaration} ${selection}`
              const request = { query, variables: { condition: true } }
              const expected = [
                {
                  message:
                    'Subscription "Conditional" must not use `@skip` or `@include` directives in the top level selection.',
                  locations: [{ line: 1, column: query.indexOf("@") + 1 }],
                  extensions: { code: "GRAPHQL_VALIDATION_FAILED" },
                },
              ]
              if (transport === "HTTP")
                return createHttpHandler(options)(httpEvent(request), {}).pipe(
                  Effect.map((response) => {
                    expect(response.statusCode).toBe(400)
                    expect(JSON.parse(response.body).errors).toEqual(expected)
                    expect(calls).toEqual({
                      contexts: 0,
                      sources: 0,
                      resolvers: 0,
                    })
                  }),
                )
              return Effect.gen(function* () {
                const websocket = createWebSocketHandler(options)
                yield* connect(websocket)
                const response = yield* websocket(
                  wsEvent("validation", "$default", {
                    type: "subscribe",
                    id: "operation",
                    payload: request,
                  }),
                  {},
                )
                expect(response.statusCode).toBe(200)
                const frames = yield* (yield* GatewayMessages).all
                expect(JSON.parse(frames[frames.length - 1].data)).toEqual({
                  type: "error",
                  id: "operation",
                  payload: expected,
                })
                yield* noRegistration
                expect(calls).toEqual({ contexts: 0, sources: 0, resolvers: 0 })
              }).pipe(Effect.provide(testLayer()))
            },
          )
        }
      }
    }
  }

  it.effect(
    `${kind} gates application rules on standard validation for HTTP and WebSocket`,
    () =>
      Effect.gen(function* () {
        const { options, calls } = fixture(kind)
        let ruleCalls = 0
        const configured = {
          ...options,
          validationRules: [
            assumesValidFragments(() => {
              ruleCalls++
            }),
          ],
        }
        const http = createHttpHandler(configured)
        const websocket = createWebSocketHandler(configured)
        yield* connect(websocket)
        for (const operation of ["query", "subscription"]) {
          const type = operation === "query" ? "Query" : "Subscription"
          for (const test of [
            {
              query: `${operation} {...Missing}`,
              message: 'Unknown fragment "Missing".',
            },
            {
              query: `${operation} {...Loop} fragment Loop on ${type} {...Loop}`,
              message: 'Cannot spread fragment "Loop" within itself.',
            },
          ]) {
            // HTTP subscription-kind rejection precedes execution; query diagnostics come from validation.
            if (operation === "query") {
              const response = yield* http(httpEvent({ query: test.query }), {})
              expect(response.statusCode).toBe(400)
              expect(JSON.parse(response.body).errors).toContainEqual(
                expect.objectContaining({ message: test.message }),
              )
            }
            expect((yield* start(websocket, test.query)).statusCode).toBe(200)
            const frames = yield* (yield* GatewayMessages).all
            const replies = frames
              .map((frame) => JSON.parse(frame.data))
              .filter(
                (frame) => frame.type === "next" || frame.type === "error",
              )
            const reply = replies[replies.length - 1]
            expect(
              reply.type === "error" ? reply.payload : reply.payload.errors,
            ).toContainEqual(expect.objectContaining({ message: test.message }))
            yield* noRegistration
            expect(ruleCalls).toBe(0)
            expect(calls.sources).toBe(0)
            expect(calls.resolvers).toBe(0)
          }
        }
        const response = yield* http(
          httpEvent({ query: "{...Known} fragment Known on Query {ping}" }),
          {},
        )
        expect(JSON.parse(response.body)).toEqual({ data: { ping: "pong" } })
        expect(ruleCalls).toBe(1)
      }).pipe(Effect.provide(testLayer())),
  )

  it.effect(
    `${kind} masks and logs thrown application rules on both transports`,
    () =>
      Effect.gen(function* () {
        const { options, calls } = fixture(kind)
        const secret = new Error("private application validator crash")
        const causes: Cause.Cause<unknown>[] = []
        const configured = {
          ...options,
          validationRules: [
            (): ReturnType<ValidationRule> => {
              throw secret
            },
          ],
        }
        yield* Effect.gen(function* () {
          const response = yield* createHttpHandler(configured)(
            httpEvent({ query: "{ping}" }),
            {},
          )
          expect(response.statusCode).toBe(500)
          expect(JSON.parse(response.body).errors).toEqual([
            {
              message: "Internal server error",
              extensions: { code: "INTERNAL_SERVER_ERROR" },
            },
          ])
          expect(causes.map(Cause.squash)).toEqual([secret])
          causes.length = 0
          const websocket = createWebSocketHandler(configured)
          yield* connect(websocket)
          yield* start(websocket, "subscription {event}")
          const frames = yield* (yield* GatewayMessages).all
          expect(JSON.parse(frames[frames.length - 1].data)).toEqual({
            type: "error",
            id: "operation",
            payload: [
              {
                message: "Internal server error",
                extensions: { code: "INTERNAL_SERVER_ERROR" },
              },
            ],
          })
          yield* noRegistration
        }).pipe(
          Effect.provide(
            Logger.layer([
              Logger.make(({ cause }) => {
                causes.push(cause)
              }),
            ]),
          ),
        )
        expect(causes.map(Cause.squash)).toEqual([secret])
        expect(calls).toEqual({ contexts: 0, sources: 0, resolvers: 0 })
      }).pipe(Effect.provide(testLayer())),
  )
}

it.effect("allows conditional directives below the subscription root", () =>
  Effect.gen(function* () {
    const { options } = fixture("plain")
    const nested = new GraphQLObjectType({
      name: "Nested",
      fields: { text: { type: GraphQLString } },
    })
    const executor = graphqlExecutor<AppContext>(
      new GraphQLSchema({
        query: options.executor.schema.getQueryType(),
        subscription: new GraphQLObjectType<unknown, AppContext>({
          name: "Subscription",
          fields: {
            event: {
              type: nested,
              subscribe: (_, __, context) => context.pubSub.subscribe(topic),
            },
          },
        }),
      }),
    )
    const websocket = createWebSocketHandler({ ...options, executor })
    yield* connect(websocket)
    yield* websocket(
      wsEvent("validation", "$default", {
        type: "subscribe",
        id: "operation",
        payload: {
          query:
            "subscription Nested($condition:Boolean!){event {...Content @include(if:$condition)}} fragment Content on Nested {text @skip(if:false)}",
          variables: { condition: true },
        },
      }),
      {},
    )
    const storage = yield* Storage
    expect(
      (yield* storage.operation(
        ConnectionId.make("validation"),
        OperationId.make("operation"),
      ))?.state,
    ).toBe("active")
    expect(
      yield* Stream.runCollect(
        storage.subscribers(topic, yield* Clock.currentTimeMillis),
      ),
    ).toHaveLength(1)
  }).pipe(Effect.provide(testLayer())),
)

it.effect("HTTP still logs executor failures outside request validation", () =>
  Effect.gen(function* () {
    const { options } = fixture("plain")
    const failure = new ExecutionError({ cause: new Error("executor failed") })
    const records: unknown[] = []
    const http = createHttpHandler({
      ...options,
      executor: { ...options.executor, execute: () => Effect.fail(failure) },
    })
    const response = yield* http(httpEvent({ query: "{ping}" }), {}).pipe(
      Effect.provide(
        Logger.layer([
          Logger.make(({ message }) => {
            records.push(message)
          }),
        ]),
      ),
    )
    expect(response.statusCode).toBe(500)
    expect(records).toEqual([["GraphQL HTTP operation failed", failure]])
  }),
)
