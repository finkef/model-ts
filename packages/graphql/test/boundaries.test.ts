import { expect, it } from "@effect/vitest"
import { Cause, Deferred, Effect, Fiber, Schema, Stream } from "effect"
import { GraphQLError, GraphQLScalarType, Kind, parse } from "graphql"
import * as G from "../src/GraphQL.ts"

class PublicInput extends Schema.TaggedError<PublicInput>()("PublicInput", {
  message: Schema.String,
  field: Schema.String,
}) {}
const secret = new Error("db-password=secret")
const publicInput = new PublicInput({
  message: "Choose another value",
  field: "value",
})
const prelocated = () =>
  new GraphQLError("private-token", {
    path: ["leak"],
    extensions: { secret: "private-extension", code: "PRETEND_PUBLIC" },
  })
const throwingScalar = () =>
  new GraphQLScalarType({
    name: "Custom",
    serialize: (value) => value,
    parseValue: (value) => {
      if (value === "public") throw publicInput
      if (value === "graphql") throw prelocated()
      throw secret
    },
    parseLiteral: (node) => {
      if (node.kind === Kind.STRING && node.value === "public")
        throw publicInput
      if (node.kind === Kind.STRING && node.value === "graphql")
        throw prelocated()
      throw secret
    },
  })

for (const operation of ["query", "subscription"] as const) {
  it.effect(
    `adopted ${operation}: scalar coercion preserves only declared public failures`,
    () =>
      Effect.gen(function* () {
        const scalarSchema = Schema.String
        const builder = G.createBuilder<
          G.Register<typeof scalarSchema, "Custom">
        >().withErrorPolicy({
          public: [PublicInput],
          publicCodes: { PublicInput: "BAD_USER_INPUT" },
        })
        const custom = throwingScalar()
        const scalar = builder.scalar("Custom", scalarSchema, custom)
        const query = builder.query({
          ping: builder.fields.field("String", () => "ok"),
        })
        const field = { args: { value: scalar.schema } }
        const built = builder.schema({
          scalar,
          query,
          echo: builder.query({
            echo: builder.fields.field("String", {
              ...field,
              resolve: () => "ok",
            }),
          }),
          events: builder.subscription({
            echo: builder.fields.events("String", {
              ...field,
              subscribe: () => Stream.succeed("ok"),
            }),
          }),
        })
        const reports: G.ErrorReport[] = []
        for (const variables of [false, true]) {
          for (const value of ["raw", "graphql", "public"]) {
            const source = variables
              ? `${operation}($value:Custom!){echo(value:$value)}`
              : `${operation}{echo(value:"${value}")}`
            const request = { source, variableValues: { value } }
            const result = yield* (
              operation === "query"
                ? G.execute(built, request)
                : Stream.runCollect(G.subscribe(built, request)).pipe(
                    Effect.map((results) => results[0]!),
                  )
            ).pipe(
              Effect.provideService(G.ErrorReporter, {
                report: (event) =>
                  Effect.sync(() => {
                    reports.push(event)
                  }),
              }),
            )
            const error = result.errors?.[0]
            expect(error?.message).toBe(
              value === "public"
                ? "Choose another value"
                : "Internal server error",
            )
            expect(error?.extensions).toEqual(
              value === "public"
                ? { field: "value", code: "BAD_USER_INPUT" }
                : { code: "INTERNAL_SERVER_ERROR" },
            )
            expect(JSON.stringify(result)).not.toMatch(
              /db-password|private-token|private-extension/,
            )
            expect(error?.locations?.length).toBeGreaterThan(0)
          }
        }
        expect(reports).toHaveLength(4)
        expect(
          reports.some((report) => Cause.squash(report.cause) === secret),
        ).toBe(true)
      }),
  )
}

it.effect(
  "ordinary document and built-in input diagnostics remain useful",
  () =>
    Effect.gen(function* () {
      const builder = G.createBuilder<{}>()
      const schema = builder.schema({
        query: builder.query({
          echo: builder.fields.field("String", {
            args: { value: Schema.String },
            resolve: (_, { value }) => value,
          }),
        }),
      })
      for (const request of [
        { source: "{missing}", message: 'Cannot query field "missing"' },
        {
          source: "{echo(value:123)}",
          message: "String cannot represent a non string value",
        },
        {
          source: "query($value:String!){echo(value:$value)}",
          variableValues: { value: 123 },
          message: "String cannot represent a non string value",
        },
        { source: "{", message: "Syntax Error" },
      ]) {
        expect(
          (yield* G.execute(schema, request)).errors?.[0]?.message,
        ).toContain(request.message)
      }
    }),
)

it.effect(
  "builder resolver, abstract resolver and serialization exceptions obey the public policy",
  () =>
    Effect.gen(function* () {
      class Node extends Schema.TaggedClass<Node>()("Node", {
        id: Schema.String,
      }) {}
      class Other extends Schema.TaggedClass<Other>()("Other", {
        id: Schema.String,
      }) {}
      const abstract = Schema.Union([Node, Other])
      const output = Schema.String
      for (const error of [prelocated(), publicInput]) {
        const builder = G.createBuilder<
          G.Register<typeof Node> &
            G.Register<typeof Other> &
            G.Register<typeof abstract, "Abstract"> &
            G.Register<typeof output, "Output">
        >().withErrorPolicy({ public: [PublicInput] })
        const schema = builder.schema({
          node: builder.object(Node, { id: builder.fields.id() }),
          other: builder.object(Other, { id: builder.fields.id() }),
          abstract: builder.union("Abstract", abstract, {
            members: ["Node", "Other"],
            resolveType: () => {
              throw error
            },
          }),
          output: builder.scalar(
            "Output",
            output,
            new GraphQLScalarType({
              name: "Output",
              serialize: () => {
                throw error
              },
            }),
          ),
          query: builder.query({
            leak: builder.fields.nullable("String", () => {
              throw error
            }),
            abstract: builder.fields.nullable(
              "Abstract",
              () => new Node({ id: "id" }),
            ),
            scalar: builder.fields.nullable("Output", () => "value"),
          }),
        })
        const result = yield* G.execute(schema, {
          source: "{leak abstract{__typename} scalar}",
        })
        expect(result.errors).toHaveLength(3)
        for (const actual of result.errors ?? []) {
          expect(actual.message).toBe(
            error === publicInput
              ? publicInput.message
              : "Internal server error",
          )
          expect(actual.extensions).toEqual(
            error === publicInput
              ? { field: "value", code: "PublicInput" }
              : { code: "INTERNAL_SERVER_ERROR" },
          )
        }
        expect(JSON.stringify(result)).not.toMatch(
          /private-token|private-extension/,
        )
      }
    }),
)

it.effect(
  "executor formats public query and subscription errors for an independent transport",
  () =>
    Effect.gen(function* () {
      const builder = G.createBuilder<{}>().withErrorPolicy({
        public: [PublicInput],
        publicCodes: { PublicInput: "SAFE_INPUT" },
      })
      const adapter = G.executor(
        builder.schema({
          query: builder.query({
            denied: builder.fields.nullable("String", {
              authorize: () => false,
              resolve: () => "no",
            }),
            public: builder.fields.nullable("String", () =>
              Effect.fail(publicInput),
            ),
            json: builder.fields.field("JSON", () => ({
              at: new Date("2026-10-23T00:00:00Z"),
              omitted: undefined,
            })),
          }),
          subscription: builder.subscription({
            public: builder.fields.events("String", {
              subscribe: () => Stream.fail(publicInput),
            }),
          }),
        }),
      )
      const query = "{denied public json}"
      const result = yield* adapter.execute({
        document: parse(query),
        request: { query },
        context: {},
      })
      expect(result.errors?.map((error) => error.extensions?.code)).toEqual([
        "FORBIDDEN",
        "SAFE_INPUT",
      ])
      expect(result.data?.json).toEqual({ at: "2026-10-23T00:00:00.000Z" })
      expect(result.errors?.[1]?.message).toBe("Choose another value")
      expect(result.errors?.[1]?.locations).toEqual([{ line: 1, column: 9 }])
      const subscription = "subscription {public}"
      const events = yield* Stream.runCollect(
        adapter.subscribe({
          document: parse(subscription),
          request: { query: subscription },
          context: {},
        }),
      )
      expect(events[0]?.errors?.[0]?.extensions?.code).toBe("SAFE_INPUT")
      expect(events[0]?.errors?.[0]?.locations).toEqual([
        { line: 1, column: 15 },
      ])
      for (const response of [result, ...events]) {
        expect(response).toEqual(JSON.parse(JSON.stringify(response)))
        for (const error of response.errors ?? []) {
          expect(Object.getPrototypeOf(error)).toBe(Object.prototype)
          expect(error).not.toHaveProperty("originalError")
          expect(error).not.toHaveProperty("stack")
        }
      }
    }),
)

it.effect(
  "executor masks and reports values that cannot be serialized as JSON",
  () =>
    Effect.gen(function* () {
      const builder = G.createBuilder<{}>()
      const cycle: { self?: unknown } = {}
      cycle.self = cycle
      for (const value of [1n, cycle]) {
        const adapter = G.executor(
          builder.schema({
            query: builder.query({
              value: builder.fields.field("JSON", () => value),
            }),
            subscription: builder.subscription({
              value: builder.fields.events("JSON", {
                subscribe: () => Stream.succeed(value),
              }),
            }),
          }),
        )
        for (const operation of ["query", "subscription"]) {
          const source = `${operation}{value}`
          const input = {
            document: parse(source),
            request: { query: source },
            context: {},
          }
          const reports: G.ErrorReport[] = []
          const result = yield* (
            operation === "query"
              ? adapter.execute(input)
              : Stream.runCollect(adapter.subscribe(input)).pipe(
                  Effect.map((events) => events[0]!),
                )
          ).pipe(
            Effect.provideService(G.ErrorReporter, {
              report: (event) =>
                Effect.sync(() => {
                  reports.push(event)
                }),
            }),
          )
          expect(result).toEqual({
            errors: [
              {
                message: "Internal server error",
                extensions: { code: "INTERNAL_SERVER_ERROR" },
              },
            ],
          })
          expect(reports).toHaveLength(1)
          expect(Cause.squash(reports[0]!.cause)).toBeInstanceOf(TypeError)
        }
      }
    }),
)

for (const phase of ["authorize", "resolve"] as const) {
  it.effect(
    `subscription interruption awaits in-flight event ${phase} finalizers`,
    () =>
      Effect.gen(function* () {
        class Node extends Schema.TaggedClass<Node>()("Node", {
          id: Schema.String,
        }) {}
        const builder = G.createBuilder<G.Register<typeof Node>>()
        const entered = yield* Deferred.make<void>()
        const finalizing = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        let finalized = false
        let sourceFinalized = false
        const blocked = Effect.gen(function* () {
          yield* Deferred.succeed(entered, undefined)
          return yield* Effect.never
        }).pipe(
          Effect.ensuring(
            Effect.gen(function* () {
              yield* Deferred.succeed(finalizing, undefined)
              yield* Deferred.await(release)
              finalized = true
            }),
          ),
        )
        const schema = builder.schema({
          node: builder.object(Node, {
            id: builder.fields.field("String", () =>
              phase === "resolve" ? blocked : "id",
            ),
          }),
          query: builder.query({
            ping: builder.fields.field("String", () => "ok"),
          }),
          subscription: builder.subscription({
            event: builder.fields.events("Node", {
              subscribe: () =>
                Stream.succeed(new Node({ id: "id" })).pipe(
                  Stream.ensuring(
                    Effect.sync(() => {
                      sourceFinalized = true
                    }),
                  ),
                ),
              authorize: () => (phase === "authorize" ? blocked : true),
            }),
          }),
        })
        const fiber = yield* G.subscribe(schema, {
          source: "subscription{event{id}}",
        }).pipe(Stream.runDrain, Effect.forkChild)
        yield* Deferred.await(entered)
        const cancellation = yield* Fiber.interrupt(fiber).pipe(
          Effect.forkChild,
        )
        yield* Deferred.await(finalizing)
        expect(cancellation.pollUnsafe()).toBeUndefined()
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(cancellation).pipe(Effect.timeout("2 seconds"))
        expect(finalized).toBe(true)
        expect(sourceFinalized).toBe(true)
      }),
  )
}

it.effect(
  "subscription cancellation awaits source finalization during a pending pull",
  () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>()
      const finalizing = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const finished = yield* Deferred.make<void>()
      const source = Stream.fromEffect(
        Effect.andThen(Deferred.succeed(entered, undefined), Effect.never),
      ).pipe(
        Stream.ensuring(
          Effect.gen(function* () {
            yield* Deferred.succeed(finalizing, undefined)
            yield* Deferred.await(release)
            yield* Deferred.succeed(finished, undefined)
          }),
        ),
      )
      const builder = G.createBuilder<{}>()
      const schema = builder.schema({
        query: builder.query({
          ping: builder.fields.field("String", () => "ok"),
        }),
        subscription: builder.subscription({
          event: builder.fields.events("String", { subscribe: () => source }),
        }),
      })
      const fiber = yield* G.subscribe(schema, {
        source: "subscription{event}",
      }).pipe(Stream.runDrain, Effect.forkChild)
      yield* Deferred.await(entered)
      const cancellation = yield* Fiber.interrupt(fiber).pipe(Effect.forkChild)
      yield* Deferred.await(finalizing)
      // Let cancellation finish if it is only waiting for the resolver promises.
      yield* Effect.yieldNow
      const completedEarly = cancellation.pollUnsafe() !== undefined
      yield* Deferred.succeed(release, undefined)
      yield* Deferred.await(finished)
      yield* Fiber.join(cancellation)
      expect(completedEarly).toBe(false)
    }),
)

it.effect(
  "registered typed-error fields receive their class instance, including methods and getters",
  () =>
    Effect.gen(function* () {
      class Problem extends Schema.TaggedError<Problem>()("Problem", {
        message: Schema.String,
      }) {
        label() {
          return `Problem: ${this.message}`
        }
        get display() {
          return this.label().toUpperCase()
        }
      }
      class Node extends Schema.TaggedClass<Node>()("Node", {
        id: Schema.String,
      }) {}
      const problem = new Problem({ message: "bad input" })
      Object.assign(problem, {
        secret: "private-field",
        cause: new Error("private-cause"),
      })
      const builder = G.createBuilder<
        G.Register<typeof Problem> & G.Register<typeof Node>
      >()
      const schema = builder.schema({
        node: builder.object(Node, { id: builder.fields.id() }),
        problem: builder.object(Problem, {
          message: builder.fields.string(),
          display: builder.fields.string(),
          label: builder.fields.field("String", {
            authorize: (parent) => parent === problem,
            resolve: (parent) => parent.label(),
          }),
        }),
        query: builder.query({
          result: builder.fields.field("Node", {
            errors: [Problem],
            resolve: () => Effect.fail(problem),
          }),
        }),
      })
      const result = yield* G.execute(schema, {
        source: "{result{__typename ...on Problem{message label display}}}",
      })
      expect(result).toEqual({
        data: {
          result: {
            __typename: "Problem",
            message: "bad input",
            label: "Problem: bad input",
            display: "PROBLEM: BAD INPUT",
          },
        },
      })
      expect(JSON.stringify(result)).not.toMatch(
        /private-field|private-cause|stack/,
      )
    }),
)

it.effect(
  "codec scalars distinguish invalid inputs from thrown application failures",
  () =>
    Effect.gen(function* () {
      const custom = Schema.String.check(
        Schema.makeFilter((value) => {
          if (value === "raw") throw secret
          if (value === "public") throw publicInput
          return value === "valid"
        }),
      )
      const builder = G.createBuilder<
        G.Register<typeof custom, "Checked">
      >().withErrorPolicy({ public: [PublicInput] })
      const scalar = builder.scalar("Checked", custom)
      const schema = builder.schema({
        scalar,
        query: builder.query({
          echo: builder.fields.field("String", {
            args: { value: scalar.schema },
            resolve: (_, { value }) => value,
          }),
        }),
      })
      for (const variables of [false, true]) {
        for (const value of ["raw", "public", "invalid"]) {
          const reports: G.ErrorReport[] = []
          const result = yield* G.execute(schema, {
            source: variables
              ? "query($value:Checked!){echo(value:$value)}"
              : `{echo(value:"${value}")}`,
            variableValues: { value },
          }).pipe(
            Effect.provideService(G.ErrorReporter, {
              report: (event) =>
                Effect.sync(() => {
                  reports.push(event)
                }),
            }),
          )
          expect(result.errors?.[0]?.message).toBe(
            value === "raw"
              ? "Internal server error"
              : value === "public"
                ? "Choose another value"
                : "Invalid value for Checked",
          )
          expect(result.errors?.[0]?.extensions.code).toBe(
            value === "raw"
              ? "INTERNAL_SERVER_ERROR"
              : value === "public"
                ? "PublicInput"
                : "BAD_USER_INPUT",
          )
          if (value === "raw")
            expect(Cause.squash(reports[0]!.cause)).toBe(secret)
          else expect(reports).toHaveLength(0)
        }
      }
    }),
)
