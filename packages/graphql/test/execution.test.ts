import { expect, it } from "@effect/vitest"
import {
  Cause,
  Context,
  Deferred,
  Effect,
  Fiber,
  Layer,
  Option,
  Schema,
  Stream,
} from "effect"
import { GraphQLError, parse } from "graphql"
import {
  createBuilder,
  execute,
  subscribe,
  executor,
  ErrorReporter,
  type ErrorReport,
  type Register,
} from "../src/GraphQL.ts"
import * as Testing from "../src/Testing.ts"

class PublicError extends Schema.TaggedError<PublicError>()("PublicError", {
  message: Schema.String,
  member: Schema.String,
}) {}
class PrivateError extends Schema.TaggedError<PrivateError>()("PrivateError", {
  message: Schema.String,
}) {}
class Node extends Schema.TaggedClass<Node>()("Node", { id: Schema.String }) {}
class Gate extends Context.Service<Gate, { readonly allowed: boolean }>()(
  "test/Gate",
) {}
const G = createBuilder<
  Register<typeof Node>,
  { readonly tenant: string }
>().withErrorPolicy({
  public: [PublicError],
  internal: [PrivateError],
  publicCodes: { PublicError: "BAD_REQUEST_ERROR" },
})
const node = G.object(Node, { id: G.id() })
const context = { tenant: "tenant-1" }
const query = G.queryField("ping", "String", (_, __, ctx) => ctx.tenant)
const visible = new PublicError({ message: "Safe", member: "member" })
const secret = new Error("private-backend-detail")
const reports = () => {
  const events: ErrorReport[] = []
  return {
    events,
    layer: Layer.succeed(ErrorReporter, {
      report: (event) =>
        Effect.sync(() => {
          events.push(event)
        }),
    }),
  }
}

for (const operation of ["query", "mutation"] as const) {
  it.effect(
    `${operation}: sync and Effect authorization preserve pagination failures`,
    () =>
      Effect.gen(function* () {
        const fields = {
          sync: {
            authorize: () => true,
            nodes: () => [],
          },
          effect: {
            authorize: () => Effect.succeed(true),
            nodes: () => [],
          },
          paginateSync: {
            authorize: () => true,
            resolve: () => {
              throw new Error("Invalid pagination must stop before resolving")
            },
          },
          paginateEffect: {
            authorize: () => Effect.succeed(true),
            resolve: () => {
              throw new Error("Invalid pagination must stop before resolving")
            },
          },
        }
        const definition = Object.fromEntries(
          Object.entries(fields).map(([name, field]) => [
            name,
            operation === "query"
              ? G.queryField(name, G.connection("Node"), field)
              : G.mutationField(name, G.connection("Node"), field),
          ]),
        )
        const schema = G.schema({ node, query, ...definition })
        for (const name of [
          "sync",
          "effect",
          "paginateSync",
          "paginateEffect",
        ]) {
          const result = yield* execute(
            schema,
            {
              source: `${operation} { ${name}(first:101) { edges { node { id } } } }`,
            },
            context,
          )
          expect(result.errors?.[0]?.extensions.code).toBe("BAD_USER_INPUT")
          expect(result.errors?.[0]?.message).toContain(
            '"first" must be between',
          )
        }
      }),
  )

  it.effect(
    `${operation}: declared errors, authorization, unexpected failures and causes share a policy`,
    () =>
      Effect.gen(function* () {
        const report = reports()
        const fields = {
          denied: {
            authorize: () => false,
            resolve: () => "never",
          },
          effectDenied: {
            authorize: () => Effect.succeed(false),
            resolve: () => "never",
          },
          thrownPublic: () => {
            throw visible
          },
          failedPublic: () => Effect.fail(visible),
          authorization: {
            authorize: () => Effect.fail(visible),
            resolve: () => "never",
          },
          thrown: () => {
            throw secret
          },
          defect: () => Effect.die(secret),
          internal: () => Effect.fail(new PrivateError({ message: "private" })),
          graphql: () => {
            throw new GraphQLError("private-graphql", {
              extensions: { code: "BAD_USER_INPUT" },
            })
          },
        }
        const definition = Object.fromEntries(
          Object.entries(fields).map(([name, field]) => [
            name,
            operation === "query"
              ? G.queryField(name, G.nullable("String"), field)
              : G.mutationField(name, G.nullable("String"), field),
          ]),
        )
        const schema = G.schema({ node, query, ...definition })
        const result = yield* Testing.execute(
          schema,
          { source: `${operation} { ${Object.keys(fields).join(" ")} }` },
          context,
        ).pipe(Effect.provide(report.layer))
        expect(result.errors).toHaveLength(9)
        for (const name of ["denied", "effectDenied"]) {
          expect(
            result.errors?.find((e) => e.path?.[0] === name),
          ).toMatchObject({
            message: `You don't have permission to access field "${name}"`,
            extensions: { code: "FORBIDDEN" },
          })
        }
        for (const name of ["thrownPublic", "failedPublic", "authorization"]) {
          expect(
            result.errors?.find((e) => e.path?.[0] === name),
          ).toMatchObject({
            message: "Safe",
            extensions: { code: "BAD_REQUEST_ERROR", member: "member" },
          })
        }
        expect(JSON.stringify(result)).not.toContain("private")
        expect(report.events).toHaveLength(4)
        expect(
          report.events.filter((event) => Cause.squash(event.cause) === secret),
        ).toHaveLength(2)
        expect(
          report.events.every((event) =>
            event.coordinate.startsWith(
              operation === "query" ? "Query." : "Mutation.",
            ),
          ),
        ).toBe(true)
      }),
  )
}

it.effect(
  "subscriptions mask setup and stream failures and retain original causes",
  () =>
    Effect.gen(function* () {
      const report = reports()
      const subscription = {
        setup: G.subscriptionField("setup", "String", {
          subscribe: () => {
            throw secret
          },
        }),
        stream: G.subscriptionField("stream", "String", {
          subscribe: () => Stream.die(secret),
        }),
        failed: G.subscriptionField("failed", "String", {
          subscribe: () =>
            Stream.fail(new PrivateError({ message: "private" })),
        }),
        publicSetup: G.subscriptionField("publicSetup", "String", {
          subscribe: () => {
            throw visible
          },
        }),
        publicStream: G.subscriptionField("publicStream", "String", {
          subscribe: () => Stream.fail(visible),
        }),
      }
      const schema = G.schema({ node, query, ...subscription })
      for (const name of Object.keys(subscription)) {
        const result = yield* Stream.runCollect(
          Testing.subscribe(
            schema,
            { source: `subscription { ${name} }` },
            context,
          ),
        ).pipe(Effect.provide(report.layer))
        expect(result).toHaveLength(1)
        expect(result[0]?.errors?.[0]).toMatchObject(
          name.startsWith("public")
            ? {
                message: "Safe",
                extensions: { code: "BAD_REQUEST_ERROR", member: "member" },
              }
            : {
                message: "Internal server error",
                extensions: { code: "INTERNAL_SERVER_ERROR" },
              },
        )
      }
      expect(report.events).toHaveLength(3)
      expect(
        report.events.filter((event) => Cause.squash(event.cause) === secret),
      ).toHaveLength(2)
      expect(report.events.every((event) => event.phase === "subscribe")).toBe(
        true,
      )
    }),
)

it.effect(
  "subscriptions authorize every delivery with typed args, context and services",
  () =>
    Effect.gen(function* () {
      let calls = 0
      const subscription = G.subscriptionField("events", "String", {
        args: { prefix: Schema.String },
        authorize: (value, args, ctx) =>
          Effect.map(Gate, (gate) => {
            calls++
            expect(args.prefix).toBe("event")
            expect(ctx).toBe(context)
            return gate.allowed && value === "allowed"
          }),
        subscribe: () => Stream.make("allowed", "denied"),
      })
      const schema = G.schema({ node, query, subscription })
      const result = yield* Stream.runCollect(
        Testing.subscribe(
          schema,
          { source: 'subscription {events(prefix:"event")}' },
          context,
        ),
      ).pipe(Effect.provideService(Gate, { allowed: true }))
      expect(calls).toBe(2)
      expect(result[0]).toEqual({ data: { events: "allowed" } })
      expect(result[1]?.errors?.[0]?.extensions?.code).toBe("FORBIDDEN")
    }),
)

it.effect(
  "executor consumes the parsed document, preserves context/services, validates and subscribes",
  () =>
    Effect.gen(function* () {
      const subscription = G.subscriptionField("events", "String", {
        subscribe: (_, __, ctx) =>
          Stream.fromEffect(Effect.map(Gate, () => ctx.tenant)),
      })
      const adapter = executor(G.schema({ node, query, subscription }))
      expect(adapter.validation).toBe("executor")
      const input = {
        document: parse("query Named {ping}"),
        request: {
          query: "unused (already parsed)",
          operationName: "Named",
          variables: null,
          extensions: null,
        },
        context,
      }
      expect(
        yield* adapter
          .execute(input)
          .pipe(Effect.provideService(Gate, { allowed: true })),
      ).toEqual({ data: { ping: "tenant-1" } })
      const events = yield* Stream.runCollect(
        adapter.subscribe({
          ...input,
          document: parse("subscription {events}"),
          request: { query: "", operationName: null },
        }),
      ).pipe(Effect.provideService(Gate, { allowed: true }))
      expect(events).toEqual([{ data: { events: "tenant-1" } }])
      const invalid = yield* adapter
        .execute({ ...input, document: parse("{unknown}") })
        .pipe(Effect.provideService(Gate, { allowed: true }))
      expect(invalid.errors?.[0]?.message).toContain(
        'Cannot query field "unknown"',
      )
    }),
)

it.effect("telemetry defects cannot replace the original error", () =>
  Effect.gen(function* () {
    const definition = G.queryField("broken", G.nullable("String"), () =>
      Effect.die(secret),
    )
    const result = yield* execute(
      G.schema({ node, query, definition }),
      { source: "{broken}" },
      context,
    ).pipe(
      Effect.provideService(ErrorReporter, {
        report: () => Effect.die("telemetry-secret"),
      }),
    )
    expect(result.errors?.[0]?.message).toBe("Internal server error")
    const original = result.errors?.[0]?.originalError
    if (!(original instanceof GraphQLError))
      throw new Error("Expected located boundary error")
    const cause = original?.originalError?.cause
    if (!Cause.isCause(cause)) throw new Error("Expected original Effect cause")
    expect(Cause.squash(cause)).toBe(secret)
  }),
)

it.effect("subscription interruption closes the source exactly once", () =>
  Effect.gen(function* () {
    let closed = 0
    const subscription = G.subscriptionField("events", "String", {
      subscribe: () =>
        Stream.make("one", "two").pipe(
          Stream.onExit(() =>
            Effect.sync(() => {
              closed++
            }),
          ),
        ),
    })
    const result = yield* Stream.runCollect(
      subscribe(
        G.schema({ node, query, subscription }),
        { source: "subscription {events}" },
        context,
      ).pipe(Stream.take(1)),
    )
    expect(result).toHaveLength(1)
    expect(closed).toBe(1)
  }),
)

it.effect(
  "execution cancellation finalizes resolver Effects without reporting an internal error",
  () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>()
      const stopped = yield* Deferred.make<void>()
      const report = reports()
      const definition = G.queryField("waiting", "String", () =>
        Effect.gen(function* () {
          yield* Deferred.succeed(started, undefined)
          return yield* Effect.never
        }).pipe(Effect.ensuring(Deferred.succeed(stopped, undefined))),
      )
      const fiber = yield* execute(
        G.schema({ node, query, definition }),
        { source: "{waiting}" },
        context,
      ).pipe(Effect.provide(report.layer), Effect.forkChild)
      yield* Deferred.await(started)
      yield* Fiber.interrupt(fiber)
      yield* Deferred.await(stopped)
      expect(report.events).toHaveLength(0)
    }),
)

it.effect("graphql-js serialization errors are masked and reported", () =>
  Effect.gen(function* () {
    const report = reports()
    const definition = G.queryField(
      "invalid",
      G.nullable("Float"),
      () => Number.NaN,
    )
    const result = yield* Testing.execute(
      G.schema({ node, query, definition }),
      { source: "{invalid}" },
      context,
    ).pipe(Effect.provide(report.layer))
    expect(result.errors?.[0]).toMatchObject({
      message: "Internal server error",
      path: ["invalid"],
      extensions: { code: "INTERNAL_SERVER_ERROR" },
    })
    expect(report.events).toHaveLength(1)
    expect(String(Cause.squash(report.events[0]!.cause))).toContain("NaN")
  }),
)

it.effect("a JavaScript Promise authorization cannot bypass the gate", () =>
  Effect.gen(function* () {
    let invoked = false
    const options = {
      authorize: () => true,
      resolve: () => {
        invoked = true
        return "private"
      },
    }
    const definition = G.queryField("blocked", G.nullable("String"), options)
    // Model an untyped JS caller without weakening the public TypeScript API.
    Reflect.set(options, "authorize", () => Promise.resolve(false))
    const result = yield* Testing.execute(
      G.schema({ node, query, definition }),
      { source: "{blocked}" },
      context,
    )
    expect(invoked).toBe(false)
    expect(result.errors?.[0]?.extensions?.code).toBe("INTERNAL_SERVER_ERROR")
  }),
)

it.effect(
  "an explicitly null context is not replaced with the default object",
  () =>
    Effect.gen(function* () {
      const G = createBuilder<{}, null>()
      const query = G.queryField(
        "check",
        "Boolean",
        (_, __, context) => context === null,
      )
      const subscription = G.subscriptionField("check", "Boolean", {
        subscribe: (_, __, context) => Stream.succeed(context === null),
      })
      const built = G.schema({ query, subscription })
      expect(yield* execute(built, { source: "{check}" }, null)).toEqual({
        data: { check: true },
      })
      expect(
        yield* Stream.runCollect(
          subscribe(built, { source: "subscription {check}" }, null),
        ),
      ).toEqual([{ data: { check: true } }])
    }),
)

it.effect(
  "subscription setup and delivery preserve native Date decoding, optional arguments and null diagnostics",
  () =>
    Effect.gen(function* () {
      let setups = 0
      const subscription = G.subscriptionField("dated", "String", {
        args: {
          at: Schema.Date,
          label: G.optional(Schema.String),
          count: G.optional(Schema.Int, { default: 2 }),
          note: G.optional.nullable(Schema.String),
        },
        subscribe: (_, { at, count, ...rest }) => {
          setups++
          expect(at).toBeInstanceOf(Date)
          return Stream.succeed(
            [
              at.toISOString(),
              count,
              "note" in rest ? String(rest.note) : "omitted",
            ].join(" "),
          )
        },
        authorize: (_, { at }) => {
          expect(at).toBeInstanceOf(Date)
          return Effect.succeed(true)
        },
      })
      const schema = G.schema({ node, query, subscription })
      const run = (args: string) =>
        Stream.runCollect(
          Testing.subscribe(
            schema,
            {
              source: `subscription {dated(at:"2026-10-23T00:00:00Z"${args})}`,
            },
            context,
          ),
        )
      expect(yield* run("")).toEqual([
        { data: { dated: "2026-10-23T00:00:00.000Z 2 omitted" } },
      ])
      expect(yield* run(",note:null,count:3")).toEqual([
        { data: { dated: "2026-10-23T00:00:00.000Z 3 null" } },
      ])
      for (const [args, message] of [
        [",label:null", "label cannot be null."],
        [",count:null", "count cannot be null."],
      ]) {
        const invalid = yield* run(args)
        expect(invalid[0]?.errors?.[0]?.extensions?.code).toBe("BAD_USER_INPUT")
        expect(invalid[0]?.errors?.[0]?.message).toBe(message)
      }
      expect(setups).toBe(2)
    }),
)

it.effect(
  "subscription field wrappers normalize delivered objects and lists after authorization",
  () =>
    Effect.gen(function* () {
      const value = new Node({ id: "one" })
      const optional = Option.some(value)
      const calls: unknown[] = []
      const subscription = {
        nullable: G.subscriptionField("nullable", G.nullable("Node"), {
          subscribe: () => Stream.make(optional, Option.none()),
          authorize: (event) => {
            calls.push(event)
            return true
          },
        }),
        list: G.subscriptionField("list", G.list("Node"), {
          subscribe: () => Stream.succeed([value]),
        }),
        nullableList: G.subscriptionField(
          "nullableList",
          G.nullable(G.list("Node")),
          {
            subscribe: () => Stream.make([value], null),
          },
        ),
        nullableElements: G.subscriptionField(
          "nullableElements",
          G.list(G.nullable("Node")),
          {
            subscribe: () => Stream.succeed([optional, Option.none()]),
          },
        ),
        wrappedRef: G.subscriptionField(
          "wrappedRef",
          G.nullable(G.list("Node")),
          {
            subscribe: () => Stream.make([value], undefined),
          },
        ),
      }
      const built = G.schema({ node, query, ...subscription })
      const collect = (field: string) =>
        Stream.runCollect(
          Testing.subscribe(
            built,
            { source: `subscription { ${field} { id } }` },
            context,
          ),
        )
      expect(yield* collect("nullable")).toEqual([
        { data: { nullable: { id: "one" } } },
        { data: { nullable: null } },
      ])
      expect(calls[0]).toBe(optional)
      expect(calls).toHaveLength(2)
      expect(yield* collect("list")).toEqual([
        { data: { list: [{ id: "one" }] } },
      ])
      expect(yield* collect("nullableList")).toEqual([
        { data: { nullableList: [{ id: "one" }] } },
        { data: { nullableList: null } },
      ])
      expect(yield* collect("nullableElements")).toEqual([
        { data: { nullableElements: [{ id: "one" }, null] } },
      ])
      expect(yield* collect("wrappedRef")).toEqual([
        { data: { wrappedRef: [{ id: "one" }] } },
        { data: { wrappedRef: null } },
      ])
      expect(G).not.toHaveProperty("events")
    }),
)
