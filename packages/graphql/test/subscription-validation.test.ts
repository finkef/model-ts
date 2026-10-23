import { expect, it } from "@effect/vitest"
import { Effect, Schema, Stream } from "effect"
import { parse } from "graphql"
import { createBuilder, executor, type Register } from "../src/GraphQL.ts"

class Payload extends Schema.TaggedClass<Payload>()("Payload", {
  text: Schema.String,
  extra: Schema.String,
}) {}

function fixture() {
  let sources = 0
  const GraphQL = createBuilder<Register<typeof Payload>>()
  const events = () =>
    GraphQL.field("Payload", {
      subscribe: () => {
        sources++
        return Stream.succeed(new Payload({ text: "hello", extra: "world" }))
      },
    })
  const schema = GraphQL.schema({
    payload: GraphQL.object(Payload, {
      text: GraphQL.string(),
      extra: GraphQL.string(),
    }),
    queryField: GraphQL.queryField(
      "ping",
      GraphQL.field("String", () => "pong"),
    ),
    event: GraphQL.subscriptionField("event", events()),
    other: GraphQL.subscriptionField("other", events()),
  })
  return { adapter: executor(schema), sources: () => sources }
}

const merged = {
  "repeated spreads":
    "subscription{...A ...A} fragment A on Subscription{event{text extra}}",
  "distinct fragments":
    "subscription{...A ...B} fragment A on Subscription{event{text}} fragment B on Subscription{event{extra}}",
  "repeated fields": "subscription{event{text} event{extra}}",
  "direct and inline fields":
    "subscription{event{text} ...on Subscription{event{extra}}}",
  "nested fragments":
    "subscription{...A} fragment A on Subscription{...B} fragment B on Subscription{event{text extra}}",
  "one alias across fragments":
    "subscription{...A ...B} fragment A on Subscription{same:event{text}} fragment B on Subscription{same:event{extra}}",
}
for (const [shape, query] of Object.entries(merged)) {
  it.effect(`subscription ${shape} merge into one delivered response key`, () =>
    Effect.gen(function* () {
      const { adapter, sources } = fixture()
      const result = yield* Stream.runCollect(
        adapter.subscribe({
          document: parse(query),
          request: { query },
          context: {},
        }),
      )
      expect(result).toEqual([
        {
          data: {
            [shape === "one alias across fragments" ? "same" : "event"]: {
              text: "hello",
              extra: "world",
            },
          },
        },
      ])
      expect(sources()).toBe(1)
    }),
  )
}

const invalid = [
  {
    query: "subscription{event{text} other{text}}",
    message: "Anonymous Subscription must select only one top level field.",
  },
  {
    query:
      "subscription S{...A ...B} fragment A on Subscription{event{text}} fragment B on Subscription{other{text}}",
    message: 'Subscription "S" must select only one top level field.',
  },
  {
    query: "subscription{one:event{text} two:event{text}}",
    message: "Anonymous Subscription must select only one top level field.",
  },
  {
    query: "subscription{...A} fragment A on Subscription{__typename}",
    message:
      "Anonymous Subscription must not select an introspection top level field.",
  },
  {
    query: "subscription S{alias:__typename}",
    message:
      'Subscription "S" must not select an introspection top level field.',
  },
  { query: "subscription{...Missing}", message: 'Unknown fragment "Missing".' },
  {
    query: "subscription{...A} fragment A on Subscription{event{text} ...A}",
    message: 'Cannot spread fragment "A" within itself.',
  },
  {
    query:
      "subscription{...A} fragment A on Subscription{...B} fragment B on Subscription{event{text} ...A}",
    message: 'Cannot spread fragment "A" within itself via "B".',
  },
  {
    query: "subscription{...on Query{ping}}",
    message:
      'Fragment cannot be spread here as objects of type "Subscription" can never be of type "Query".',
  },
]
for (const { query, message } of invalid) {
  it.effect(`subscription validation: ${query}`, () =>
    Effect.gen(function* () {
      const { adapter, sources } = fixture()
      const input = { document: parse(query), request: { query }, context: {} }
      const executed = yield* adapter.execute(input)
      const subscribed = yield* Stream.runCollect(adapter.subscribe(input))
      expect(subscribed).toEqual([executed])
      expect(executed.errors?.map((error) => error.message)).toContain(message)
      expect(
        executed.errors?.every((error) => (error.locations?.length ?? 0) > 0),
      ).toBe(true)
      expect(
        executed.errors?.some(
          (error) => error.extensions?.code === "INTERNAL_SERVER_ERROR",
        ),
      ).toBe(false)
      expect(sources()).toBe(0)
    }),
  )
}

const rootPlacements = {
  field: (directive: string) => `{event ${directive}{text}}`,
  spread: (directive: string) =>
    `{...A ${directive}} fragment A on Subscription{event{text}}`,
  inline: (directive: string) =>
    `{...on Subscription ${directive}{event{text}}}`,
  "fragment field": (directive: string) =>
    `{...A} fragment A on Subscription{event ${directive}{text}}`,
  "fragment inline": (directive: string) =>
    `{...A} fragment A on Subscription{...on Subscription ${directive}{event{text}}}`,
  "fragment spread": (directive: string) =>
    `{...A} fragment A on Subscription{...B ${directive}} fragment B on Subscription{event{text}}`,
}
for (const [placement, selection] of Object.entries(rootPlacements)) {
  it.effect(
    `root directives on a ${placement} are public validation errors without evaluating variables`,
    () =>
      Effect.gen(function* () {
        const { adapter, sources } = fixture()
        for (const name of ["skip", "include"]) {
          for (const condition of ["true", "false", "$flag"]) {
            for (const flag of [true, false]) {
              const query = `subscription S${condition === "$flag" ? "($flag:Boolean!)" : ""}${selection(`@${name}(if:${condition})`)}`
              const input = {
                document: parse(query),
                request: { query, variables: { flag } },
                context: {},
              }
              const result = yield* adapter.execute(input)
              expect(result.errors).toHaveLength(1)
              expect(result.errors?.[0]?.message).toBe(
                'Subscription "S" must not use `@skip` or `@include` directives in the top level selection.',
              )
              expect(result.errors?.[0]?.locations).toEqual([
                { line: 1, column: query.indexOf("@") + 1 },
              ])
              expect(result.errors?.[0]?.extensions).toBeUndefined()
              expect(
                yield* Stream.runCollect(adapter.subscribe(input)),
              ).toEqual([result])
            }
          }
        }
        expect(sources()).toBe(0)
      }),
  )
}

const nested = {
  fields: "text @include(if:$show) extra @skip(if:$hide)",
  spreads: "...Text @include(if:$show) ...Extra @skip(if:$hide)",
  inline:
    "...on Payload @include(if:$show){text} ...on Payload @skip(if:$hide){extra}",
}
for (const [placement, selection] of Object.entries(nested)) {
  it.effect(
    `nested conditional ${placement} still control event delivery`,
    () =>
      Effect.gen(function* () {
        const { adapter, sources } = fixture()
        const query = `subscription($show:Boolean!,$hide:Boolean!){event{${selection}}}${placement === "spreads" ? " fragment Text on Payload{text} fragment Extra on Payload{extra}" : ""}`
        for (const enabled of [true, false]) {
          const result = yield* Stream.runCollect(
            adapter.subscribe({
              document: parse(query),
              request: { query, variables: { show: enabled, hide: !enabled } },
              context: {},
            }),
          )
          expect(result).toEqual([
            {
              data: { event: enabled ? { text: "hello", extra: "world" } : {} },
            },
          ])
        }
        expect(sources()).toBe(2)
      }),
  )
}
