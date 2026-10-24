import { expect, test } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { GraphQLScalarType, Kind, printSchema } from "graphql"
import {
  createBuilder,
  DateTime,
  JSONValue,
  input,
  jsonScalar,
  type Register,
} from "../src/GraphQL.ts"

const G = createBuilder<{}>()
const execute = (
  query: Parameters<typeof G.schema>[0],
  source: string,
  variableValues?: Record<string, unknown>,
) => Effect.runPromise(G.execute(G.schema(query), { source, variableValues }))

test("DateTime accepts Date and string outputs, normalizes ISO inputs, and decodes native Dates", async () => {
  const query = {
    dateField: G.queryField(
      "date",
      "DateTime",
      () => new Date("2026-10-22T08:00:00Z"),
    ),
    isoField: G.queryField(
      "iso",
      "DateTime",
      () => "2026-10-22T10:00:00+02:00",
    ),
    echoField: G.queryField("echo", "DateTime", {
      args: { at: DateTime },
      resolve: (_, { at }) => {
        expect(typeof at).toBe("string")
        return at
      },
    }),
    nativeField: G.queryField("native", "DateTime", {
      args: { at: Schema.Date },
      resolve: (_, { at }) => {
        expect(at).toBeInstanceOf(Date)
        return at
      },
    }),
    codecField: G.queryField("codec", "DateTime", {
      args: { at: input("DateTime", Schema.DateFromString) },
      resolve: (_, { at }) => {
        expect(at).toBeInstanceOf(Date)
        return at
      },
    }),
  }
  expect(
    await execute(
      { ...query },
      "query($at:DateTime!){date iso echo(at:$at) native(at:$at) codec(at:$at)}",
      { at: "2026-10-22T10:00:00+02:00" },
    ),
  ).toEqual({
    data: Object.fromEntries(
      ["date", "iso", "echo", "native", "codec"].map((key) => [
        key,
        "2026-10-22T08:00:00.000Z",
      ]),
    ),
  })
  expect(
    await execute({ ...query }, '{echo(at:"2026-10-22T10:00:00+02:00")}'),
  ).toEqual({ data: { echo: "2026-10-22T08:00:00.000Z" } })
  for (const literal of ['"not-a-date"', "123", "true"])
    expect(
      (await execute({ ...query }, `{echo(at:${literal})}`)).errors,
    ).toHaveLength(1)
})

test("JSON keeps permissive JS variables/outputs but rejects enum literals, including nested ones", async () => {
  const value = { date: new Date("2026-10-22T00:00:00Z"), missing: undefined }
  const query = {
    echoField: G.queryField("echo", "JSON", {
      args: { value: JSONValue },
      resolve: (_, { value }) => value,
    }),
    outputField: G.queryField("output", "JSON", () => value),
  }
  expect(
    await execute(
      { ...query },
      "query($value:JSON!){echo(value:$value) output}",
      {
        value,
      },
    ),
  ).toEqual({ data: { echo: value, output: value } })
  expect(
    await execute({ ...query }, '{echo(value:{a:[1,true,null,"text"]})}'),
  ).toEqual({ data: { echo: { a: [1, true, null, "text"] } } })
  expect(
    (await execute({ ...query }, "{echo(value:{a:[UNQUOTED]})}")).errors,
  ).toHaveLength(1)
})

test("custom scalar codecs retain brands and metadata, and decode list arguments", async () => {
  const URL = Schema.String.check(
    Schema.isPattern(
      /^(https?:\/\/)?((localhost|\w+\.localhost)(:\d+)?|([a-zA-Z0-9-]+\.)*[a-zA-Z0-9-]+\.[a-z]{2,12})(\/\S*)?$/,
    ),
  )
    .pipe(Schema.brand("URL"))
    .annotate({
      description: "A valid URL value",
      specifiedByURL: "https://url.spec.whatwg.org/",
    })
  const TimeZone = Schema.String.check(
    Schema.makeFilter((value) => {
      try {
        new Intl.DateTimeFormat("en", { timeZone: value })
        return true
      } catch {
        return "Use an IANA time zone"
      }
    }),
  )
  interface Types
    extends
      Register<typeof URL, "URL">,
      Register<typeof TimeZone, "TimeZone"> {}
  const local = createBuilder<Types>()
  const url = local.scalar("URL", URL)
  const zone = local.scalar("TimeZone", TimeZone)
  const query = local.queryField("urls", local.list("URL"), {
    args: { urls: Schema.Array(url.schema), zone: zone.schema },
    resolve: (_, args) => {
      expect(args.zone).toBe("Europe/Berlin")
      return args.urls
    },
  })
  const built = local.schema({ url, zone, query })
  const scalar = built.schema.getType("URL")
  expect(scalar?.description).toBe("A valid URL value")
  expect(scalar instanceof GraphQLScalarType && scalar.specifiedByURL).toBe(
    "https://url.spec.whatwg.org/",
  )
  expect(
    await Effect.runPromise(
      local.execute(built, {
        source:
          '{urls(urls:["https://example.com","localhost:3000","example.org"],zone:"Europe/Berlin")}',
      }),
    ),
  ).toEqual({
    data: { urls: ["https://example.com", "localhost:3000", "example.org"] },
  })
  expect(
    (
      await Effect.runPromise(
        local.execute(built, { source: '{urls(urls:["bad"],zone:"UTC")}' }),
      )
    ).errors,
  ).toHaveLength(1)
})

test("existing scalar behavior is preserved without mutating the instance; built-ins can be replaced by registered codecs", async () => {
  const parsed = Schema.String
  const existing = new GraphQLScalarType({
    name: "TimeZone",
    description: "Existing coercion",
    serialize: (value) => `out:${value}`,
    parseValue: (value) => `variable:${value}`,
    parseLiteral: (node) =>
      node.kind === Kind.STRING ? `literal:${node.value}` : "invalid",
  })
  const dates = Schema.String.check(Schema.isPattern(/^date:/))
  const json = Schema.String
  interface Types
    extends
      Register<typeof parsed, "TimeZone">,
      Register<typeof dates, "DateTime">,
      Register<typeof json, "JSON"> {}
  const local = createBuilder<Types>()
  const zone = local.scalar("TimeZone", parsed, existing)
  const date = local.scalar("DateTime", dates)
  const jsonType = local.scalar("JSON", json)
  const query = {
    zoneField: local.queryField("zone", "TimeZone", {
      args: { value: zone.schema },
      resolve: (_, { value }) => value,
    }),
    dateField: local.queryField("date", "DateTime", {
      args: { value: date.schema },
      resolve: (_, { value }) => value,
    }),
    jsonField: local.queryField("json", "JSON", {
      args: { value: jsonType.schema },
      resolve: (_, { value }) => value,
    }),
  }
  const built = local.schema({ zone, date, jsonType, ...query })
  expect(built.schema.getType("TimeZone")).not.toBe(existing)
  expect(existing.parseValue("UTC")).toBe("variable:UTC")
  expect(
    await Effect.runPromise(
      local.execute(built, {
        source:
          'query($zone:TimeZone!){zone(value:$zone) date(value:"date:today") json(value:"plain")}',
        variableValues: { zone: "UTC" },
      }),
    ),
  ).toEqual({
    data: { zone: "out:variable:UTC", date: "date:today", json: "plain" },
  })
  expect(
    await Effect.runPromise(
      local.execute(built, { source: '{zone(value:"UTC")}' }),
    ),
  ).toEqual({ data: { zone: "out:literal:UTC" } })
  expect(() => local.scalar("DateTime", dates, existing)).toThrow(
    "register the scalar under its existing name",
  )
})

class Patch extends Schema.Class<Patch>("Patch")(
  {
    label: Schema.optionalKey(
      Schema.String.annotate({ description: "Omit to leave unchanged." }),
    ),
    clearable: Schema.optionalKey(Schema.NullOr(Schema.String)).annotate({
      description: "Null clears the value.",
    }),
    tags: Schema.optionalKey(Schema.NullOr(Schema.Array(Schema.String))),
  },
  { description: "A partial update." },
) {}

test("input descriptions, omission, explicit clearing, nested lists and defaults", async () => {
  const query = G.queryField("patch", "JSON", {
    args: {
      input: Patch,
      count: G.default(Schema.Int, 3),
      labels: Schema.optionalKey(Schema.Array(Schema.String)),
    },
    resolve: (_, { input, count, labels }) => {
      expect(input).toBeInstanceOf(Patch)
      return { ...input, count, labels }
    },
  })
  const built = G.schema({ query })
  const sdl = printSchema(built.schema)
  expect(sdl).toContain('"""A partial update."""')
  expect(sdl).toContain('"""Omit to leave unchanged."""')
  expect(sdl).toContain('"""Null clears the value."""')
  expect(sdl).toContain("tags: [String!]")
  expect(
    await execute(
      { query },
      '{patch(input:{clearable:null,tags:null},labels:["a","b"])}',
    ),
  ).toEqual({
    data: {
      patch: { clearable: null, tags: null, count: 3, labels: ["a", "b"] },
    },
  })
  const invalid = await execute({ query }, "{patch(input:{label:null})}")
  expect(invalid.errors?.[0]?.message).toContain(
    "input.label: null is not accepted by Schema.optionalKey(S)",
  )
  expect(invalid.errors?.[0]?.message).toContain(
    "Schema.optionalKey(Schema.NullOr(S))",
  )
  expect(invalid.errors?.[0]?.extensions.code).toBe("BAD_USER_INPUT")
  expect(
    (await execute({ query }, "{patch(input:{tags:[null]})}")).errors,
  ).toHaveLength(1)
})

test("null diagnostics do not blame a permissive JSON field when another field fails", async () => {
  const query = G.queryField("value", "String", {
    args: {
      json: Schema.optionalKey(JSONValue),
      label: Schema.NonEmptyString,
    },
    resolve: (_, { label }) => label,
  })
  const result = await execute({ query }, '{value(json:null,label:"")}')
  expect(result.errors?.[0]?.message).toBe("Invalid arguments")
})

test("DateTime decoding reaches native Date fields inside input classes and lists", async () => {
  class Times extends Schema.Class<Times>("Times")({
    dates: Schema.Array(Schema.Date),
    stored: Schema.DateFromString,
  }) {}
  const query = G.queryField("times", G.list("DateTime"), {
    args: { input: Times },
    resolve: (_, { input }) => {
      expect(input).toBeInstanceOf(Times)
      expect(input.dates[0]).toBeInstanceOf(Date)
      expect(input.stored).toBeInstanceOf(Date)
      return [...input.dates, input.stored]
    },
  })
  expect(
    await execute(
      { query },
      '{times(input:{dates:["2026-10-22T10:00:00+02:00"],stored:"2026-10-22T10:00:00+02:00"})}',
    ),
  ).toEqual({
    data: { times: ["2026-10-22T08:00:00.000Z", "2026-10-22T08:00:00.000Z"] },
  })
})

test("codec scalars such as TimeZone require quoted string literals", async () => {
  const zoneValue = Schema.String
  const local = createBuilder<Register<typeof zoneValue, "TimeZone">>()
  const zone = local.scalar("TimeZone", zoneValue)
  const query = local.queryField("zone", "TimeZone", {
    args: { value: zone.schema },
    resolve: (_, { value }) => value,
  })
  const result = await Effect.runPromise(
    local.execute(local.schema({ zone, query }), {
      source: "{zone(value:UTC)}",
    }),
  )
  expect(result.errors?.[0]?.message).toContain("quote the value as a string")
})

const ecma404 =
  "https://ecma-international.org/wp-content/uploads/ECMA-404_2nd_edition_december_2017.pdf"
const AnnotatedJSON = JSONValue.annotate({ specifiedByURL: ecma404 })
interface JSONTypes extends Register<typeof AnnotatedJSON, "JSON"> {}

for (const [path, adopted] of [
  ["jsonScalar", jsonScalar({ specifiedByURL: ecma404 })],
  ["an annotated JSONValue", undefined],
] as const) {
  test(`JSON declares @specifiedBy through ${path} and keeps its parsing`, async () => {
    const local = createBuilder<JSONTypes>()
    const json = local.scalar("JSON", AnnotatedJSON, adopted)
    const built = local.schema({
      json,
      echoField: local.queryField("echo", "JSON", {
        args: { value: JSONValue, annotated: Schema.optionalKey(json.schema) },
        resolve: (_, { value }) => value,
      }),
    })
    const run = (source: string, variableValues?: Record<string, unknown>) =>
      Effect.runPromise(local.execute(built, { source, variableValues }))

    expect(printSchema(built.schema)).toContain(
      `"""\nThe \`JSON\` scalar type represents JSON values as specified by [ECMA-404](http://www.ecma-international.org/publications/files/ECMA-ST/ECMA-404.pdf).\n"""\nscalar JSON @specifiedBy(url: "${ecma404}")`,
    )
    // An adopted jsonScalar is already guarded, so it is not cloned again.
    expect(
      adopted === undefined || built.schema.getType("JSON") === adopted,
    ).toBe(true)

    const value = { date: new Date("2026-10-22T00:00:00Z"), missing: undefined }
    expect(
      await run("query($value:JSON!){echo(value:$value)}", { value }),
    ).toEqual({ data: { echo: value } })
    expect(await run('{echo(value:{a:[1,true,null,"text"]})}')).toEqual({
      data: { echo: { a: [1, true, null, "text"] } },
    })
    for (const literal of ["UNQUOTED", "{a:[UNQUOTED]}"]) {
      const { errors } = await run(`{echo(value:${literal})}`)
      expect(errors?.map((error) => error.extensions.code)).toEqual([
        "BAD_USER_INPUT",
      ])
    }
  })
}

test("jsonScalar without options keeps the default metadata", () => {
  const json = jsonScalar()
  expect(json.description).toContain("ECMA-404")
  expect(json.specifiedByURL).toBeUndefined()
  expect(jsonScalar({ description: "Any JSON value" }).description).toBe(
    "Any JSON value",
  )
  expect(
    printSchema(
      G.schema({ output: G.queryField("output", "JSON", () => 1) }).schema,
    ),
  ).not.toContain("@specifiedBy")
})
