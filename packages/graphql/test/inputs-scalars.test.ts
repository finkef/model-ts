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
      count: G.optional(Schema.Int, { default: 3 }),
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
  expect(invalid.errors?.[0]?.message).toBe("label cannot be null.")
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

test("arguments and input fields do not inherit a named input type's description", () => {
  class FavoritePosition extends Schema.Class<FavoritePosition>(
    "FavoritePosition",
  )(
    { index: Schema.Number },
    { description: "A position in the user's favorites." },
  ) {}
  class FavoriteMoveInput extends Schema.Class<FavoriteMoveInput>(
    "FavoriteMoveInput",
  )(
    {
      favoriteId: Schema.String,
      position: FavoritePosition,
      after: Schema.optionalKey(Schema.NullOr(FavoritePosition)),
      before: FavoritePosition.annotateKey({
        description: "Move the favorite before this position.",
      }),
    },
    { description: "A new position for one of the user's favorites." },
  ) {}
  const built = G.schema({
    query: G.queryField("ping", "String", () => "pong"),
    move: G.mutationField("favoriteMove", G.nullable("String"), {
      description: "Move one of the caller's favorites to a new position.",
      args: { input: FavoriteMoveInput },
      resolve: () => null,
    }),
    described: G.mutationField("favoriteMoveDescribed", G.nullable("String"), {
      args: {
        input: FavoriteMoveInput.annotateKey({
          description: "The favorite to move and where to.",
        }),
        note: Schema.optionalKey(
          Schema.String.annotate({ description: "Shown in the history." }),
        ),
      },
      resolve: () => null,
    }),
  })
  const printed = printSchema(built.schema)

  expect(printed).toContain(`type Mutation {
  """Move one of the caller's favorites to a new position."""
  favoriteMove(input: FavoriteMoveInput!): String
  favoriteMoveDescribed(
    """The favorite to move and where to."""
    input: FavoriteMoveInput!

    """Shown in the history."""
    note: String
  ): String
}`)
  expect(printed)
    .toContain(`"""A new position for one of the user's favorites."""
input FavoriteMoveInput {
  favoriteId: String!
  position: FavoritePosition!
  after: FavoritePosition

  """Move the favorite before this position."""
  before: FavoritePosition!
}`)
  expect(printed).toContain(`"""A position in the user's favorites."""
input FavoritePosition {`)
})

test("an input type described differently at two uses fails to assemble", () => {
  class Filter extends Schema.Class<Filter>("Filter")(
    { term: Schema.String },
    { description: "Narrows a search." },
  ) {}
  for (const order of ["annotated first", "annotated last"]) {
    const annotated = Filter.annotate({ description: "Narrows this search." })
    const [first, second] =
      order === "annotated first" ? [annotated, Filter] : [Filter, annotated]
    expect(() =>
      G.schema({
        query: G.queryField("search", G.nullable("String"), {
          args: { first, second },
          resolve: () => null,
        }),
      }),
    ).toThrow(
      "input Filter is described differently here than where it was first used",
    )
  }
})

test("GraphQL.annotate describes arguments and input fields without changing their type", async () => {
  class Point extends Schema.Class<Point>("Point")(
    {
      x: G.annotate(Schema.Number, { description: "Distance from the left." }),
      label: G.optional(Schema.String, {
        description: "Shown next to the point.",
        deprecationReason: "Use caption.",
      }),
    },
    { description: "A point on the board." },
  ) {}
  class Placement extends Schema.Class<Placement>("Placement")(
    { point: G.annotate(Point, { description: "Where to place it." }) },
    { description: "Something to place." },
  ) {}
  const built = G.schema({
    query: G.queryField("ping", "String", () => "pong"),
    place: G.mutationField("place", "String", {
      args: {
        placement: G.annotate(Placement, { description: "What to place." }),
        origin: Point,
        note: G.annotate(Schema.String.annotate({ description: "A string." }), {
          description: "Kept in the history.",
        }),
        times: G.optional(Schema.Number, {
          default: 1,
          description: "How often to place it.",
        }),
        legacy: G.optional(Schema.String, { deprecationReason: "Use note." }),
      },
      resolve: (_, { placement, origin, note, times }) =>
        [
          placement instanceof Placement && placement.point instanceof Point,
          placement.point.x - origin.x,
          note,
          times,
        ].join(" "),
    }),
  })
  const printed = printSchema(built.schema)

  expect(printed).toContain(`  place(
    """What to place."""
    placement: Placement!
    origin: Point!

    """Kept in the history."""
    note: String!

    """How often to place it."""
    times: Float = 1
    legacy: String @deprecated(reason: "Use note.")
  ): String!`)
  expect(printed).toContain(`"""Something to place."""
input Placement {
  """Where to place it."""
  point: Point!
}`)
  expect(printed).toContain(`"""A point on the board."""
input Point {
  """Distance from the left."""
  x: Float!

  """Shown next to the point."""
  label: String @deprecated(reason: "Use caption.")
}`)
  expect(
    await Effect.runPromise(
      G.execute(built, {
        source:
          'mutation { place(placement: { point: { x: 3 } }, origin: { x: 1 }, note: "n") }',
      }),
    ),
  ).toEqual({ data: { place: "true 2 n 1" } })
})

test("GraphQL.optional rejects null and GraphQL.optional.nullable keeps it apart from omission", async () => {
  class Rename extends Schema.Class<Rename>("Rename")({
    title: G.optional.nullable(Schema.String, {
      description: "Null clears the title.",
    }),
    subtitle: G.optional(Schema.String),
    tags: G.optional.nullable(Schema.Array(Schema.String), {
      default: ["new"],
    }),
    rank: G.optional(Schema.Int, { default: 0 }),
  }) {}
  const seen: Array<Record<string, unknown>> = []
  const built = G.schema({
    query: G.queryField("ping", "String", () => "pong"),
    rename: G.mutationField("rename", "String", {
      args: {
        input: Rename,
        note: G.optional.nullable(Schema.String),
        count: G.optional(Schema.Int, { default: 1 }),
        color: G.optional.nullable(Schema.String, { default: "blue" }),
      },
      resolve: (_, { input, ...args }) => {
        expect(input).toBeInstanceOf(Rename)
        seen.push({ ...args, input: { ...input } })
        return "ok"
      },
    }),
  })
  const printed = printSchema(built.schema)
  expect(printed).toContain(
    'rename(input: Rename!, note: String, count: Int = 1, color: String = "blue"): String!',
  )
  expect(printed).toContain(`input Rename {
  """Null clears the title."""
  title: String
  subtitle: String
  tags: [String!] = ["new"]
  rank: Int = 0
}`)

  const run = (source: string, variableValues?: Record<string, unknown>) =>
    Effect.runPromise(G.execute(built, { source, variableValues }))
  for (const [source, variableValues] of [
    ["mutation { rename(input: {}) }"],
    [
      "mutation { rename(input: { title: null, tags: null }, note: null, color: null) }",
    ],
    [
      "mutation($note: String, $count: Int) { rename(input: { rank: 2 }, note: $note, count: $count) }",
      {},
    ],
  ] as const)
    expect(await run(source, variableValues)).toEqual({
      data: { rename: "ok" },
    })
  expect(seen).toEqual([
    { count: 1, color: "blue", input: { tags: ["new"], rank: 0 } },
    {
      note: null,
      count: 1,
      color: null,
      input: { title: null, tags: null, rank: 0 },
    },
    { count: 1, color: "blue", input: { tags: ["new"], rank: 2 } },
  ])

  for (const [source, message] of [
    [
      "mutation { rename(input: { subtitle: null }) }",
      "subtitle cannot be null.",
    ],
    ["mutation { rename(input: { rank: null }) }", "rank cannot be null."],
    ["mutation { rename(input: {}, count: null) }", "count cannot be null."],
  ]) {
    const result = await run(source)
    expect(
      result.errors?.map((error) => [error.message, error.extensions.code]),
    ).toEqual([[message, "BAD_USER_INPUT"]])
  }
  expect(seen).toHaveLength(3)
})

test("GraphQL.optional defaults are validated and encoded with their schema", async () => {
  expect(() => G.optional(Schema.Int, { default: 1.5 })).toThrow()
  expect(() =>
    G.optional.nullable(Schema.NonEmptyString, { default: "" }),
  ).toThrow()
  const query = G.queryField("page", "String", {
    args: { size: G.optional(Schema.NumberFromString, { default: 20 }) },
    resolve: (_, { size }) => `${typeof size} ${size}`,
  })
  const built = G.schema({ query })
  expect(printSchema(built.schema)).toContain(
    'page(size: String = "20"): String!',
  )
  expect(
    await Effect.runPromise(G.execute(built, { source: "{ page }" })),
  ).toEqual({ data: { page: "number 20" } })
  expect(
    Schema.decodeUnknownSync(
      Schema.Struct({
        size: G.optional(Schema.NumberFromString, { default: 20 }),
      }),
    )({}),
  ).toEqual({ size: 20 })
})
