import { expect, it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { createBuilder, type Register } from "../src/GraphQL.ts"

// Boundary projections cover connections declared on objects, on interfaces and
// at the root, over interface and union nodes.
const Identified = Schema.Struct({ id: Schema.String })
const member = <Name extends string>(name: Name) =>
  Schema.Struct({ id: Schema.String, __typename: Schema.Literal(name) })
const Author = member("Author")
const Book = member("Book")
const Publisher = member("Publisher")
const Review = member("Review")
const Series = member("Series")
const Shelf = member("Shelf")
const SearchResult = Schema.Union([
  Author,
  Book,
  Publisher,
  Review,
  Series,
  Shelf,
])
type Types = Register<typeof Identified, "Store"> &
  Register<typeof Identified, "Media", "interface"> &
  Register<typeof Identified, "Edition", "interface"> &
  Register<typeof Identified, "Event", "interface"> &
  Register<typeof Identified, "Ebook"> &
  Register<typeof Identified, "PaperbackEdition"> &
  Register<typeof Identified, "SigningEvent"> &
  Register<typeof Author, "Author"> &
  Register<typeof Book, "Book"> &
  Register<typeof Publisher, "Publisher"> &
  Register<typeof Review, "Review"> &
  Register<typeof Series, "Series"> &
  Register<typeof Shelf, "Shelf"> &
  Register<typeof SearchResult, "SearchResult">

it.effect(
  "abstract connections execute on objects, interfaces and the root, including a six-member union",
  () =>
    Effect.gen(function* () {
      const G = createBuilder<Types>()
      const searchNodes = [
        { id: "author", __typename: "Author" },
        { id: "book", __typename: "Book" },
        { id: "publisher", __typename: "Publisher" },
        { id: "review", __typename: "Review" },
        { id: "series", __typename: "Series" },
        { id: "shelf", __typename: "Shelf" },
      ] as const
      const editions = () => [{ id: "paperback" }]
      const definitions = {
        store: G.object(
          "Store",
          Identified,
          {},
          {
            catalog: G.connection("Media", {
              nodes: () => [{ id: "ebook" }],
            }),
          },
        ),
        media: G.interface(
          "Media",
          Identified,
          { resolveType: () => "Ebook" },
          {
            id: G.id(),
            editions: G.connection("Edition", { nodes: editions }),
          },
        ),
        edition: G.interface(
          "Edition",
          Identified,
          { resolveType: () => "PaperbackEdition" },
          { id: G.id() },
        ),
        event: G.interface(
          "Event",
          Identified,
          { resolveType: () => "SigningEvent" },
          { id: G.id() },
        ),
        ebook: G.object("Ebook", Identified, { implements: ["Media"] }, {}),
        paperback: G.object(
          "PaperbackEdition",
          Identified,
          { implements: ["Edition"] },
          {},
        ),
        signing: G.object(
          "SigningEvent",
          Identified,
          { implements: ["Event"] },
          {},
        ),
        author: G.object("Author", Author, {}, { id: G.id() }),
        book: G.object("Book", Book, {}, { id: G.id() }),
        publisher: G.object("Publisher", Publisher, {}, { id: G.id() }),
        review: G.object("Review", Review, {}, { id: G.id() }),
        series: G.object("Series", Series, {}, { id: G.id() }),
        shelf: G.object(
          "Shelf",
          Shelf,
          {},
          {
            id: G.id(),
            editions: G.connection("Edition", { nodes: editions }),
          },
        ),
        searchResult: G.union("SearchResult", SearchResult, {
          members: ["Author", "Book", "Publisher", "Review", "Series", "Shelf"],
          resolveType: (value) => value.__typename,
        }),
        query: G.query({
          store: G.field("Store", () => ({ id: "store" })),
          shelf: G.field("Shelf", () => searchNodes[5]),
          events: G.connection("Event", {
            nodes: () => [{ id: "signing" }],
          }),
          search: G.connection("SearchResult", {
            args: { query: Schema.String },
            nodes: (_, { query }) => (query ? searchNodes : []),
          }),
        }),
      }
      const built = G.schema(definitions)
      const paperbacks = {
        edges: [{ node: { __typename: "PaperbackEdition", id: "paperback" } }],
      }
      expect(
        yield* G.execute(built, {
          source: `{
      store { catalog { edges { node { __typename editions { edges { node { __typename id } } } } } } }
      shelf { editions { edges { node { __typename id } } } }
      events { edges { node { __typename id } } }
      search(query: "all") { edges { node { __typename } } }
    }`,
        }),
      ).toEqual({
        data: {
          store: {
            catalog: {
              edges: [{ node: { __typename: "Ebook", editions: paperbacks } }],
            },
          },
          shelf: { editions: paperbacks },
          events: {
            edges: [{ node: { __typename: "SigningEvent", id: "signing" } }],
          },
          search: {
            edges: searchNodes.map(({ __typename }) => ({
              node: { __typename },
            })),
          },
        },
      })
    }),
)
