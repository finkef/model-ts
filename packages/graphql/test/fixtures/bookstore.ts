// A small bookstore schema whose printed SDL is checked against bookstore-sdl.json.
import { Schema } from "effect"
import { createBuilder, type Register } from "../../src/GraphQL.ts"

// Enum names differ from their stored values, including multi-segment values.
export const scopeValues = {
  AUTHOR_READ: "author:read",
  BOOK_READ: "book:read",
  BOOK_CREATE: "book:create",
  BOOK_UPDATE: "book:update",
  BOOK_DELETE: "book:delete",
  REVIEW_READ: "review:read",
  REVIEW_WRITE: "review:write",
  SHELF_READ: "shelf:read",
  SHELF_UPDATE: "shelf:update",
  STORE_INVENTORY_READ: "store:inventory:read",
} as const

const Scope = Schema.Literals(Object.values(scopeValues))
const Boundary = Schema.Struct({ id: Schema.String })
const ErrorModel = Schema.Struct({ message: Schema.String })
class NotFoundError extends Schema.TaggedError<NotFoundError>()(
  "NotFoundError",
  { message: Schema.String },
) {}
class UserInputError extends Schema.TaggedError<UserInputError>()(
  "UserInputError",
  { message: Schema.String },
) {}
class OutOfStockError extends Schema.TaggedError<OutOfStockError>()(
  "OutOfStockError",
  { message: Schema.String },
) {}
const Search = Schema.Union([
  Boundary,
  Boundary,
  Boundary,
  Boundary,
  Boundary,
  Boundary,
])
const OrderSuccess = Schema.Union([Boundary, Boundary])
type ObjectNames =
  | "Author"
  | "Backorder"
  | "Book"
  | "OrderConfirmation"
  | "Publisher"
  | "Refund"
  | "Review"
  | "Series"
  | "Shelf"
  | "WishlistClearSuccess"
interface Types
  extends
    Register<typeof Boundary, ObjectNames>,
    Register<typeof Scope, "Scope">,
    Register<typeof ErrorModel, "Error", "interface">,
    Register<typeof NotFoundError>,
    Register<typeof UserInputError>,
    Register<typeof OutOfStockError>,
    Register<typeof Search, "SearchResult">,
    Register<typeof OrderSuccess, "OrderSuccess"> {}
const G = createBuilder<Types>()
const boundary = (name: ObjectNames) =>
  G.object(name, Boundary, {}, { id: G.id() })
export const schema = G.schema({
  scope: G.enum("Scope", Scope, { values: scopeValues }),
  error: G.interface("Error", ErrorModel, { message: G.string() }),
  notFound: G.object(NotFoundError, { implements: ["Error"] }, {}),
  inputError: G.object(UserInputError, { implements: ["Error"] }, {}),
  outOfStock: G.object(OutOfStockError, { implements: ["Error"] }, {}),
  author: G.object(
    "Author",
    Boundary,
    {},
    {
      portrait: G.nullable("String", {
        description: "The URL of the author's portrait.",
        deprecationReason: "Use photo instead.",
        resolve: () => null,
      }),
    },
  ),
  backorder: boundary("Backorder"),
  book: boundary("Book"),
  orderConfirmation: boundary("OrderConfirmation"),
  publisher: boundary("Publisher"),
  refund: boundary("Refund"),
  review: boundary("Review"),
  series: boundary("Series"),
  shelf: boundary("Shelf"),
  wishlistClearSuccess: boundary("WishlistClearSuccess"),
  search: G.union("SearchResult", Search, {
    members: ["Author", "Book", "Publisher", "Review", "Series", "Shelf"],
    description: "An entity returned from catalog search.",
  }),
  orderSuccess: G.union("OrderSuccess", OrderSuccess, {
    members: ["OrderConfirmation", "Backorder"],
  }),
  query: G.query({ health: G.field("Boolean", () => true) }),
  mutation: G.mutation({
    bookCreate: G.nullable("Book", {
      errors: [NotFoundError, UserInputError],
      resolve: () => null,
    }),
    orderPlace: G.nullable("OrderSuccess", {
      errors: [OutOfStockError, UserInputError],
      resolve: () => null,
    }),
    adminRefundIssue: G.nullable("Refund", {
      errors: [UserInputError],
      result: { name: "AdminRefundResult" },
      resolve: () => null,
    }),
    wishlistClear: G.nullable("WishlistClearSuccess", {
      result: {},
      resolve: () => null,
    }),
  }),
})
