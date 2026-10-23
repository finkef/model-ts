import { Context, Effect, Schema } from "effect"
import { createBuilder, type Register } from "../../src/GraphQL.ts"

export const Scope = Schema.Literals(["user:read", "workspace:write"])
export const Status = Schema.Literals(["ACTIVE", "ARCHIVED"])
export const Identity = Schema.Struct({ id: Schema.String })
export const Item = Schema.Struct({
  id: Schema.String,
  kind: Schema.Literal("item"),
  name: Schema.String,
})
export const Alternative = Schema.Struct({
  id: Schema.String,
  kind: Schema.Literal("alternative"),
})
export const Success = Schema.Union([Item, Alternative])
export const ErrorModel = Schema.Struct({ message: Schema.String })
export class Missing extends Schema.TaggedError<Missing>()("Missing", {
  message: Schema.String,
}) {}
export class Labels extends Context.Service<
  Labels,
  { readonly prefix: string }
>()("Labels") {}

interface Types
  extends
    Register<typeof Scope, "Scope">,
    Register<typeof Status, "Status">,
    Register<typeof Identity, "Identity", "interface">,
    Register<typeof Item, "Item">,
    Register<typeof Alternative, "Alternative">,
    Register<typeof Success, "Success">,
    Register<typeof ErrorModel, "Error", "interface">,
    Register<typeof Missing> {}

export const G = createBuilder<Types, { readonly allowed: boolean }>()
export const scope = G.enum("Scope", Scope, {
  description: "Permissions granted to an API key.",
  values: {
    USER_READ: { value: "user:read", description: "Read the user." },
    WORKSPACE_WRITE: {
      value: "workspace:write",
      deprecationReason: "Use a scoped key.",
    },
  },
})
export const status = G.enum("Status", Status)
export const identity = G.interface(
  "Identity",
  Identity,
  {
    description: "An identifiable value.",
    resolveType: (value) =>
      value.id.startsWith("item") ? "Item" : "Alternative",
  },
  {
    id: G.id(),
    label: G.field("String", {
      description: "Inherited label.",
      authorize: (_, __, context) => Effect.succeed(context.allowed),
      resolve: (value) =>
        Labels.useSync((labels) => `${labels.prefix}${value.id}`),
    }),
  },
)
export const item = G.object(
  "Item",
  Item,
  { implements: ["Identity"] },
  {
    name: G.string({ deprecationReason: "Use label instead." }),
    label: G.field("String", (value) => value.name),
    change: G.nullable("Item", {
      errors: [Missing],
      resolve: (value) => value,
    }),
  },
)
export const alternative = G.object(
  "Alternative",
  Alternative,
  { implements: ["Identity"] },
  {
    change: G.nullable("Item", {
      errors: [Missing],
      resolve: () => ({ id: "item-1", kind: "item" as const, name: "one" }),
    }),
  },
)
export const success = G.union("Success", Success, {
  description: "A successful operation.",
  members: ["Item", "Alternative"],
  resolveType: (value) => (value.kind === "item" ? "Item" : "Alternative"),
})
export const error = G.interface("Error", ErrorModel, { message: G.string() })
export const missing = G.object(Missing, { implements: ["Error"] }, {})
const ScopeInput = G.input(
  "ScopeInput",
  Schema.Struct({ scopes: Schema.Array(Scope) }),
)
export const query = G.query({
  status: G.field("Status", () => "ACTIVE"),
  echo: G.field("Scope", {
    args: { scope: Scope },
    resolve: (_, args) => args.scope,
  }),
  input: G.list("Scope", {
    args: { input: ScopeInput },
    resolve: (_, args) => args.input.scopes,
  }),
  identity: G.field("Identity", () => ({ id: "alt-1" })),
  item: G.field("Item", () => ({
    id: "item-1",
    kind: "item" as const,
    name: "one",
  })),
  change: G.nullable("Item", { errors: [Missing], resolve: () => null }),
})
export const mutation = G.mutation({
  change: G.nullable("Success", {
    errors: [Missing],
    result: {
      name: "ChangeResult",
      description: "The result of changing an item.",
    },
    args: { fail: Schema.Boolean, alternative: Schema.Boolean },
    resolve: (_, args) =>
      args.fail
        ? Effect.fail(new Missing({ message: "Not found" }))
        : args.alternative
          ? { id: "alt-1", kind: "alternative" as const }
          : { id: "item-1", kind: "item" as const, name: "one" },
  }),
  markAllRead: G.nullable("Item", {
    result: { name: "MarkAllReadResult" },
    errors: [],
    resolve: () => ({ id: "item-1", kind: "item" as const, name: "one" }),
  }),
  denied: G.nullable("Item", {
    errors: [Missing],
    authorize: () => false,
    resolve: () => ({ id: "item-1", kind: "item" as const, name: "one" }),
  }),
  empty: G.nullable("Item", { errors: [Missing], resolve: () => null }),
  broken: G.nullable("Item", {
    errors: [Missing],
    resolve: () => Effect.die("private"),
  }),
})
export const definitions = {
  scope,
  status,
  identity,
  item,
  alternative,
  success,
  error,
  missing,
  query,
  mutation,
}
export const schema = G.schema(definitions)
export const literalProbe = G.field("Status", () => "ACTIVE")
