# @model-ts/graphql

Build GraphQL APIs with model-ts models or ordinary Effect schemas. Define fields
as plain objects, return values or Effects from resolvers, and let TypeScript check the parent,
arguments, result, request context, and required services together.

The field API keeps related definitions together in a compact form. Model
registration replaces server-side schema type generation: there is no generated
resolver-types file to keep in sync. Models stay free of GraphQL metadata, while the API decides which
fields to expose. Typed failures can become result unions that clients handle
with fragments. Storage, logging, and other services come from Effect layers.

This package builds and executes schemas. It does not provide an HTTP or
WebSocket server.

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Registration and type safety](#registration-and-type-safety)
- [Objects and fields](#objects-and-fields)
- [Arguments and inputs](#arguments-and-inputs)
- [Enums](#enums)
- [Interfaces and unions](#interfaces-and-unions)
- [Mutations and typed results](#mutations-and-typed-results)
- [Connections and pagination](#connections-and-pagination)
- [Scalars](#scalars)
- [Subscriptions](#subscriptions)
- [Request context and layers](#request-context-and-layers)
- [Error policy and reporting](#error-policy-and-reporting)
- [Testing](#testing)
- [Calling Promise APIs from resolvers](#calling-promise-apis-from-resolvers)
- [API summary](#api-summary)
- [Development](#development)

## Install

Use Node.js 22 or later and the coordinated model-ts 7 packages:

```sh
npm install @model-ts/core@^7 @model-ts/graphql@^7 effect@^4.0.5 graphql@^16.11.0
```

The examples below also use DynamoDB models and their in-memory sandbox:

```sh
npm install @model-ts/dynamodb@^7 @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb @aws-sdk/util-dynamodb
```

`effect` and `graphql` are peer dependencies. The supported ranges are
`effect@^4.0.5` and `graphql@^16.11.0`. Use one installed copy of GraphQL
across your application and schema libraries.

The examples use TypeScript 7 and explicit `.ts` imports. Configure your project's
TypeScript build to rewrite those extensions, or adapt imports to your toolchain.
Client operation code generation is optional and independent of this builder.

## Quick start

This example exposes lists and their workspaces, loads them through the DynamoDB
provider, and updates a list with a typed result. It runs entirely in memory.

### 1. Define the models

Keep IDs, persistence fields, and keys in the store. Each ID has a distinct brand;
GraphQL will expose them as `ID` without losing those distinctions in resolvers.

`store/models.ts`

```ts
import { Model } from "@model-ts/core"
import { DynamoDB } from "@model-ts/dynamodb"
import { Schema } from "effect"

export const WorkspaceId = Schema.String.pipe(Schema.brand("WorkspaceId"))
export type WorkspaceId = typeof WorkspaceId.Type
export const ListId = Schema.String.pipe(Schema.brand("ListId"))
export type ListId = typeof ListId.Type

export class Workspace extends Model.Class<Workspace>()(
  "Workspace",
  DynamoDB.provider,
  { id: WorkspaceId, name: Schema.String },
) {
  get PK() {
    return `WORKSPACE#${this.id}`
  }
  get SK() {
    return "WORKSPACE"
  }
  static key(id: WorkspaceId) {
    return { PK: `WORKSPACE#${id}`, SK: "WORKSPACE" }
  }
}

export class List extends Model.Class<List>()("List", DynamoDB.provider, {
  id: ListId,
  workspaceId: WorkspaceId,
  name: Schema.String,
  description: Schema.optionalKey(Schema.NullOr(Schema.String)),
}) {
  get PK() {
    return `LIST#${this.id}`
  }
  get SK() {
    return "LIST"
  }
  get GSI1PK() {
    return `WORKSPACE#${this.workspaceId}`
  }
  get GSI1SK() {
    return this.PK
  }
  static key(id: ListId) {
    return { PK: `LIST#${id}`, SK: "LIST" }
  }
}
```

### 2. Create the application builder

`Types` is the application's registry. Each definition file extends it locally;
there is no central list of model names to maintain.

`api/graphql.ts`

```ts
import {
  ConditionalCheckFailedError,
  DynamoDBError,
  ItemDecodeError,
  ItemEncodeError,
  ItemNotFoundError,
  KeyExistsError,
  PaginationError,
  RaceConditionError,
  TransactionCanceledError,
} from "@model-ts/dynamodb"
import { createBuilder } from "@model-ts/graphql"

export type { Register } from "@model-ts/graphql"
export interface Types {}
export interface AppContext {
  readonly requestId: string
  readonly identity: {
    readonly userId: string
    readonly canWrite: boolean
  } | null
}

export const GraphQL = createBuilder<Types, AppContext>().withErrorPolicy({
  internal: [
    ConditionalCheckFailedError,
    DynamoDBError,
    ItemDecodeError,
    ItemEncodeError,
    ItemNotFoundError,
    KeyExistsError,
    PaginationError,
    RaceConditionError,
    TransactionCanceledError,
  ],
})
```

The policy lets resolvers use storage operations while keeping their failures
private. Translate a storage failure into a domain error when clients should
handle it, as the mutation below does.

The builder carries the definitions, the field helpers such as
`GraphQL.string()` and `GraphQL.field(...)`, and execution. It is your
application's own value, so you may export it under a shorter name such as `G`.

### 3. Expose objects

Register the **model**, then define its GraphQL fields. The object's name comes
from its model tag. No registration is needed for individual fields.

`api/types/workspace.ts`

```ts
import { Workspace } from "../../store/models.ts"
import { GraphQL, type Register } from "../graphql.ts"

declare module "../graphql.ts" {
  interface Types extends Register<typeof Workspace> {}
}

export const WorkspaceType = GraphQL.object(Workspace, {
  id: GraphQL.id(),
  name: GraphQL.string(),
})
```

`api/types/list.ts`

```ts
import { List, Workspace } from "../../store/models.ts"
import { GraphQL, type Register } from "../graphql.ts"

declare module "../graphql.ts" {
  interface Types extends Register<typeof List> {}
}

export const ListType = GraphQL.object(
  List,
  { description: "A list belonging to a workspace." },
  {
    id: GraphQL.id(),
    name: GraphQL.string(),
    description: GraphQL.nullable.string(),
    workspace: GraphQL.field("Workspace", (list) =>
      Workspace.load(Workspace.key(list.workspaceId)),
    ),
  },
)
```

Here `list` is a `List`, the resolver returns an Effect requiring `DynamoDB`, and
that requirement becomes part of the built schema's type.

### 4. Add a query

`api/query/list.ts`

```ts
import { Effect } from "effect"
import { List, ListId } from "../../store/models.ts"
import { GraphQL } from "../graphql.ts"

export const ListQuery = GraphQL.queryField("list", GraphQL.nullable("List"), {
  args: { id: GraphQL.id(ListId) },
  authorize: (_, __, context) => context.identity !== null,
  resolve: (_, { id }) =>
    List.load(List.key(id)).pipe(
      Effect.catchTag("ItemNotFoundError", () => Effect.succeed(null)),
    ),
})
```

Each root field is an exported constant with a name, an output type, and a config
or resolver. The output type is a registered or scalar name, or a config-less
`nullable`, `list`, or `connection` helper. It determines the resolver result and
available config options, just as on object fields. For example:

```ts
export const ListsQuery = GraphQL.queryField(
  "lists",
  GraphQL.list("List"),
  () => [],
)
```

Roots have no parent property to read, so the third argument is required. Use
`resolve` (or resolver shorthand), `nodes` for a connection, or `subscribe` for
a subscription. Pass connection options such as `args`, `name`, `edgeName`,
`authorize`, and `totalCount` in that third argument.

A missing list becomes `null`. A denied request produces a GraphQL field error
with code `FORBIDDEN`. A storage outage follows the internal-error policy.

### 5. Add a mutation with an input class and typed errors

Keep a mutation's input, expected failures, and resolver together. These error
classes become GraphQL result members automatically; they do not need object
registration unless you want custom fields or interfaces on them.

`api/mutation/list-update.ts`

```ts
import { Effect, Schema } from "effect"
import { List, ListId } from "../../store/models.ts"
import { GraphQL } from "../graphql.ts"

export class NotFoundError extends Schema.TaggedError<NotFoundError>()(
  "NotFoundError",
  { message: Schema.String },
) {}
export class UserInputError extends Schema.TaggedError<UserInputError>()(
  "UserInputError",
  { message: Schema.String, field: Schema.String },
) {}
export class ListUpdateInput extends Schema.Class<ListUpdateInput>(
  "ListUpdateInput",
)({
  name: Schema.optionalKey(Schema.NonEmptyString),
  description: GraphQL.nullable.arg(Schema.String),
}) {}

export const ListUpdateMutation = GraphQL.mutationField("listUpdate", "List", {
  args: { id: GraphQL.id(ListId), input: ListUpdateInput },
  errors: [NotFoundError, UserInputError],
  authorize: (_, __, { identity }) => identity?.canWrite === true,
  resolve: (_, { id, input }) =>
    Effect.gen(function* () {
      const list = yield* List.load(List.key(id)).pipe(
        Effect.catchTag("ItemNotFoundError", () =>
          Effect.fail(new NotFoundError({ message: "List not found" })),
        ),
      )
      return yield* list.update(input)
    }),
})
```

The generated field is `listUpdate: ListUpdateResult!`, with
`union ListUpdateResult = List | NotFoundError | UserInputError`. When a field
lists a `UserInputError` class with `message` and `field`, argument decoding can
return that result member for Schema validation failures. Errors rejected earlier
by GraphQL coercion remain ordinary GraphQL errors.

### 6. Assemble the schema

Use `export *` barrels. Assembly checks that every registered definition appears
in the supplied module; input and error class exports can share the barrel.
Root field exports are merged into `Query`, `Mutation`, and `Subscription`. Two
definitions of the same name on the same root fail assembly with a message such
as `Duplicate field: Query.list`. The same name on different roots is valid.

`api/definitions.ts`

```ts
export * from "./types/workspace.ts"
export * from "./types/list.ts"
export * from "./query/list.ts"
export * from "./mutation/list-update.ts"
```

`api/schema.ts`

```ts
import * as definitions from "./definitions.ts"
import { GraphQL } from "./graphql.ts"

export const schema = GraphQL.schema(definitions)
```

### 7. Execute with shared services and per-request context

A `ManagedRuntime` owns the service layers. The context argument belongs to one
request. This executable example seeds an in-memory table, runs a mutation, and
resolves its nested workspace through the same DynamoDB service.

`main.ts`

```ts
import { Sandbox } from "@model-ts/dynamodb/Sandbox"
import { Effect, ManagedRuntime } from "effect"
import { GraphQL, type AppContext } from "./api/graphql.ts"
import { schema } from "./api/schema.ts"
import { List, ListId, Workspace, WorkspaceId } from "./store/models.ts"

const context: AppContext = {
  requestId: "example-1",
  identity: { userId: "user-1", canWrite: true },
}
const runtime = ManagedRuntime.make(Sandbox.layerMemory())
try {
  const result = await runtime.runPromise(
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(
        new Workspace({ id: WorkspaceId.make("w1"), name: "Product" }),
        new List({
          id: ListId.make("l1"),
          workspaceId: WorkspaceId.make("w1"),
          name: "Launch",
          description: "Draft",
        }),
      )
      return yield* GraphQL.execute(
        schema,
        {
          source: `mutation($id: ID!, $input: ListUpdateInput!) {
        listUpdate(id: $id, input: $input) {
          __typename
          ... on List { id name description workspace { name } }
          ... on NotFoundError { message }
          ... on UserInputError { message field }
        }
      }`,
          variableValues: {
            id: "l1",
            input: { name: "Launch plan", description: null },
          },
        },
        context,
      )
    }),
  )
  console.log(result.data)
} finally {
  await runtime.dispose()
}
```

The success result contains `__typename: "List"`, the new name, a null
description, and workspace name `"Product"`. A long-running application creates
its runtime once and disposes it on shutdown. To use a real table, supply
[`DynamoDB.layer`](../dynamodb/README.md#the-dynamodb-service) with the table and
cursor configuration instead of a sandbox layer.

The remaining examples are additions or alternatives to this app. Export new
definitions from `api/definitions.ts`; when replacing an object definition,
export the replacement instead of both versions.

## Registration and type safety

Registration describes a GraphQL name's model and kind. It does not import or
construct the corresponding GraphQL definition.

| Definition              | Registration                                    |
| ----------------------- | ----------------------------------------------- |
| Tagged object           | `Register<typeof List>`; name comes from `_tag` |
| Explicitly named object | `Register<typeof Projection, "SearchHit">`      |
| Enum, scalar, or union  | `Register<typeof Model, "Name">`                |
| Interface               | `Register<typeof Model, "Name", "interface">`   |

Place the declaration merge beside the definition, targeting the module that
exports your application's `Types`. Define explicitly named objects with
`GraphQL.object("SearchHit", Projection, options, fields)`.

The checks happen where you can fix the problem:

| Problem                                                             | Diagnostic location                                  |
| ------------------------------------------------------------------- | ---------------------------------------------------- |
| Unknown or unregistered name                                        | The reference or definition, with a registration fix |
| Wrong property type, resolver result, ID brand, or decoded argument | The field definition                                 |
| Registered type omitted from the barrel                             | `GraphQL.schema(definitions)`                        |
| Incompatible inherited interface field                              | Schema assembly, naming the object and field         |
| Definitions from builders with incompatible contexts                | Field definition or schema assembly                  |
| Missing or wrong request context                                    | The execute/subscribe call                           |
| Missing service layer                                               | Running the Effect or Stream                         |
| Duplicate runtime name or invalid GraphQL type shape                | Schema construction                                  |

Output fields are non-null by default. Use `GraphQL.nullable` when the model or resolver
can return null, undefined, or an `Option`. A real registered name takes precedence
over a possible derived name such as `LibraryConnection`. Generated connection
and edge names are reserved only when used; actual collisions still fail assembly.

## Objects and fields

A field without a resolver reads the property with the same name. A field with a
resolver receives `(parent, decodedArgs, context)` and returns a value or Effect.
Use `GraphQL.object(Model, options, fields)` for descriptions and interfaces.

| Helper                                                                                      | GraphQL output                      |
| ------------------------------------------------------------------------------------------- | ----------------------------------- |
| `GraphQL.id()`, `GraphQL.string()`, `GraphQL.int()`, `GraphQL.float()`, `GraphQL.boolean()` | Required scalar property            |
| `GraphQL.dateTime()`, `GraphQL.json()`                                                      | Required DateTime or JSON property  |
| `GraphQL.nullable.string()` and other nullable scalar helpers                               | Nullable scalar property            |
| `GraphQL.field("Workspace", resolve)`                                                       | Required named type with a resolver |
| `GraphQL.nullable("Workspace", { resolve })`                                                | Nullable named type                 |
| `GraphQL.list("List", { resolve })`                                                         | Required list of required items     |
| `GraphQL.list(GraphQL.nullable("List"), { resolve })`                                       | Required list allowing null items   |
| `GraphQL.nullable(GraphQL.list("List"), { resolve })`                                       | Nullable list of required items     |

For computed scalar fields, use `GraphQL.field` with the scalar name:

`examples/fields.ts`

```ts
import { GraphQL } from "../api/graphql.ts"
import { List } from "../store/models.ts"

export const ListWithSummary = GraphQL.object(List, {
  id: GraphQL.id(),
  name: GraphQL.string({ deprecationReason: "Use displayName." }),
  displayName: GraphQL.field("String", (list) => list.name),
  summary: GraphQL.field("String", {
    description: "A short label for this list.",
    resolve: (list) => `${list.name}: ${list.description ?? "No description"}`,
  }),
})
```

This is an alternative to the quick start's `ListType`; export one definition per
object name. Field options include `description`, `deprecationReason`, and
`authorize`. Authorization can return a boolean or an Effect of a boolean.

## Arguments and inputs

Arguments are Effect schemas. Decoding preserves branded IDs, nested class
instances, refinements, and transformations. Use `GraphQL.id(IdSchema)` to expose
a string-based branded schema as GraphQL `ID` without changing the model schema.
Without a schema, `GraphQL.id()` is the `ID` output field; each form is rejected
by the compiler in the other position.
`GraphQL.input("FilterInput", Schema.Struct(...))` names an input struct;
`Schema.Class` inputs already supply a name. Inputs need no `Types` registration.

GraphQL omission and explicit null have different meanings:

| Input schema                                       | Omitted                          | Explicit `null` |
| -------------------------------------------------- | -------------------------------- | --------------- |
| `Schema.String`                                    | Rejected                         | Rejected        |
| `Schema.optionalKey(Schema.String)`                | Accepted                         | Rejected        |
| `Schema.NullOr(Schema.String)`                     | Schema decoding requires the key | Accepted        |
| `Schema.optionalKey(Schema.NullOr(Schema.String))` | Accepted                         | Accepted        |

A nullable GraphQL argument can be omitted or `null`, which is the last row.
`GraphQL.nullable.arg(Schema.String)` writes it without the wrappers, and takes
the same `{ description, deprecationReason }` as `GraphQL.arg`. Decoding keeps
an omitted key apart from `null`.

GraphQL itself cannot express “omittable but not null” or “required but nullable”;
Schema decoding enforces those distinctions after GraphQL coercion. For updates,
model-ts drops undefined values. Omit a field to leave it unchanged; send `null`
to write an explicit empty value, and allow null in both its input and stored
model field. Null is a stored value, not a DynamoDB attribute removal.

`Schema.Array(S)` requires non-null items. `Schema.Array(Schema.NullOr(S))` allows
null items; `Schema.NullOr(Schema.Array(S))` allows a null list. Schema descriptions
on input classes and fields become GraphQL descriptions.

Describe an argument or input field with `GraphQL.arg(schema, { description,
deprecationReason })`. It documents the argument without changing its type, and
works the same on the fields of an input class:

```ts
class FavoriteMoveInput extends Schema.Class<FavoriteMoveInput>(
  "FavoriteMoveInput",
)(
  {
    favoriteId: Schema.String,
    note: GraphQL.arg(Schema.optionalKey(Schema.String), {
      description: "Kept in the history.",
    }),
  },
  { description: "A new position for one of the user's favorites." },
) {}

export const FavoriteMove = GraphQL.mutationField("favoriteMove", "String", {
  args: {
    input: GraphQL.arg(FavoriteMoveInput, {
      description: "The favorite to move and where to.",
    }),
  },
  resolve: (_, { input }) => input.favoriteId,
})
```

A named input type's description belongs to its `input` definition, so an
argument or input field of that type is only described by `GraphQL.arg` (which
sets Effect key annotations, like `annotateKey`).
`FavoriteMoveInput.annotate({ description })` would describe the type instead,
and an input type described differently at two uses fails to assemble, because
GraphQL gives it one description. Scalar, enum and list arguments keep their
schema's description unless `GraphQL.arg` gives them their own. Only optional
arguments and input fields can be deprecated.

Use `GraphQL.default(schema, decodedValue)` for argument defaults:

`examples/arguments.ts`

```ts
import { Schema } from "effect"
import { GraphQL } from "../api/graphql.ts"

export const GreetingQuery = GraphQL.queryField("greeting", "String", {
  args: { name: GraphQL.default(Schema.String, "world") },
  resolve: (_, { name }) => `Hello, ${name}`,
})
```

Defaults are validated and encoded with their schema. An explicit null sent to an
optional-only input reports its path and suggests omitting the key or using
`GraphQL.nullable.arg(S)`.

## Enums

An enum model is a `Schema.Literals` of strings. Register it with an explicit
name, then choose identity values or map GraphQL names to domain values.

`examples/enums.ts`

```ts
import { Schema } from "effect"
import { GraphQL, type Register } from "../api/graphql.ts"

const Scope = Schema.Literals(["user:read", "workspace:write"])
declare module "../api/graphql.ts" {
  interface Types extends Register<typeof Scope, "Scope"> {}
}
export const ScopeType = GraphQL.enum("Scope", Scope, {
  description: "Permissions granted to an API key.",
  values: {
    USER_READ: { value: "user:read", description: "Read the current user." },
    WORKSPACE_WRITE: "workspace:write",
  },
})
export const ScopeQuery = GraphQL.queryField("echoScope", "Scope", {
  args: { scope: Scope },
  resolve: (_, { scope }) => scope,
})
```

Clients send and receive `USER_READ`; resolvers see `"user:read"`. Every model
literal must have one external name. Duplicate internal values are rejected.
For identity values such as `ACTIVE` and `ARCHIVED`, omit the `values` option.
Mapped values can also carry `deprecationReason`.

Export both definitions through your schema barrel. In a larger application,
keep `Scope` in the store and its GraphQL mapping in the API. Arguments and nested
inputs can use that same model schema directly, including optional, nullable,
annotated, and list wrappers. Mapping is by schema identity: constructing a new
literal schema with the same values does not reuse the registration.

## Interfaces and unions

### Inherit fields from an interface

An interface defines a source shape and a reusable field map. Objects declare
`implements` and inherit its fields, including resolvers and authorization.

`examples/interfaces.ts`

```ts
import { Schema } from "effect"
import { GraphQL, type Register } from "../api/graphql.ts"
import { List, Workspace } from "../store/models.ts"

const Node = Schema.Struct({ id: Schema.String })
declare module "../api/graphql.ts" {
  interface Types extends Register<typeof Node, "Node", "interface"> {}
}
export const NodeType = GraphQL.interface("Node", Node, {
  id: GraphQL.id(),
})
export const ListWithNode = GraphQL.object(
  List,
  { implements: ["Node"] },
  { name: GraphQL.string() },
)
export const WorkspaceWithNode = GraphQL.object(
  Workspace,
  { implements: ["Node"] },
  { name: GraphQL.string() },
)
```

Replace the original object definitions with these variants and export
`NodeType`. Both objects inherit `id`. An explicit object field replaces the
inherited field, including its resolver and authorization; use this to adapt an
interface to a different source representation. Services used by inherited fields
still become schema requirements.

For an interface description or custom type resolver, use
`GraphQL.interface(name, model, { description, resolveType }, fields)`.
Without `resolveType`, abstract values resolve through `__typename`, then `_tag`.
The model-ts classes above already carry their tags.

### Return one of several objects

Tagged model unions infer their member names:

`examples/unions.ts`

```ts
import { Schema } from "effect"
import { GraphQL, type Register } from "../api/graphql.ts"
import { List, Workspace } from "../store/models.ts"

const SearchResult = Schema.Union([List, Workspace])
declare module "../api/graphql.ts" {
  interface Types extends Register<typeof SearchResult, "SearchResult"> {}
}
export const SearchResultType = GraphQL.union("SearchResult", SearchResult)
```

For untagged classes or projections, register each object with an explicit name,
then pass `members` in the same order as the schemas in `Schema.Union`. A custom
`resolveType` selects a registered member name or returns null:

`examples/projections.ts`

```ts
import { Schema } from "effect"
import { GraphQL, type Register } from "../api/graphql.ts"

const Project = Schema.Struct({
  kind: Schema.Literal("project"),
  title: Schema.String,
})
const Person = Schema.Struct({
  kind: Schema.Literal("person"),
  name: Schema.String,
})
const Hit = Schema.Union([Project, Person])
declare module "../api/graphql.ts" {
  interface Types
    extends
      Register<typeof Project, "ProjectHit">,
      Register<typeof Person, "PersonHit">,
      Register<typeof Hit, "Hit"> {}
}
export const ProjectType = GraphQL.object(
  "ProjectHit",
  Project,
  {},
  { title: GraphQL.string() },
)
export const PersonType = GraphQL.object(
  "PersonHit",
  Person,
  {},
  { name: GraphQL.string() },
)
export const HitType = GraphQL.union("Hit", Hit, {
  members: ["ProjectHit", "PersonHit"],
  resolveType: (value) =>
    value.kind === "project" ? "ProjectHit" : "PersonHit",
})
```

Union membership and resolver return types are checked against the registered
models. GraphQL metadata need not be added to the projection values.

## Mutations and typed results

`GraphQL.mutationField(name, type, config)` defines one root field in the same way as
`GraphQL.queryField(name, type, config)`. A field's `errors: [ErrorClass, ...]` turns
expected typed failures into ordinary GraphQL data. Clients inspect `__typename` and select the corresponding fragment.
Only errors declared on that field become its result members.

The quick start generates this contract:

```graphql
union ListUpdateResult = List | NotFoundError | UserInputError

type Mutation {
  listUpdate(id: ID!, input: ListUpdateInput!): ListUpdateResult!
}
```

Use `GraphQL.nullable("List", { errors, resolve })` for a nullable result and to allow a
null success. Its nullability also determines where GraphQL contains an
execution or authorization failure. A union success reference, such as
`"SearchResult"` above, contributes all its object members to the generated
result. Its type resolver is reused.

The `result` option controls the wrapper's name and description:

`examples/results.ts`

```ts
import { Effect } from "effect"
import { GraphQL } from "../api/graphql.ts"
import { NotFoundError } from "../api/mutation/list-update.ts"

export const OptionalUpdate = GraphQL.mutationField(
  "optionalUpdate",
  GraphQL.nullable("List"),
  {
    errors: [NotFoundError],
    result: {
      name: "OptionalUpdateResult",
      description: "The updated list or a domain error.",
    },
    resolve: () =>
      Effect.fail(new NotFoundError({ message: "List not found" })),
  },
)
```

`result: {}` also creates a wrapper when there are no errors, including a
one-member result union. Default mutation result names are `<Field>Result`;
non-mutation names include the parent type, such as `QueryUpdateResult`.

To share an `Error` interface, register the error class as an object and define
it with `implements`. The generated union reuses that object definition:

`examples/error-interface.ts`

```ts
import { Schema } from "effect"
import { GraphQL, type Register } from "../api/graphql.ts"
import { NotFoundError } from "../api/mutation/list-update.ts"

const ErrorModel = Schema.Struct({ message: Schema.String })
declare module "../api/graphql.ts" {
  interface Types
    extends
      Register<typeof ErrorModel, "Error", "interface">,
      Register<typeof NotFoundError> {}
}
export const ErrorType = GraphQL.interface("Error", ErrorModel, {
  message: GraphQL.string(),
})
export const NotFoundType = GraphQL.object(
  NotFoundError,
  { implements: ["Error"] },
  {},
)
```

Registered error-object fields receive the original error class instance as their
parent, including in authorization callbacks. Getters and methods therefore work
in computed fields. Only the declared GraphQL fields are exposed; incidental
properties, stacks, and causes are not copied into the response.

An error class declared internal in the builder policy cannot also be exposed as
a result member. Translate infrastructure failures into domain errors instead.

## Connections and pagination

`GraphQL.connection("List", options)` generates `edges`, `node`, and `pageInfo` fields
with Relay connection shapes. Nodes can be registered objects, interfaces, or
unions. A storage-backed connection returns its page directly:

`examples/connections.ts`

```ts
import { GraphQL } from "../api/graphql.ts"
import { List, WorkspaceId } from "../store/models.ts"

export const ListsQuery = GraphQL.queryField(
  "lists",
  GraphQL.connection("List"),
  {
    args: { workspaceId: GraphQL.id(WorkspaceId) },
    authorize: (_, __, { identity }) => identity !== null,
    resolve: (_, { workspaceId, ...page }) =>
      List.paginate(page, {
        IndexName: "GSI1",
        KeyConditionExpression: "GSI1PK = :pk",
        ExpressionAttributeValues: { ":pk": `WORKSPACE#${workspaceId}` },
      }),
  },
)
```

Clients can request:

```graphql
query {
  lists(workspaceId: "w1", first: 20) {
    edges {
      cursor
      node {
        id
        name
      }
    }
    pageInfo {
      hasNextPage
      hasPreviousPage
      startCursor
      endCursor
    }
  }
}
```

Choose the pagination mode according to the source:

| Option    | Resolver returns                           | Defaults and cursors                                                            |
| --------- | ------------------------------------------ | ------------------------------------------------------------------------------- |
| `resolve` | A `Connection<Node>` page or Effect of one | Storage owns defaults and opaque cursors. `List.paginate` results pass through. |
| `nodes`   | All matching nodes or Effect of that array | Builder slices the array and creates base64 offset cursors.                     |

For `nodes`, `createBuilder({ defaultPageSize: 20, maxPageSize: 100 })` controls
slicing. Those are the defaults; the default size is capped by the maximum.
A before-only request slices backward. With `resolve`, the builder does not
insert a size or interpret the cursor: a DynamoDB layer's `pagination.default`
and encrypted cursor format continue to apply. Array offsets and DynamoDB
cursors are not interchangeable.

The builder validates explicit `first` and `last` in both modes. Zero is an empty
page; null means omitted. Negative or excessive sizes, conflicting directions,
and simultaneous `before`/`after` produce `BAD_USER_INPUT`. This behavior is the
same with synchronous and Effect authorization. Storage-level failures, including
DynamoDB `PaginationError`, follow the application's configured error policy.
The four pagination argument names are reserved; additional `args` use Schema
decoding as usual.

`totalCount(parent, args, context)` adds `totalCount: Int!` and can return a number
or Effect. It runs **only when that field is selected**, including selections
through fragments and directives. It receives the decoded custom arguments and
request context, and its service requirements are collected with the page's.

Uncounted connections default to `<Node>Connection`; counted ones default to
`<Node>CountedConnection`. `name` overrides the connection name and `edgeName`
overrides the default `<Node>Edge` independently. A real `LibraryConnection`
object can coexist with a generated `LibraryDirectoryConnection` by naming the
latter explicitly. Assembly rejects incompatible reuse of generated names.
Derived references such as `GraphQL.field("ListEdge", resolve)` retain the node type.

## Scalars

The builder supplies `ID`, `String`, `Boolean`, `Int`, `Float`, `DateTime`, and
`JSON`. Use the exported input schemas when the wire scalar is significant:

`examples/scalars.ts`

```ts
import { DateTime, JSONValue } from "@model-ts/graphql"
import { GraphQL } from "../api/graphql.ts"

export const EchoTimeQuery = GraphQL.queryField("echoTime", "DateTime", {
  args: { at: DateTime },
  resolve: (_, { at }) => at,
})
export const EchoJSONQuery = GraphQL.queryField("echoJSON", "JSON", {
  args: { value: JSONValue },
  resolve: (_, { value }) => value,
})
```

`DateTime` inputs deliver normalized ISO strings. `Schema.Date` inputs deliver
`Date` objects, including inside input classes and lists. For an explicit codec,
use `GraphQL.input("DateTime", Schema.DateFromString)`. DateTime output accepts a
valid Date or parseable date string and serializes to ISO.

`JSONValue` has type `unknown`: variables and outputs follow the permissive
`graphql-type-json` convention. Literal JSON rejects unquoted enum values. For
stricter validation, use `Schema.Json.annotate({ graphqlType: "JSON" })`.

To give the JSON scalar a `@specifiedBy` URL or its own description, register
it with an annotated `JSONValue`. The builder keeps the built-in JSON parsing
and takes `specifiedByURL` and `description` from the annotations:

```ts
import { JSONValue } from "@model-ts/graphql"
import { GraphQL, type Register } from "../api/graphql.ts"

const specifiedByURL =
  "https://ecma-international.org/wp-content/uploads/ECMA-404_2nd_edition_december_2017.pdf"
const AnnotatedJSON = JSONValue.annotate({ specifiedByURL })
declare module "../api/graphql.ts" {
  interface Types extends Register<typeof AnnotatedJSON, "JSON"> {}
}
export const JSONType = GraphQL.scalar("JSON", AnnotatedJSON)
```

The schema then prints `scalar JSON @specifiedBy(url: "...")`. To set the
metadata on the scalar itself, pass `jsonScalar({ specifiedByURL, description })`
as the third argument; it has the same parsing as the built-in scalar.

Custom scalars pair a registered name with a Schema codec:

`examples/custom-scalar.ts`

```ts
import { Schema } from "effect"
import { GraphQL, type Register } from "../api/graphql.ts"

const Slug = Schema.String.check(Schema.isPattern(/^[a-z0-9-]+$/)).pipe(
  Schema.brand("Slug"),
)
declare module "../api/graphql.ts" {
  interface Types extends Register<typeof Slug, "Slug"> {}
}
export const SlugType = GraphQL.scalar(
  "Slug",
  Slug.annotate({
    description: "Lowercase letters, numbers, and hyphens.",
  }),
)
export const SlugQuery = GraphQL.queryField("echoSlug", "Slug", {
  args: { slug: SlugType.schema },
  resolve: (_, { slug }) => slug,
})
```

The codec validates inputs and encodes outputs; use the definition's `.schema`
for custom scalar arguments. `description` and `specifiedByURL` annotations
become scalar metadata. To keep an existing implementation, pass a matching
`GraphQLScalarType` as the third argument to `GraphQL.scalar`. Its name must match,
and the Schema codec must accept what its parser returns. The adopted scalar is
defensively cloned: its coercion behavior and metadata are preserved, but its
object identity is not.

Expected codec input failures use `BAD_USER_INPUT` with a generic message such as
`Invalid value for Slug`. Exceptions thrown by custom or adopted scalar callbacks
follow the public/internal error policy for both inline literals and variables.
Declared public failures retain their safe fields and code; unexpected causes are
masked and sent to `ErrorReporter`. Ordinary graphql-js document and built-in
input diagnostics remain intact.

Registering and exporting a scalar named `DateTime` or `JSON` replaces that
built-in implementation. The five standard graphql-js scalars remain reserved.

## Subscriptions

Define a subscription with `GraphQL.subscriptionField(name, type, config)` and
a `subscribe` callback that returns an Effect `Stream`. Its `authorize` callback
runs for each delivered value, with that value as its parent, and returns a boolean or an Effect. Both
callbacks receive decoded arguments and request context. Their service requirements,
along with nested resolver requirements, are retained in the built schema type.

`examples/subscriptions.ts`

```ts
import { Stream } from "effect"
import { GraphQL } from "../api/graphql.ts"
import { List, ListId } from "../store/models.ts"

export const ListSubscription = GraphQL.subscriptionField(
  "listChanged",
  "List",
  {
    args: { id: GraphQL.id(ListId) },
    // A finite stream for this example; replace it with your event source.
    subscribe: (_, { id }) => Stream.fromEffect(List.load(List.key(id))),
    authorize: (_, __, { identity }) => identity !== null,
  },
)
```

The third argument to `GraphQL.subscriptionField` must have `subscribe` and
cannot have `resolve`. `subscribe` is rejected on object, interface, query, and
mutation fields, including reused configs. Nullable and list subscriptions use
config-less refs in the second argument, such as `GraphQL.nullable("List")`,
`GraphQL.list("List")`, or `GraphQL.nullable(GraphQL.list("List"))`. The stream
emits the declared nullable or list value;
`authorize` receives it before output normalization.

Export the subscription in the schema barrel with `export *`. Consume
`GraphQL.subscribe(schema, request, context)` as a
Stream, for example with `Stream.runCollect` in the application's runtime. Put
subscription setup checks in the source callback; delivery authorization receives
the emitted object as its parent. Setup and terminal stream failures use the
same declared error policy as queries and mutations. Cancellation interrupts the
source and in-flight event work, including authorization and nested resolvers,
and waits for their finalizers before completing.

Repeated spreads such as `subscription { ...Changed ...Changed }` are valid
when `Changed` selects one root response key. Validation merges repeated fields,
named fragments, and inline fragments by that key.
Different aliases count as different keys, even when they select the same field.
The root field must not be an introspection field.

Root `@skip` and `@include` directives are rejected without evaluating their
arguments, including on root fragment spreads and inline fragments. Conditional
selections inside the root field's payload remain valid and use the operation's
variables. Invalid root selections return public validation errors before the
subscription source runs.

## Request context and layers

Use **context** for request data: identity, tenant, locale, and request ID. Use
**layers** for services and their lifetimes: DynamoDB, HTTP clients, caches, and
telemetry. Resolvers receive context as their third argument and access services
through Effect. Never capture a request's identity in a shared runtime layer.

`BuiltSchema<R, C>` tracks both. The context type is carried through fields,
definitions, schema assembly, execution, and subscriptions. Services used by
resolvers, authorization, inherited fields, and total counts contribute to `R`.
Supply those services when running the operation; you do not have to annotate the
schema's requirement union yourself.

`GraphQL.execute` returns an Effect of an `ExecutionResult`. `GraphQL.subscribe`
returns a Stream of those results. Both take
`{ source, variableValues?, operationName? }` and require the application's context
when its type requires one. They apply the error policy and retain the graphql-js
result shape, including GraphQLError objects. Public failures are represented in
the result rather than the returned Effect's error channel.

### Formatted execution results

Use the package-level `executor(schema)` for transport-ready results. Its
`execute` and `subscribe` methods accept a parsed document and return an Effect
or Stream of `FormattedExecutionResult`: plain, JSON-safe data and error records.
The executor declares `validation: "executor"`: it runs standard schema validation
before execution or subscription setup, so literal scalar failures receive the same
public/internal policy as variable coercion failures. Transports should skip their
own standard schema validation for this capability, retaining parsing, operation
selection, request limits and transport-specific rules.

The executor owns result formatting. Transports must preserve its public
messages, extension codes, locations, and paths when sending the result.

`examples/executor.ts`

```ts
import { executor } from "@model-ts/graphql"
import { parse } from "graphql"
import { schema } from "../api/schema.ts"

const query = "query($id: ID!) { list(id: $id) { id name } }"
export const lookup = executor(schema).execute({
  document: parse(query),
  request: { query, variables: { id: "l1" } },
  context: {
    requestId: "lookup-1",
    identity: { userId: "user-1", canWrite: false },
  },
})
```

Run `lookup` through the application's `ManagedRuntime`. Parsed requests may also
supply `operationName` and `extensions` in `request`; the supplied document is
what executes. The same interface's `subscribe` method produces formatted results
for subscription deliveries.

If formatting fails, for example because a JSON scalar returns a cycle or a
BigInt, the executor reports the original cause through `ErrorReporter` and
returns a JSON-safe `INTERNAL_SERVER_ERROR` response. Result formatting failures
always use that internal fallback. Direct `execute`/`subscribe` retain their
`ExecutionResult` shape and do not perform this JSON formatting step.

## Error policy and reporting

There are three ways an expected failure can reach a client:

| Configuration                    | Response                                                   |
| -------------------------------- | ---------------------------------------------------------- |
| Field `errors: [NotFoundError]`  | A typed result-union member in `data`                      |
| Builder `public: [AccessDenied]` | A GraphQL field error with declared safe fields and a code |
| `authorize` returns false        | A GraphQL field error with code `FORBIDDEN`                |

An authorization callback must return true to permit execution. False produces
`You don't have permission to access field "<field>"`. Returning an Effect of a
boolean follows the same policy. Authorization failures are field errors, not
members of a mutation's result union.

Choose the public error code exposed to clients:

`examples/error-policy.ts`

```ts
import { ItemNotFoundError } from "@model-ts/dynamodb"
import { createBuilder, ErrorReporter } from "@model-ts/graphql"
import { Cause, Effect, Layer, Schema } from "effect"
import type { AppContext, Types } from "../api/graphql.ts"

class AccessDenied extends Schema.TaggedError<AccessDenied>()("AccessDenied", {
  message: Schema.String,
}) {}
export const GraphQL = createBuilder<Types, AppContext>().withErrorPolicy({
  public: [AccessDenied, ItemNotFoundError],
  publicCodes: {
    AccessDenied: "ACCESS_DENIED",
    ItemNotFoundError: "NOT_FOUND",
  },
  publicMessages: { ItemNotFoundError: "Not found" },
})
export const Reporting = Layer.succeed(ErrorReporter, {
  report: ({ cause, coordinate, phase }) =>
    Effect.logError(
      "GraphQL operation failed",
      { coordinate, phase },
      Cause.pretty(cause),
    ),
})
```

Apply the policy before constructing fields and definitions with the builder. A
public error's default code is its tag; `publicCodes` overrides it. Only declared
Schema fields are exposed, not incidental properties or stacks. Keep internal
errors in `internal`; unclassified resolver failures and defects are masked as
`Internal server error` with code `INTERNAL_SERVER_ERROR`. A code attached to an
arbitrary thrown exception does not make it public. This policy also applies to
custom scalar callbacks and abstract-type resolution. Ordinary graphql-js
document diagnostics remain available to clients.

Some errors are safe to report but carry fields that are not, such as the
storage key of an `ItemNotFoundError`. Give their tag a fixed message in
`publicMessages`: clients then get only that message and the error's code, here
`{ message: "Not found", extensions: { code: "NOT_FOUND" } }`, and never the
error's own message or fields. The keys must be tags of the `public` errors. A
field that lists the error in its `errors` union still returns it as a union
member with its declared fields.

Provide `Reporting` alongside the application's service layers, for example with
`Layer.mergeAll(Sandbox.layerMemory(), Reporting)`. The optional `ErrorReporter`
receives the original Effect cause, field coordinate, and phase for masked
failures. A reporter failure cannot replace the client error. Resolver and
authorization work is traced under the GraphQL execution spans.

## Testing

Use `@model-ts/graphql/Testing` to run real operations through the same schema
and layers as the application. Results are plain records with GraphQL errors
normalized, so they work directly with `toMatchSnapshot` and `toMatchInlineSnapshot`.

```sh
npm install --save-dev @effect/vitest vitest
```

Bind the schema to a default request context once. The factory's options become
the options of every request, so each test states only what differs:

`test/client.ts`

```ts
import * as Testing from "@model-ts/graphql/Testing"
import type { AppContext } from "../api/graphql.ts"
import { schema } from "../api/schema.ts"

export { graphql } from "@model-ts/graphql/Testing"
export const { request, subscribe } = Testing.client(schema, {
  context: (options: { identity?: AppContext["identity"] }) => ({
    requestId: "test",
    identity: options.identity ?? null,
  }),
})
```

`test/list-update.test.ts`

```ts
import { expect, layer } from "@effect/vitest"
import { Sandbox } from "@model-ts/dynamodb/Sandbox"
import { Effect } from "effect"
import { List, ListId, WorkspaceId } from "../store/models.ts"
import { graphql, request } from "./client.ts"

const MUTATION = graphql`
  mutation ListUpdate($id: ID!, $input: ListUpdateInput!) {
    listUpdate(id: $id, input: $input) {
      __typename
      ... on List {
        id
        name
      }
      ... on NotFoundError {
        message
      }
    }
  }
`
const identity = { userId: "user-1", canWrite: true }

layer(Sandbox.layerMemory())((it) => {
  it.effect("renames a list", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(
        new List({
          id: ListId.make("l1"),
          workspaceId: WorkspaceId.make("w1"),
          name: "Launch",
        }),
      )
      const variables = { id: "l1", input: { name: "Ship" } }
      expect(yield* request(MUTATION, { identity, variables }))
        .toMatchInlineSnapshot(`
        {
          "data": {
            "listUpdate": {
              "__typename": "List",
              "id": "l1",
              "name": "Ship",
            },
          },
        }
      `)
    }),
  )
})
```

`request(source, options?)` returns an Effect and `subscribe` returns a Stream;
both also accept `variables` and `operationName`. When the factory's options have a
required key, every call must pass it; a schema without request context needs no
factory. The factory may return an Effect to read services such as `Sandbox`, and
those services join the schema's requirements, so a missing layer is still a
compiler error.

The `graphql` tag interpolates values and removes ignored characters with
graphql-js `stripIgnoredCharacters`, leaving string literals intact. Snapshots and error
locations therefore stay the same when a test is reformatted, and editors still
highlight the tagged document.

A file-level layer shares its table within that test group. For a fresh table per
test, use `it.effect` and provide `Sandbox.layerMemory()` on the individual Effect.
Replace the memory layer with `Sandbox.layerLocal()` to run the same tests against
DynamoDB Local at `http://localhost:8000`. Allow time for real table creation and
cleanup in your test and hook timeouts. `Sandbox.seed`, `snapshot`, and `diff` let
tests assert storage changes as well as GraphQL responses.

`Testing.execute(schema, { source, variableValues?, operationName? }, context)` and
`Testing.subscribe` remain the low-level API for an explicit context;
`Testing.snapshot(result)` normalizes an already executed, serializable result. Use
the executor when testing the reported serialization-failure response itself.

## Calling Promise APIs from resolvers

Resolvers return values or Effects. Wrap asynchronous dependencies with
`Effect.tryPromise`, give failures a typed error, and classify it in the
application policy:

`examples/promises.ts`

```ts
import { createBuilder } from "@model-ts/graphql"
import { Effect, Schema } from "effect"

class UpstreamError extends Schema.TaggedError<UpstreamError>()(
  "UpstreamError",
  {
    cause: Schema.Unknown,
  },
) {}
const GraphQL = createBuilder<{}>().withErrorPolicy({
  internal: [UpstreamError],
})
export const RemoteQuery = GraphQL.queryField("status", "String", {
  resolve: () =>
    Effect.tryPromise({
      try: async (signal) => {
        const response = await fetch("https://example.com/status", { signal })
        if (!response.ok) throw new Error(`HTTP ${response.status}`)
        return response.text()
      },
      catch: (cause) => new UpstreamError({ cause }),
    }),
})
```

For an existing remote client, put the client in an Effect service layer and
access it from the resolver. Direct Promise returns are rejected by the typed
field API.

## API summary

The package root exports named functions and types, plus a `GraphQL` namespace.
The quick start calls its application-specific builder `GraphQL`; package-level
functions such as `executor` are separate exports, not builder methods.

| API                                                                  | Purpose                                                           |
| -------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `createBuilder<Types, Context>(options?)`                            | Create definitions and field helpers for one registry and context |
| `Register<typeof Model, Name?, Kind?>`                               | Extend the registry with a model                                  |
| Builder `.object`, `.interface`, `.union`, `.enum`, `.scalar`        | Define named output types                                         |
| Builder `.queryField`, `.mutationField`, `.subscriptionField`        | Define one named operation field                                  |
| Builder `.id()`, `.string()`, `.nullable`, `.list`, `.field`         | Define output fields                                              |
| Builder `.connection`                                                | Define connection fields                                          |
| Builder `.field`, `.nullable`, `.list` with `{ subscribe }`          | Define subscription fields returning Effect Streams               |
| Builder `.id(Schema)`, `.input`, `.default`, `.arg`, `.nullable.arg` | Annotate argument/input schemas                                   |
| Builder `.withErrorPolicy`                                           | Classify global public and internal typed failures                |
| Builder `.schema(definitions)`                                       | Check completeness and build a `BuiltSchema<R, C>`                |
| `execute`, `subscribe` (also on the builder)                         | Run source-text requests as Effects or Streams                    |
| `executor(built)`                                                    | Execute parsed documents with JSON-safe formatted results         |
| `ExecutionResult`, `FormattedExecutionResult`                        | Direct graphql-js results and formatted executor results          |
| `ErrorReporter`                                                      | Optional service for masked-failure telemetry                     |
| `ID`, `DateTime`, `JSONValue`                                        | Built-in input schemas                                            |
| `jsonScalar({ specifiedByURL?, description? })`                      | The built-in JSON scalar, with its own metadata                   |
| `id`, `input`, `defaulted`, `arg`                                    | Package-level schema annotation helpers                           |
| `Connection`, `Edge`, `PageInfo`, `ConnectionArgs`                   | Structural pagination types                                       |
| `@model-ts/graphql/Testing`                                          | Test clients, the graphql tag, and normalized snapshots           |

## Development

From the repository root, `bun run build`, `bun run typecheck`, `bun run lint`,
`bun run format:check`, and `bun run test` include this package. Its tests use
the in-memory DynamoDB Sandbox.

From `packages/graphql`:

| Command                 | Checks                                                            |
| ----------------------- | ----------------------------------------------------------------- |
| `bun run test`          | Runtime and in-memory storage tests                               |
| `bun run test:types`    | Inferred parent, argument, context, and service types             |
| `bun run test:compiler` | Negative typing and registry diagnostics with native TypeScript 7 |
| `bun run test:scale`    | A generated 200-model schema                                      |
