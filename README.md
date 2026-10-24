# model-ts

Typed models for TypeScript, built on [Effect](https://effect.website) and its
`Schema` module. A model is a class: it decodes and encodes itself, carries
your own getters and methods, and gains persistence or messaging methods from
_providers_ such as DynamoDB and EventBridge.

```ts
class Post extends Model.Class<Post>()("Post", DynamoDB.provider, {
  id: Schema.String,
  title: Schema.String,
  publishedAt: Schema.NullOr(Schema.DateFromString),
}) {
  get PK() {
    return `POST#${this.id}`
  }
  get SK() {
    return "POST"
  }
}

const program = Effect.gen(function* () {
  const post = yield* new Post({
    id: "1",
    title: "Hello",
    publishedAt: null,
  }).put()
  const renamed = yield* post.update({ title: "Hello, world" })
})
```

> [!IMPORTANT]
> This is model-ts **7**, a rewrite on Effect v4. Versions up to and including
> **@model-ts/core 0.4.x, @model-ts/dynamodb 6.x and @model-ts/eventbridge 3.x**
> are built on [io-ts](https://github.com/gcanti/io-ts) and fp-ts. They are
> still on npm and their source is on the
> [`@model-ts/dynamodb@6.0.0` tag of this repository](https://github.com/finkef/model-ts/tree/%40model-ts/dynamodb%406.0.0).
> To upgrade, read [Migrating from the io-ts versions](#migrating-from-the-io-ts-versions).

## Contents

- [Packages](#packages)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Models](#models)
- [Fields and defaults](#fields-and-defaults)
- [Unions](#unions)
- [Providers](#providers)
- [Services and layers](#services-and-layers)
- [Errors](#errors)
- [Tracing](#tracing)
- [Testing](#testing)
- [Running in AWS Lambda](#running-in-aws-lambda)
- [Migrating from the io-ts versions](#migrating-from-the-io-ts-versions)
- [Development](#development)

## Packages

| Package                                                         | What it does                                                                    |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| [`@model-ts/core`](packages/core/README.md)                     | `Model.Class`, `Model.Union` and the provider API.                              |
| [`@model-ts/dynamodb`](packages/dynamodb/README.md)             | Single-table DynamoDB: reads, writes, transactions, pagination, sandboxes.      |
| [`@model-ts/eventbridge`](packages/eventbridge/README.md)       | Publishing models as EventBridge events, now or later.                          |
| [`@model-ts/graphql`](packages/graphql/README.md)               | Effect-native GraphQL definitions, model registration and execution.            |
| [`@model-ts/graphql-lambda`](packages/graphql-lambda/README.md) | Effect-native GraphQL HTTP, WebSockets and subscription delivery on AWS Lambda. |

All five are versioned together, so `@model-ts/core@7.1.0` goes with
`@model-ts/dynamodb@7.1.0`.

## Installation

`effect` and the AWS SDK clients are peer dependencies. Your application
owns their versions, and model-ts never bundles a copy of its own.

```sh
# Models only
npm install @model-ts/core effect

# DynamoDB
npm install @model-ts/core @model-ts/dynamodb effect \
  @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb @aws-sdk/util-dynamodb

# EventBridge
npm install @model-ts/core @model-ts/eventbridge effect @aws-sdk/client-eventbridge
```

The packages are ESM only and need Node.js 22 or later. They require
`effect@^4.0.5`.

## Quick start

Define a model, give it `DynamoDB.provider` and table keys, then run an
Effect program with a `DynamoDB` layer.

```ts
import { Model } from "@model-ts/core"
import { DynamoDB } from "@model-ts/dynamodb"
import { Effect, Schema } from "effect"

const PostId = Schema.String.pipe(Schema.brand("PostId"))
type PostId = typeof PostId.Type

class Post extends Model.Class<Post>()("Post", DynamoDB.provider, {
  id: PostId,
  title: Schema.String,
  publishedAt: Schema.NullOr(Schema.DateFromString),
}) {
  get PK() {
    return `POST#${this.id}`
  }
  get SK() {
    return "POST"
  }

  static key(id: PostId) {
    return { PK: `POST#${id}`, SK: "POST" }
  }
}

const program = Effect.gen(function* () {
  const post = yield* new Post({
    id: PostId.make("post_001"),
    title: "Hello",
    publishedAt: null,
  }).put()
  const loaded = yield* Post.load(Post.key(post.id))
  return yield* loaded.update({ publishedAt: new Date() })
})

await Effect.runPromise(
  program.pipe(
    Effect.provide(DynamoDB.layer({ tableName: "app", cursors: "plain" })),
  ),
)
```

The type of `program` tells you everything it needs and everything that can go
wrong:

```ts
Effect.Effect<
  Post,
  ItemNotFoundError | ItemDecodeError | KeyExistsError | RaceConditionError | ...,
  DynamoDB
>
```

## Models

`Model.Class<Self>()(tag, fields)` returns a class to extend. Pass the class itself
as `Self` so that decoders and provider methods return your subclass, getters
and methods included. With providers, they go between the tag and the fields:
`Model.Class<Self>()(tag, provider, fields)` or `Model.Class<Self>()(tag, [a, b], fields)`.

```ts
class User extends Model.Class<User>()("User", {
  id: Schema.String,
  email: Email,
  firstName: Schema.String,
  lastName: Schema.String,
}) {
  get fullName() {
    return `${this.firstName} ${this.lastName}`
  }
}
```

A model class is a real `Schema.Class`, so it works with every Schema API and
nests in other schemas and models. On top of that it has:

| Member               | Does                                                                                  |
| -------------------- | ------------------------------------------------------------------------------------- |
| `new User({...})`    | Builds an instance from typed values. Fills in defaults and skips filters and brands. |
| `User.make({...})`   | Like `new`, but runs every check and throws on invalid input.                         |
| `User.from(input)`   | Decodes untrusted input and throws a `SchemaError` when it does not match.            |
| `User.decode(input)` | Decodes untrusted input as an `Effect<User, SchemaError>`.                            |
| `user.encode()`      | The encoded form, for JSON APIs, events and storage.                                  |
| `user.values()`      | The field values as a plain object, without `_tag` or getters.                        |

`from` and `decode` are shorthands: `Schema.decodeUnknownSync(User)` and every
other Schema decoder return `User` instances too, getters and methods included.

**Construction trusts its input.** The constructor's parameter is already
typed, brands included, so `new User({...})` passes Effect's
`disableChecks`: it fills in defaults and checks the shape, but skips filters
and brands. Pass `{ disableChecks: false }` as the second argument, or use
`User.make`, to run every check. Validation happens where data enters your
program: `from`, `decode` or any Schema decoder. A value that slipped past a
filter is caught when it is encoded, for example as an `ItemEncodeError`
before a DynamoDB write.

**`_tag` is handled for you.** Every model has a `_tag` equal to its name. You
never pass it to the constructor, `from` or `decode`; it is optional when
decoding, at any depth, and always written when encoding.

```ts
const user = User.from({
  id: "user_01J...",
  email: "ada@example.com",
  firstName: "Ada",
  lastName: "Lovelace",
})
user.fullName // "Ada Lovelace"
user.encode() // { _tag: "User", id: "user_01J...", email: "ada@example.com", ... }
```

## Fields and defaults

Fields are ordinary Schemas, so optional and nullable fields, brands and
defaults use Effect's own combinators:

| Field                                                                        | Type                               |
| ---------------------------------------------------------------------------- | ---------------------------------- |
| `Schema.optional(Schema.String)`                                             | `bio?: string`                     |
| `Schema.NullOr(Schema.String)`                                               | `bio: string \| null`              |
| `Schema.optional(Schema.NullOr(Schema.String))`                              | `bio?: string \| null`             |
| `Schema.String.pipe(Schema.withConstructorDefault(Effect.succeed("light")))` | `theme: string`, optional in `new` |

`Schema.withConstructorDefault` fills a field the constructor leaves out;
`Schema.withDecodingDefaultKey` fills a key that stored data lacks, for
example rows written before the field existed.

## Unions

`Model.Union(members)` is a `Schema.Union` of models with `from` and `decode`. It
picks the member by `_tag` when the input has one and tries each member in
order when it does not.

```ts
const Shape = Model.Union([Circle, Square])
type Shape = typeof Shape.Type // Circle | Square

Shape.from({ _tag: "Circle", radius: 1 }) // Circle
```

A provider can add members to unions too. With `DynamoDB.provider`, a union
reads mixed items, for example a post and its comments in one partition:

```ts
const PostContent = Model.Union([Post, Comment], DynamoDB.provider)
const program = Effect.gen(function* () {
  const items = yield* PostContent.query({
    KeyConditionExpression: "PK = :pk",
    ExpressionAttributeValues: { ":pk": post.PK },
  }) // Array<Post | Comment>
})
```

## Providers

A provider adds members to models: instance methods, getters, statics and
union members. `DynamoDB.provider` adds `put`, `load`, `query` and the rest;
`EventBridge.provider` adds `publish`. Pass several as an array; they merge left to right, and a later member replaces an
earlier one with the same name.

```ts
class InvoiceSent extends Model.Class<InvoiceSent>()(
  "InvoiceSent",
  [DynamoDB.provider, EventBridge.provider],
  { invoiceId: InvoiceId },
) {
  get PK() {
    return `INVOICE#${this.invoiceId}`
  }
  get SK() {
    return "SENT"
  }
  get source() {
    return "billing"
  }
  get detailType() {
    return "billing.invoice.sent"
  }
}

const program = Effect.gen(function* () {
  const sent = yield* new InvoiceSent({ invoiceId }).put()
  yield* sent.publish()
})
```

Providers are constant values. They never hold a client or configuration;
their methods return Effects that ask for a service, such as `DynamoDB` or
`EventBridge`, and the layer you provide decides which one they get (see
[Services and layers](#services-and-layers)).

Writing your own takes two steps: declare the members as a type lambda, then
implement them with `Provider.make`. The members can refer to the model through
`this["Self"]` and `this["Fields"]`:

```ts
import { Provider } from "@model-ts/core"

interface AuditInstance<Self> {
  /** Writes an audit entry for this item. */
  audit(this: Self & { readonly id: string }, action: string): Effect.Effect<void, never, AuditLog>
}

interface AuditMembers extends Provider.Members {
  readonly instance: AuditInstance<this["Self"]>
}

export const audited = Provider.make<AuditMembers>()({
  instanceProps: {
    audit(action) {
      return AuditLog.use((log) => log.write(this.id, action))
    },
  },
})

class Invoice extends Model.Class<Invoice>()("Invoice", [DynamoDB.provider, audited], { ... }) {}
```

A `this` parameter such as `this: Self & { readonly id: string }` states what
the model must have. Calling `audit` on a model without an `id` is a type
error. `DynamoDB.provider` uses this to require `PK` and `SK`, and
`EventBridge.provider` to require `source` and `detailType`.

The [core README](packages/core/README.md#providers) covers statics, union
members and `Provider.merge`.

## Services and layers

Every model method that talks to AWS returns an `Effect` that requires a
service:

| Service       | Required by                                              | Layers                                                        |
| ------------- | -------------------------------------------------------- | ------------------------------------------------------------- |
| `DynamoDB`    | `DynamoDB.provider` members and `DynamoDB.*`             | `DynamoDB.layer`, `Sandbox.layerMemory`, `Sandbox.layerLocal` |
| `EventBridge` | `EventBridge.provider` members and `EventBridge.publish` | `EventBridge.layer`, `EventBridge.layerMemory`                |

Programs say what they need in their type and get it once, at the edge, from a
layer. Production, tests and local development differ only in the layers they
provide, and nothing reads a global client or an environment variable behind
your back.

```ts
// Options from Config, read when the layer is built
const AppLayer = Layer.unwrap(
  Effect.gen(function* () {
    const tableName = yield* Config.String("TABLE_NAME")
    const encryptionKey = yield* Config.Redacted("CURSOR_KEY")
    const eventBusName = yield* Config.String("EVENT_BUS_NAME")
    return Layer.mergeAll(
      DynamoDB.layer({ tableName, cursors: { encryptionKey } }),
      EventBridge.layer({ eventBusName }),
    )
  }),
)
```

Each layer creates its AWS client when it is built and destroys it when its
scope closes.

## Errors

Every failure is a tagged error in the Effect error channel, a
`Schema.TaggedError` with a readable `message`. The compiler knows which ones
an operation can produce, and `Effect.catchTag` handles them one by one:

```ts
const program = Effect.gen(function* () {
  const post = yield* Post.load(key).pipe(
    Effect.catchTag("ItemNotFoundError", () => Effect.succeed(null)),
  )
})
```

When a missing item is expected, `Post.loadOption(key)` says so directly: it
returns `Option.none()` and has no `ItemNotFoundError` to handle.

| Error                         | Raised when                                                                         |
| ----------------------------- | ----------------------------------------------------------------------------------- |
| `ItemNotFoundError`           | No item is stored under the key.                                                    |
| `KeyExistsError`              | `put` found an item at the key (pass `ignoreExistence: true` to overwrite).         |
| `ConditionalCheckFailedError` | A condition you passed was false.                                                   |
| `RaceConditionError`          | The stored item changed since it was read (optimistic locking).                     |
| `TransactionCanceledError`    | DynamoDB cancelled a transaction; `cancellationReasons` says why.                   |
| `BulkWriteRollbackError`      | A bulk write failed and could not be fully rolled back.                             |
| `PaginationError`             | Invalid pagination arguments, or a cursor that is unreadable or from another query. |
| `ItemDecodeError`             | A stored item does not match the model's schema.                                    |
| `ItemEncodeError`             | An instance holds a value its schema cannot encode.                                 |
| `DynamoDBError`               | Any other AWS failure: throttling, permissions, network. `code` has the AWS name.   |
| `PublishError`                | EventBridge rejected some entries; `entries` has one result per event.              |
| `EventEncodeError`            | An event could not be encoded, so nothing was sent.                                 |
| `EventBridgeError`            | The PutEvents call itself failed.                                                   |

Each operation's error union is exported as a type, such as `DynamoDB.GetError`
or `DynamoDB.UpdateError`, for use in your own signatures. Defects (bugs, such
as a transaction group of more than 100 operations) are not in the error
channel.

## Tracing

Every operation runs in a span, and every AWS call in a client span inside it,
so a trace shows both what your code asked for and what it sent:

```text
DynamoDB.update              model_ts.model=Post  model_ts.key=POST#post_01J... / POST
└─ DynamoDB.put
   └─ DynamoDB.PutItem     db.system.name=aws.dynamodb  aws.dynamodb.table_names=[app]
EventBridge.publish        messaging.batch.message_count=23
├─ EventBridge.PutEvents   messaging.destination.name=orders  messaging.batch.message_count=10
├─ EventBridge.PutEvents   ...
└─ EventBridge.PutEvents   ...
```

AWS call spans follow the OpenTelemetry semantic conventions for AWS SDK and
messaging spans. Failures are recorded on the span that failed. Spans reach
your backend through any Effect tracer; with the OTLP exporter that ships with
Effect:

```ts
import { FetchHttpClient } from "effect/http"
import { OtlpSerialization, OtlpTracer } from "effect/observability"

const TracingLayer = OtlpTracer.layer({
  url: "http://localhost:4318/v1/traces",
  resource: { serviceName: "blog-api" },
}).pipe(
  Layer.provide(OtlpSerialization.layerJson),
  Layer.provide(FetchHttpClient.layer),
)

program.pipe(Effect.provide(Layer.merge(AppLayer, TracingLayer)))
```

The package READMEs list the span names and attributes.

## Testing

Tests swap the production layers for in-process ones. Nothing has to be
mocked, and no environment variable switches implementations.

| Layer                     | Gives you                                                                                              |
| ------------------------- | ------------------------------------------------------------------------------------------------------ |
| `Sandbox.layerMemory()`   | A fresh in-memory DynamoDB table. Fast; nothing to start.                                              |
| `Sandbox.layerLocal()`    | A fresh table on [DynamoDB Local](https://hub.docker.com/r/amazon/dynamodb-local), deleted afterwards. |
| `EventBridge.layerMemory` | A bus that records events; read them through `PublishedEvents`.                                        |

Both sandbox layers also provide `DynamoDB`, run the real AWS SDK client and
send the same requests; only the endpoint differs. The in-memory engine is
checked against DynamoDB Local by this repository's conformance suite.

`Sandbox` seeds the table (`sandbox.seed(...items)`) and takes snapshots, so a test can assert exactly
what an operation wrote. The diff lists each added (`+`), removed (`-`) or
changed item with all of its attributes, and marks the ones that changed:

```ts
import { describe, expect, layer } from "@effect/vitest"
import { Sandbox } from "@model-ts/dynamodb/Sandbox"

describe("Post", () => {
  layer(Sandbox.layerMemory())((it) => {
    it.effect("stores a new post", () =>
      Effect.gen(function* () {
        const sandbox = yield* Sandbox
        const before = yield* sandbox.snapshot

        yield* new Post({
          id: PostId.make("post_001"),
          title: "Hello",
          publishedAt: null,
        }).put()

        expect(yield* sandbox.diff(before)).toMatchInlineSnapshot(`
          "+ [POST#post_001 / POST]
          +   PK: "POST#post_001"
          +   SK: "POST"
          +   _docVersion: 0
          +   _tag: "Post"
          +   id: "post_001"
          +   publishedAt: null
          +   title: "Hello""
        `)
      }),
    )
  })
})
```

`layer(...)` from `@effect/vitest` builds the layer once for the whole block,
so its tests share one table. For a fresh table per test, wrap `it.effect` so
each test provides the layer itself:

```ts
import { it as vitest } from "@effect/vitest"
import type { DynamoDB } from "@model-ts/dynamodb"
import { Sandbox } from "@model-ts/dynamodb/Sandbox"
import { Effect, type Scope } from "effect"

export const it = {
  effect: <A, E>(
    name: string,
    body: () => Effect.Effect<A, E, Sandbox | DynamoDB | Scope.Scope>,
  ) =>
    vitest.effect(name, () =>
      Effect.provide(Effect.suspend(body), Sandbox.layerMemory()),
    ),
}
```

Test bodies then never call `Effect.provide` themselves. To run the same suite
against DynamoDB Local, choose the layer from a Vitest project setting, as this
repository's `vitest.config.ts` does.

## Running in AWS Lambda

Build the layers once per container with `ManagedRuntime` and run each
invocation on it. Clients and connections are reused across warm invocations,
and loads from separate `runPromise` calls, such as GraphQL resolvers, share
BatchGetItem calls. Providing the layer per call instead builds a new client
and a new batch for every call.

```ts
import { DynamoDB } from "@model-ts/dynamodb"
import { EventBridge } from "@model-ts/eventbridge"
import { Config, Effect, Layer, ManagedRuntime } from "effect"

const AppLayer = Layer.unwrap(
  Effect.gen(function* () {
    const tableName = yield* Config.String("TABLE_NAME")
    const encryptionKey = yield* Config.Redacted("CURSOR_KEY")
    const eventBusName = yield* Config.String("EVENT_BUS_NAME")
    return Layer.mergeAll(
      DynamoDB.layer({ tableName, cursors: { encryptionKey } }),
      EventBridge.layer({ eventBusName }),
    )
  }),
)

const runtime = ManagedRuntime.make(AppLayer)

export const handler = (event: OrderPlacedEvent) =>
  runtime.runPromise(
    handleOrderPlaced(event).pipe(Effect.withSpan("handler.orderPlaced")),
  )
```

A handler's tests run the same `handleOrderPlaced` with the test layers.

## Migrating from the io-ts versions

The ideas carry over (models with methods, providers, single-table DynamoDB,
sandboxes), but the API is new. Plan for one pass over each model and each
call site.

| io-ts versions                                        | Version 7                                                                                                                                                 |
| ----------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `t.type({...})` codecs                                | `Schema` fields: `Schema.String`, `Schema.Struct`, `Schema.DateFromString` …                                                                              |
| `class X extends model("X", t.type({...}), provider)` | `class X extends Model.Class<X>()("X", provider, {...})`                                                                                                  |
| `X.from(input)` / `X.decodeOrThrow(input)`            | `X.from(input)`                                                                                                                                           |
| `X.decode(input)` returns an `Either`                 | `X.decode(input)` returns an `Effect`                                                                                                                     |
| `getProvider(client)`                                 | the constant `DynamoDB.provider` plus a `DynamoDB` layer                                                                                                  |
| `new Client({ tableName })`                           | `DynamoDB.layer({ tableName, cursors })`; `cursors` is required (`{ encryptionKey }` or `"plain"`)                                                        |
| `Promise` results, thrown errors                      | `Effect` results, typed errors (see [Errors](#errors))                                                                                                    |
| `X.load(key, { null: true })`                         | `X.loadOption(key)`, an `Option`; `Option.getOrNull` gives the old `null`                                                                                 |
| `X.loadMany(keys)` with a result or error per key     | `X.loadMany(keys)` fails on the first error; for per-key results, `Effect.forEach(keys, (key) => Effect.exit(X.load(key)), { concurrency: "unbounded" })` |
| `X.iterator(...)` / async iteration                   | `X.stream(...)`, an Effect `Stream`                                                                                                                       |
| `encodeDDBCursor(item, key)` / `decodeDDBCursor(...)` | `item.cursor()` or `DynamoDB.cursor(item)`; only `paginate` reads cursors                                                                                 |
| `client.bulk([...])`                                  | `DynamoDB.bulk([...])`                                                                                                                                    |
| `item._docVersion`                                    | `docVersion(item)`                                                                                                                                        |
| `createSandbox(client)`, `startTracking`, `rollback`  | `Sandbox.layerMemory()` / `Sandbox.layerLocal()`, `snapshot`, `diff`                                                                                      |
| `EXPERIMENTAL_DYNAMODB_IN_MEMORY`                     | `Sandbox.layerMemory()`                                                                                                                                   |
| `getProvider(eventBridgeClient)`, `stubEventBus`      | `EventBridge.provider` plus `EventBridge.layer` / `EventBridge.layerMemory`                                                                               |
| CommonJS                                              | ESM only, Node.js 22+                                                                                                                                     |
| `GSI1` inverts the primary key (`SK`/`PK`)            | `GSI1` is keyed by `GSI1PK`/`GSI1SK`, like `GSI2` to `GSI19`                                                                                              |
| queries and updates skip items with `dynamotorLegacy` | no such filter or condition                                                                                                                               |

Stored items are compatible: the item format including `_tag` and
`_docVersion`, soft-deleted keys, unencrypted cursors and the `PK`/`SK` and
`GSI2` to `GSI19` indexes are unchanged, so both versions can share a table
during a migration. Two changes affect existing tables:

- `GSI1` is a regular index keyed by `GSI1PK`/`GSI1SK`. The io-ts versions
  defined it as the inverted primary index (`SK`/`PK`). DynamoDB can't change
  an index's keys, so a table that has the old `GSI1` needs it deleted and
  created again with the new keys. For the old lookup, a model can return its
  `SK` from `GSI1PK` and its `PK` from `GSI1SK`. Cursors from GSI1 queries
  made by the io-ts versions don't carry these keys and can't be resumed.
- Queries and updates no longer skip items that carry a `dynamotorLegacy`
  attribute. Queries send no default `FilterExpression`, and updates use
  `attribute_exists(PK)` as their condition. Delete or migrate such items
  before switching.

A few behaviours changed:

- Encrypted cursors use AES-256-GCM, so clients can no longer read or forge
  them. The io-ts versions used AES-256-CTR with a fixed IV. Encrypted
  cursors issued before the upgrade fail with `PaginationError`; unencrypted
  cursors keep working with `cursors: "plain"`.
- `update` fails with `RaceConditionError` when the item was deleted since
  it was read, where it used to write the item again. Its condition now
  includes `attribute_exists(PK)`.
- An `update` that changes the key checks the version of the old item
  before deleting it, and fails with `RaceConditionError` (old item changed)
  or `KeyExistsError` (new key taken).
- Queries fail with `ItemDecodeError` when an item tagged with a requested
  model doesn't decode, where they used to leave it out.
- Keys that `BatchGetItem` leaves unprocessed are read again with
  exponential backoff, up to five times.
- `paginate` keeps querying when a filter or items of other models leave a
  page short, where it used to return a short page that claimed to be the last.
- `bulk` rolls back committed transactions last to first, also after a
  defect, and waits for that before it can be interrupted.
- A retried transaction sends the same `ClientRequestToken` on every attempt,
  so DynamoDB applies it once.
- `loadMany` fails as a whole, as described above.

## Development

The repository is a Bun workspace.

| Command                  | Does                                                     |
| ------------------------ | -------------------------------------------------------- |
| `bun install`            | Installs dependencies.                                   |
| `bun run build`          | Compiles every package to `dist/`.                       |
| `bun run typecheck`      | Type-checks sources and tests, including the type tests. |
| `bun run test`           | Runs the test suite with the in-memory DynamoDB engine.  |
| `bun run dynamodb:start` | Starts DynamoDB Local in Docker on port 8000.            |
| `bun run test:local`     | Runs the DynamoDB tests against DynamoDB Local.          |
| `bun run lint`           | Checks sources and tests with oxlint.                    |
| `bun run format`         | Formats with oxfmt.                                      |
| `bun run format:check`   | Checks formatting with oxfmt.                            |

Releases go through [Changesets](https://github.com/changesets/changesets):
add a changeset with `bunx changeset`, and the release workflow versions and
publishes the packages together.

## License

MIT
