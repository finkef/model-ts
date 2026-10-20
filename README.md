# model-ts

Typed models for TypeScript, built on [Effect](https://effect.website) and its
`Schema` module. A model is a class: it decodes and encodes itself, carries
your own getters and methods, and gains persistence or messaging methods from
_providers_ such as DynamoDB and EventBridge.

```ts
class Post extends model<Post>()("Post", dynamodb, {
  id: generated(PostId),
  title: Schema.String,
  publishedAt: Field.nullable(Schema.DateFromString),
}) {
  get PK() {
    return `POST#${this.id}`
  }
  get SK() {
    return "POST"
  }
}

const post = yield * new Post({ title: "Hello", publishedAt: null }).put()
const renamed = yield * post.update({ title: "Hello, world" })
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
- [Fields, optional values and defaults](#fields-optional-values-and-defaults)
- [IDs](#ids)
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

| Package                                                   | What it does                                                               |
| --------------------------------------------------------- | -------------------------------------------------------------------------- |
| [`@model-ts/core`](packages/core/README.md)               | `model()`, fields, IDs, unions and the provider API.                       |
| [`@model-ts/dynamodb`](packages/dynamodb/README.md)       | Single-table DynamoDB: reads, writes, transactions, pagination, sandboxes. |
| [`@model-ts/eventbridge`](packages/eventbridge/README.md) | Publishing models as EventBridge events, now or later.                     |

All three are versioned together, so `@model-ts/core@7.1.0` goes with
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

Define a model, give it the `dynamodb` provider and table keys, then run an
Effect program with a `DynamoDB` layer.

```ts
import { Field, generated, Id, model } from "@model-ts/core"
import { DynamoDB, dynamodb } from "@model-ts/dynamodb"
import { Effect, Schema } from "effect"

const PostId = Id("post_", "PostId")
type PostId = typeof PostId.Type

class Post extends model<Post>()("Post", dynamodb, {
  id: generated(PostId),
  title: Schema.String,
  publishedAt: Field.nullable(Schema.DateFromString),
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
  const post = yield* new Post({ title: "Hello", publishedAt: null }).put()
  const loaded = yield* Post.load(Post.key(post.id))
  return yield* loaded.update({ publishedAt: new Date() })
})

await Effect.runPromise(
  program.pipe(Effect.provide(DynamoDB.layer({ tableName: "app" }))),
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

`model<Self>()(tag, fields)` returns a class to extend. Pass the class itself
as `Self` so that decoders and provider methods return your subclass, getters
and methods included. With providers, they go between the tag and the fields:
`model<Self>()(tag, provider, fields)` or `model<Self>()(tag, [a, b], fields)`.

```ts
class User extends model<User>()("User", {
  id: generated(UserId),
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

| Member               | Does                                                                           |
| -------------------- | ------------------------------------------------------------------------------ |
| `new User({...})`    | Builds an instance from typed values. Fills in defaults and does not validate. |
| `User.make({...})`   | Like `new`, but validates and throws on invalid input.                         |
| `User.from(input)`   | Decodes untrusted input and throws a `SchemaError` when it does not match.     |
| `User.decode(input)` | Decodes untrusted input as an `Effect<User, SchemaError>`.                     |
| `user.encode()`      | The encoded form, for JSON APIs, events and storage.                           |
| `user.values()`      | The field values as a plain object, without `_tag` or getters.                 |

**Construction trusts its input.** The constructor's parameter is already
typed, brands included, so `new User({...})` only fills in defaults. Validation
happens where data enters your program: `from`, `decode` or any Schema decoder.
A value that slipped past the types (an `as` cast, say) is caught when it is
encoded, for example as an `ItemEncodeError` before a DynamoDB write.

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

## Fields, optional values and defaults

Fields are ordinary Schemas. `Field` adds short names for the shapes models use
most:

| Field                                                  | Type                   | Encoded                                       |
| ------------------------------------------------------ | ---------------------- | --------------------------------------------- |
| `Field.optional(Schema.String)`                        | `bio?: string`         | key left out when missing                     |
| `Field.nullable(Schema.String)`                        | `bio: string \| null`  | always present, may be `null`                 |
| `Field.nullish(Schema.String)`                         | `bio?: string \| null` | may be missing or `null`                      |
| `Schema.String.pipe(Field.withDefault(() => "light"))` | `theme: string`        | optional in the constructor and when decoding |

`Field.withDefault` computes the default on each use and applies it both when
constructing (`new Settings({})`) and when decoding a stored row that predates
the field. For a constructor-only default, use
`Schema.withConstructorDefault`; for a decode-only default, use
`Schema.withDecodingDefaultKey`.

## IDs

`Id(prefix, brand)` makes one nominal type per ID. Each is a string with a
fixed prefix and its own brand, so a `UserId` is never accepted where a
`PostId` is expected.

```ts
const UserId = Id("user_", "UserId")
type UserId = typeof UserId.Type

UserId.make("user_01J...") // UserId
UserId.make("post_01J...") // type error: wrong prefix
UserId.generate() // a new ULID-based id from the system clock
const id = yield * UserId.create // a new id from Effect's Clock and Random

class User extends model<User>()("User", {
  id: generated(UserId), // filled in when left out
  invitedBy: Field.nullable(UserId),
}) {}
```

`UserId.create` reads Effect's `Clock` and `Random`, so tests get repeatable
IDs with `TestClock` and `Random.withSeed`.

## Unions

`union(members)` is a `Schema.Union` of models with `from` and `decode`. It
picks the member by `_tag` when the input has one and tries each member in
order when it does not.

```ts
const Shape = union([Circle, Square])
type Shape = typeof Shape.Type // Circle | Square

Shape.from({ _tag: "Circle", radius: 1 }) // Circle
```

A provider can add members to unions too. With `dynamodb`, a union reads mixed
items, for example a post and its comments in one partition:

```ts
const PostContent = union([Post, Comment], dynamodb)
const items =
  yield *
  PostContent.query({
    KeyConditionExpression: "PK = :pk",
    ExpressionAttributeValues: { ":pk": post.PK },
  }) // Array<Post | Comment>
```

## Providers

A provider adds members to models: instance methods, getters, statics and
union members. `dynamodb` adds `put`, `load`, `query` and the rest;
`eventBridge` adds `publish`; `defer` adds `defer`. Pass several as an array;
they merge left to right, and a later member replaces an earlier one with the
same name.

```ts
class InvoiceSent extends model<InvoiceSent>()(
  "InvoiceSent",
  [eventBridge, defer],
  { invoiceId: InvoiceId },
) {
  get source() {
    return "billing"
  }
  get detailType() {
    return "billing.invoice.sent"
  }
}

yield * new InvoiceSent({ invoiceId }).publish()
yield * new InvoiceSent({ invoiceId }).defer(tomorrow)
```

Providers are constant values. They never hold a client or configuration;
their methods return Effects that ask for a service, such as `DynamoDB` or
`EventBridge`, and the layer you provide decides which one they get (see
[Services and layers](#services-and-layers)).

Writing your own takes two steps: declare the members as a type lambda, then
implement them with `makeProvider`. The members can refer to the model through
`this["Self"]` and `this["Fields"]`:

```ts
import { makeProvider, type ProviderMembers } from "@model-ts/core"

interface AuditInstance<Self> {
  /** Writes an audit entry for this item. */
  audit(this: Self & { readonly id: string }, action: string): Effect.Effect<void, never, AuditLog>
}

interface AuditMembers extends ProviderMembers {
  readonly instance: AuditInstance<this["Self"]>
}

export const audited = makeProvider<AuditMembers>()({
  instanceProps: {
    audit(action) {
      return AuditLog.use((log) => log.write(this.id, action))
    },
  },
})

class Invoice extends model<Invoice>()("Invoice", [dynamodb, audited], { ... }) {}
```

A `this` parameter such as `this: Self & { readonly id: string }` states what
the model must have. Calling `audit` on a model without an `id` is a type
error. The `dynamodb` provider uses this to require `PK` and `SK`, and
`eventBridge` to require `source` and `detailType`.

The [core README](packages/core/README.md#providers) covers statics, union
members and `mergeProviders`.

## Services and layers

Every model method that talks to AWS returns an `Effect` that requires a
service:

| Service       | Required by                            | Layers                                                                                |
| ------------- | -------------------------------------- | ------------------------------------------------------------------------------------- |
| `DynamoDB`    | the `dynamodb` provider and `Dynamo.*` | `DynamoDB.layer`, `DynamoDB.layerConfig`, `Sandbox.layerMemory`, `Sandbox.layerLocal` |
| `EventBridge` | `publish`                              | `EventBridge.layer`, `EventBridge.layerConfig`, `EventBridge.layerMemory`             |
| `Deferrer`    | `defer`                                | `Deferrer.layerEventBridge`, `Deferrer.layerMemory`                                   |

Programs say what they need in their type and get it once, at the edge, from a
layer. Production, tests and local development differ only in the layers they
provide, and nothing reads a global client or an environment variable behind
your back.

```ts
const AppLayer = Layer.mergeAll(
  DynamoDB.layerConfig(Config.all({ tableName: Config.String("TABLE_NAME") })),
  EventBridge.layerConfig(
    Config.all({ eventBusName: Config.String("EVENT_BUS_NAME") }),
  ),
)
```

Each layer creates its AWS client when it is built and destroys it when its
scope closes.

## Errors

Every failure is a tagged error in the Effect error channel, a
`Schema.TaggedError` with a readable `message`. The compiler knows which ones
an operation can produce, and `Effect.catchTag` handles them one by one:

```ts
const post =
  yield *
  Post.load(key).pipe(
    Effect.catchTag("ItemNotFoundError", () => Effect.succeed(null)),
  )
```

| Error                         | Raised when                                                                       |
| ----------------------------- | --------------------------------------------------------------------------------- |
| `ItemNotFoundError`           | No item is stored under the key.                                                  |
| `KeyExistsError`              | `put` found an item at the key (pass `IgnoreExistence: true` to overwrite).       |
| `ConditionalCheckFailedError` | A condition you passed was false.                                                 |
| `RaceConditionError`          | The stored item changed since it was read (optimistic locking).                   |
| `TransactionCanceledError`    | DynamoDB cancelled a transaction; `cancellationReasons` says why.                 |
| `BulkWriteRollbackError`      | A bulk write failed and could not be fully rolled back.                           |
| `PaginationError`             | Invalid pagination arguments or an unreadable cursor.                             |
| `ItemDecodeError`             | A stored item does not match the model's schema.                                  |
| `ItemEncodeError`             | An instance holds a value its schema cannot encode.                               |
| `DynamoDBError`               | Any other AWS failure: throttling, permissions, network. `code` has the AWS name. |
| `PublishError`                | EventBridge rejected some entries; `entries` has one result per event.            |
| `EventEncodeError`            | An event could not be encoded, so nothing was sent.                               |
| `EventBridgeError`            | The PutEvents call itself failed.                                                 |

Each operation's error union is exported as a type, such as `Dynamo.GetError`
or `Dynamo.UpdateError`, for use in your own signatures. Defects (bugs, such
as a transaction group of more than 100 operations) are not in the error
channel.

## Tracing

Every operation runs in a span, and every AWS call in a client span inside it,
so a trace shows both what your code asked for and what it sent:

```text
Dynamo.update              model_ts.model=Post  model_ts.key=POST#post_01J... / POST
└─ Dynamo.put
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
| `Deferrer.layerMemory`    | A deferrer that records events; read them through `DeferredEvents`.                                    |

Both sandbox layers also provide `DynamoDB`, run the real AWS SDK client and
send the same requests; only the endpoint differs. The in-memory engine is
checked against DynamoDB Local by this repository's conformance suite.

`Sandbox` seeds the table (`sandbox.seed(...items)`) and takes snapshots, so a test can assert exactly
what an operation wrote. The diff lists each added (`+`), removed (`-`) or
changed item with all of its attributes, and marks the ones that changed:

```ts
import { describe, expect, layer } from "@effect/vitest"
import { Sandbox } from "@model-ts/dynamodb/sandbox"

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
import { Sandbox } from "@model-ts/dynamodb/sandbox"
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
invocation on it. Clients and connections are reused across warm invocations.

```ts
import { Config, Effect, Layer, ManagedRuntime } from "effect"
import { DynamoDB } from "@model-ts/dynamodb"
import { Deferrer, EventBridge } from "@model-ts/eventbridge"

const Bus = EventBridge.layerConfig(
  Config.all({ eventBusName: Config.String("EVENT_BUS_NAME") }),
)

const AppLayer = Layer.mergeAll(
  DynamoDB.layerConfig(Config.all({ tableName: Config.String("TABLE_NAME") })),
  Deferrer.layerEventBridge((deferred) => new DeferredEvent(deferred)),
).pipe(Layer.provideMerge(Bus))

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
| `class X extends model("X", t.type({...}), provider)` | `class X extends model<X>()("X", provider, {...})`                                                                                                        |
| `X.from(input)` / `X.decodeOrThrow(input)`            | `X.from(input)`                                                                                                                                           |
| `X.decode(input)` returns an `Either`                 | `X.decode(input)` returns an `Effect`                                                                                                                     |
| `getProvider(client)`                                 | the constant `dynamodb` provider plus a `DynamoDB` layer                                                                                                  |
| `new Client({ tableName })`                           | `DynamoDB.layer({ tableName })`                                                                                                                           |
| `Promise` results, thrown errors                      | `Effect` results, typed errors (see [Errors](#errors))                                                                                                    |
| `X.load(key, { null: true })`                         | `X.loadOption(key)`, returning an `Option`                                                                                                                |
| `X.loadMany(keys)` with a result or error per key     | `X.loadMany(keys)` fails on the first error; for per-key results, `Effect.forEach(keys, (key) => Effect.exit(X.load(key)), { concurrency: "unbounded" })` |
| `X.iterator(...)` / async iteration                   | `X.stream(...)`, an Effect `Stream`                                                                                                                       |
| `client.bulk([...])`                                  | `Dynamo.bulk([...])`                                                                                                                                      |
| `item._docVersion`                                    | `docVersion(item)`                                                                                                                                        |
| `createSandbox(client)`, `startTracking`, `rollback`  | `Sandbox.layerMemory()` / `Sandbox.layerLocal()`, `snapshot`, `diff`                                                                                      |
| `EXPERIMENTAL_DYNAMODB_IN_MEMORY`                     | `Sandbox.layerMemory()`                                                                                                                                   |
| `getProvider(eventBridgeClient)`, `stubEventBus`      | the `eventBridge` provider plus `EventBridge.layer` / `EventBridge.layerMemory`                                                                           |
| CommonJS                                              | ESM only, Node.js 22+                                                                                                                                     |

Stored data is compatible: the table layout (`PK`/`SK`, `GSI1` to `GSI19`),
the item format including `_tag` and `_docVersion`, soft-deleted keys and
pagination cursors are unchanged. Both versions read and write the same items,
so they can share a table during a migration, and each operation sends the
same requests as before. A few behaviours changed:

- `paginate` keeps querying when a filter or items of other models leave a
  page short, where it used to return a short page that claimed to be the last.
- `bulk` rolls back committed transactions last to first, also after a
  defect, and waits for that before it can be interrupted.
- A retried transaction sends the same `ClientRequestToken` on every attempt,
  so DynamoDB applies it once.
- `loadMany` fails as a whole, as described above.

## Development

The repository is a Bun workspace.

| Command                  | Does                                                      |
| ------------------------ | --------------------------------------------------------- |
| `bun install`            | Installs dependencies.                                    |
| `bun run build`          | Compiles every package to `dist/`.                        |
| `bun run typecheck`      | Type-checks sources and tests, including the type tests.  |
| `bun run test`           | Runs the test suite with the in-memory DynamoDB engine.   |
| `bun run dynamodb:start` | Starts DynamoDB Local in Docker on port 8000.             |
| `bun run test:local`     | Runs the DynamoDB suite and conformance tests against it. |
| `bun run format`         | Formats with Prettier.                                    |

Releases go through [Changesets](https://github.com/changesets/changesets):
add a changeset with `bunx changeset`, and the release workflow versions and
publishes the packages together.

## License

MIT
