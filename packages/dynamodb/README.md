# @model-ts/dynamodb

Single-table DynamoDB for `@model-ts/core` models, built on Effect. Reads are
batched, writes are version-checked, every failure is a typed error, every call
is traced, and tests run against an in-memory table. This page is the API
reference; the [main README](../../README.md) explains the concepts.

> [!NOTE] Version 7 is built on Effect v4. The io-ts version is
> **@model-ts/dynamodb 6.x**.

```sh
npm install @model-ts/core @model-ts/dynamodb effect \
  @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb @aws-sdk/util-dynamodb
```

- [Table layout](#table-layout)
- [Models](#models)
- [The `DynamoDB` service](#the-dynamodb-service)
- [Reads](#reads)
- [Writes](#writes)
- [Optimistic locking](#optimistic-locking)
- [Soft delete](#soft-delete)
- [Bulk writes and transactions](#bulk-writes-and-transactions)
- [Pagination](#pagination)
- [Errors](#errors)
- [Tracing](#tracing)
- [Testing with `Sandbox`](#testing-with-sandbox)

## Table layout

All models share one table:

| Index            | Partition key | Sort key   |
| ---------------- | ------------- | ---------- |
| primary          | `PK`          | `SK`       |
| `GSI1` … `GSI19` | `GSI<n>PK`    | `GSI<n>SK` |

All keys are strings, and every index projects all attributes. Besides the
model's encoded fields, each item stores `_tag` (the model name) and
`_docVersion` (the version used for optimistic locking).

## Models

Give a model `DynamoDB.provider` and `PK`/`SK` getters. Add `GSI<n>PK` and
`GSI<n>SK` getters for the secondary indexes it appears in.

```ts
import { Model } from "@model-ts/core"
import { DynamoDB } from "@model-ts/dynamodb"
import { Schema } from "effect"

const PostId = Schema.String.pipe(Schema.brand("PostId"))
type PostId = typeof PostId.Type

class Post extends Model.Class<Post>()("Post", DynamoDB.provider, {
  id: PostId,
  authorId: Schema.String,
  title: Schema.String,
}) {
  get PK() {
    return `POST#${this.id}`
  }
  get SK() {
    return "POST"
  }
  get GSI2PK() {
    return `AUTHOR#${this.authorId}`
  }
  get GSI2SK() {
    return `POST#${this.id}`
  }

  static key(id: PostId) {
    return { PK: `POST#${id}`, SK: "POST" }
  }
}
```

The provider adds:

| On        | Members                                                                                                             |
| --------- | ------------------------------------------------------------------------------------------------------------------- |
| instances | `put`, `update`, `delete`, `softDelete`, `applyUpdate`, `operation`, `keys`, `cursor`                               |
| the class | `get`, `load`, `loadOption`, `loadMany`, `where`, `query`, `stream`, `paginate`, `updateRaw`, `delete`, `operation` |
| unions    | `get`, `load`, `loadOption`, `loadMany`, `where`, `query`, `stream`, `paginate`                                     |

Instance members need `PK` and `SK`; calling them on a model without keys is a
type error.

The reads and writes wrap the operations on `DynamoDB`, which take the
model, union or item as their first argument: `Post.load(key)` is
`DynamoDB.load(Post, key)` and `post.update(patch)` is
`DynamoDB.update(post, patch)`. `applyUpdate` and `operation` build operations
with the `Operation` module (see [Bulk writes](#bulk-writes-and-transactions)),
and `keys()` returns the item's `PK`, `SK` and GSI attributes.

## The `DynamoDB` service

Every operation requires the `DynamoDB` service. Provide it with a layer:

```ts
import { DynamoDB } from "@model-ts/dynamodb"
import { Config, Effect, Layer } from "effect"

const DynamoDBLive = DynamoDB.layer({
  tableName: "app",
  cursors: { encryptionKey: process.env.CURSOR_KEY! },
})

// or read the options from Config when the layer is built
const DynamoDBFromEnv = Layer.unwrap(
  Effect.gen(function* () {
    return DynamoDB.layer({
      tableName: yield* Config.String("TABLE_NAME"),
      cursors: { encryptionKey: yield* Config.Redacted("CURSOR_KEY") },
    })
  }),
)

program.pipe(Effect.provide(DynamoDBLive))
```

| Option         | Default                      | Meaning                                                                              |
| -------------- | ---------------------------- | ------------------------------------------------------------------------------------ |
| `tableName`    | required                     | The table every model is stored in.                                                  |
| `client`       | `{}`                         | Passed to `new DynamoDBClient(...)`: region, credentials, endpoint, request handler. |
| `cursors`      | required                     | `{ encryptionKey, previousKeys? }` or `"plain"` (see [Pagination](#pagination)).     |
| `pagination`   | `{ default: 20, limit: 50 }` | Page size without `first`/`last`, and the largest page.                              |
| `maxBatchSize` | `100`                        | The most keys one BatchGetItem reads.                                                |

The layer creates the client when it is built and destroys it when its scope
closes.

## Reads

| Member                                          | Returns                                            | Fails with                 |
| ----------------------------------------------- | -------------------------------------------------- | -------------------------- |
| `Post.get(key, { consistentRead? })`            | `Post`, from one GetItem                           | `DynamoDB.GetError`        |
| `Post.load(key, { consistentRead?, recover? })` | `Post`, batched with concurrent loads              | `DynamoDB.GetError`        |
| `Post.loadOption(key, options?)`                | `Option<Post>`, `None` when the key holds no item  | `DynamoDB.LoadOptionError` |
| `Post.loadMany(keys, options?)`                 | `Array<Post>`, in key order                        | `DynamoDB.GetError`        |
| `Post.query(params)`                            | `Array<Post>` with `meta.lastEvaluatedKey`         | `DynamoDB.QueryError`      |
| `Post.stream(params)`                           | `Stream<Post>`, one Query per page, on demand      | `DynamoDB.QueryError`      |
| `Post.paginate(input, params)`                  | a Relay connection (see [Pagination](#pagination)) | `DynamoDB.PaginateError`   |
| `DynamoDB.query(params, { posts: Post, ... })`  | items grouped by model, plus `_unknown` and `meta` | `DynamoDB.QueryError`      |

`load` is the read to reach for. Loads issued in the same tick are combined
into BatchGetItem calls, as DataLoader combines them in the io-ts versions.
That holds for concurrent fibers, as in
`Effect.forEach(ids, load, { concurrency: "unbounded" })`, and for separate
`runtime.runPromise` calls, such as GraphQL resolvers that each run their own
Effect: every level of a nested query reads its items in one BatchGetItem.
Loads only share a batch when they run on the same `DynamoDB` layer, so build
it once, for example in a `ManagedRuntime`, and not with `Effect.provide` in
each resolver.

Use `loadOption` when a missing item is an expected outcome, such as settings a
user has never saved or a lookup by slug. It is `load` with `Option.none()` in place
of `ItemNotFoundError`, so it batches and caches the same way, and decode and
DynamoDB errors still fail. A resolver of a nullable `@model-ts/graphql` field
can return the `Option` as is; `None` resolves to `null`.

```ts
const settings = Settings.loadOption(Settings.key(userId)).pipe(
  Effect.map(Option.getOrElse(() => new Settings({ userId }))),
)
```

`where` keeps a loaded item only when a predicate holds, for rules that live
on the model, such as "belongs to this organization". Put it after the read in
`pipe`; the predicate is typed with the model.

```ts
const archiveList = Effect.fn("archiveList")(function* (
  id: ListId,
  organizationId: OrganizationId,
) {
  const list = yield* List.load(List.key(id)).pipe(
    List.where((list) => list.isBelongingToOrganization(organizationId)),
  )
  return yield* list.update({ archived: true })
})

const membership = Membership.loadOption(Membership.key(id)).pipe(
  Membership.where(
    (membership) =>
      membership.isBelongingToUser(userId) && membership.isActive(),
  ),
)
```

After a read of an item, a rejected item fails with `ItemNotFoundError` for its
key, so a caller can't tell it apart from a key that holds no item. After a read
of an `Option`, such as `loadOption` or your own finder, it becomes
`Option.none()` and the error type stays as it was. `where` runs after the
read, so the read still batches and caches, and steps can be chained.
`DynamoDB.where(List, predicate)` is the same step.

Query parameters are those of the AWS `QueryCommand` without `TableName`. Pass
`fetchAllPages: true` to follow `LastEvaluatedKey` to the end. Each item is
decoded with the model its `_tag` names. A model's `query` leaves out items of
other models; `DynamoDB.query` returns them in `_unknown`. An item tagged with a
requested model that doesn't decode is never skipped: the query fails with an
`ItemDecodeError` that names the model and the item's key. Items without `_tag`
are tried against every model and go to `_unknown` when none decodes them.
Because the result has `_unknown` and `meta`, those two names cannot be used for
a group.

```ts
const program = Effect.gen(function* () {
  const { posts, comments, _unknown } = yield* DynamoDB.query(
    {
      KeyConditionExpression: "PK = :pk",
      ExpressionAttributeValues: { ":pk": post.PK },
    },
    { posts: Post, comments: Comment },
  )
})
```

## Request cache

A Lambda invocation often loads the same item several times: once to check a
permission, again in a nested resolver, again after updating it. Wrap the
request in `DynamoDB.withRequestCache` and each key is read at most once:

```ts
export const handler = (event: APIGatewayProxyEvent) =>
  runtime.runPromise(handle(event).pipe(DynamoDB.withRequestCache))
```

Inside the request, `load` and `loadMany` return what the request already
knows about a key, including that it holds no item, and a load of a key that is
already being read waits for that read. Keys the cache doesn't know still go
through the batched read, so a GraphQL level of partly cached items costs one
BatchGetItem for the rest.

Writes keep the cache current. A successful `put`, `update`, `updateRaw`,
`delete`, `softDelete` or bulk transaction (including its rollback) records the
item it stored, as a read would return it, or that the key is now empty. The
cache forgets a key instead when it can't know what the table holds:

- after a failed or interrupted write, for example one that failed with
  `RaceConditionError`, so a retry loads fresh data;
- after writes of the key that ran at the same time, since their responses
  don't show which one DynamoDB applied last;
- after an `updateRaw` inside `bulk`, because transactions don't return items.

A read never replaces what a write, an invalidation or a consistent read that
started after it recorded. Every load gets its own copy of the item, so changing
one instance never changes what the next load returns.

`get`, `query`, `stream` and `paginate` always read the table, and
`load(key, { consistentRead: true })` reads the table and refreshes the entry.

The cache only sees writes made through model-ts in the same request. After
writing some other way, call `RequestCache.invalidate(...keys)` (include
`deletedKey(key)` when the write moved a soft-deleted item) or
`RequestCache.clear`, or load with `consistentRead: true`. Create one cache per
request, never one for the container: another invocation's writes would never
reach it. When one request runs several effects, as GraphQL resolvers that each
call `runPromise` do, make the cache once per request and provide it to each of
them. Keep the `DynamoDB` layer in the shared runtime, so resolvers still share
batches:

```ts
const execute = async (request: GraphQLRequest) => {
  const cache = await runtime.runPromise(RequestCache.make)
  const run = <A, E>(effect: Effect.Effect<A, E, AppServices>) =>
    runtime.runPromise(Effect.provideService(effect, RequestCache, cache))
  return graphql({ schema, source: request.query, contextValue: { run } })
}
```

## Writes

| Member                                     | Does                                                                                                  | Fails with                 |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------- | -------------------------- |
| `post.put(options?)`                       | Stores a new item. Fails if the key exists unless `ignoreExistence`.                                  | `DynamoDB.PutError`        |
| `post.update(patch)`                       | Stores `post` with `patch` applied and returns the new instance. Moves the item when its keys change. | `DynamoDB.UpdateError`     |
| `Post.updateRaw(key, attributes, params?)` | One UpdateItem without reading first; returns the updated item.                                       | `DynamoDB.UpdateRawError`  |
| `post.delete(options?)`                    | Deletes `post` if it was not changed since it was read.                                               | `DynamoDB.DeleteItemError` |
| `Post.delete(key, params?)`                | Deletes whatever is at `key`.                                                                         | `DynamoDB.DeleteError`     |
| `post.softDelete(options?)`                | Moves `post` to its deleted keys (see [Soft delete](#soft-delete)).                                   | `DynamoDB.SoftDeleteError` |

`put` and `updateRaw` accept `ConditionExpression`, `ExpressionAttributeNames`
and `ExpressionAttributeValues`. A failed condition of your own is a
`ConditionalCheckFailedError`. `updateRaw` adds your condition to its own check
that the item exists, so it never creates an item, and its placeholders never
collide with yours. Pass either `attributes` or your own `UpdateExpression`;
`updateRaw` adds the version bump to either.

## Optimistic locking

Each item stores `_docVersion`, starting at `0`. `update`, `delete` and
`softDelete` only write when the stored version still matches the one the
instance was read with. `update` and `updateRaw` increment it, so an instance
read before a raw update can no longer overwrite it. A concurrent change or
delete fails the write with a `RaceConditionError` instead of overwriting the
item or writing it again. An `update` that changes the keys checks the old
item's version before moving it, and fails with `KeyExistsError` when the new
keys are taken. Read the version with `docVersion(item)`.

```ts
const program = Effect.gen(function* () {
  const post = yield* Post.load(key)
  yield* post
    .update({ title: "New title" })
    .pipe(Effect.catchTag("RaceConditionError", () => retryWithFreshRead(key)))
})
```

`post.delete({ ignoreVersion: true })` deletes regardless of the version. An
`update` on an instance that was never read from the table has no version to
check, so it writes without a condition.

## Soft delete

`softDelete` moves an item to the same keys prefixed with `$$DELETED$$` and sets
`_deletedAt`, in one transaction. The deleted item no longer shows up in reads
or queries of the live keys. To read it back:

```ts
const program = Effect.gen(function* () {
  const deleted = yield* Post.load(key, { recover: true })
  deleted.isDeleted // true when the item came from its deleted keys
})
```

`deletedKey(key)` returns the deleted keys of a key.

## Bulk writes and transactions

Build operations, then run them together with `DynamoDB.bulk`:

```ts
const [renamed, renameOp] = post.applyUpdate({ title: "Renamed" })

const program = Effect.gen(function* () {
  yield* DynamoDB.bulk([
    draft.operation("put"),
    renameOp,
    Post.operation("delete", oldPost),
    Operation.condition(author, {
      ConditionExpression: "attribute_exists(PK)",
    }),
  ])
})
```

`bulk` writes up to 100 operations per TransactWriteItems call. A nested array
always stays in one transaction. If a transaction fails, or a defect stops the
bulk write, the transactions already committed are rolled back, last to first,
and the original failure is returned. If a rollback fails as well, the error is
a `BulkWriteRollbackError` that lists the operations still in the table.
Interrupting a bulk write waits until it has finished or been rolled back.

Rolling back undoes the operations that know how: `put`, `softDelete` and an
`update` that changes keys are `TransactionOperation`s with a `rollback`. A
rollback that deletes an item it created only does so while the item still has
the version it was written with; if something changed it since, the rollback
fails and you get a `BulkWriteRollbackError` rather than a lost change. A `put`
with `ignoreExistence` or a condition of your own may replace an item, which
cannot be restored, so it has no rollback. A same-key `update`, `updateRaw`,
`delete` and `condition` have none, and a committed one stays applied. To make
your own operation reversible, pass `{ action, rollback }`.

Each transaction is retried up to three times on server errors and network
failures, with the same `ClientRequestToken`, so DynamoDB applies a retried
transaction only once. Cancellations and client errors fail right away.

## Pagination

`paginate` implements
[Relay connections](https://relay.dev/graphql/connections.htm): `first`/`after`
page forward and `last`/`before` page backward.

```ts
const program = Effect.gen(function* () {
  const page = yield* Post.paginate(
    { first: 20, after: args.after },
    {
      IndexName: "GSI2",
      KeyConditionExpression: "GSI2PK = :pk",
      ExpressionAttributeValues: { ":pk": `AUTHOR#${authorId}` },
    },
  )

  page.edges // [{ node: Post, cursor: string }, ...]
  page.pageInfo // { hasNextPage, hasPreviousPage, startCursor, endCursor }
})
```

A page is full even when a filter or items of other models leave a Query short:
`paginate` keeps reading until it has the page plus one item, which tells
whether another page follows.

A cursor holds the keys the item is stored with: `PK`, `SK` and every GSI
attribute. It therefore works in every connection the item appears in, so
`item.cursor()` can build the edge of an item a mutation just added to a list.
The same item always gets the same cursor.

Every table chooses how cursors are written with the `cursors` option:

| `cursors`                                   | Cursors                                                                                                                     |
| ------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `{ encryptionKey }`                         | Encrypted and authenticated with AES-256-GCM. Clients can't read the keys, and an altered or forged cursor is rejected.     |
| `{ encryptionKey, previousKeys: [oldKey] }` | New cursors use `encryptionKey`; cursors made with an older key still work. Use it to rotate the key.                       |
| `"plain"`                                   | Base64-encoded JSON. Clients can read every key attribute of the item, GSI keys included, so use it only for harmless keys. |

A key is 32 bytes, as a `Uint8Array` or a base64 string (`openssl rand -base64
32`), and may be `Redacted`, as `Config.Redacted` returns it. A layer with an
invalid key fails to build. Encrypted cursors from the io-ts versions, which
used AES-256-CTR, are not accepted.

Invalid arguments fail with a `PaginationError`, and so does a cursor that
can't be read or that belongs to another query: DynamoDB only resumes a query
from a key that query could have returned.

## Errors

| Error                         | Fields                                       | Raised when                                                                         |
| ----------------------------- | -------------------------------------------- | ----------------------------------------------------------------------------------- |
| `ItemNotFoundError`           | `key`                                        | No item is stored at the key.                                                       |
| `KeyExistsError`              | `key`                                        | `put` found an existing item.                                                       |
| `ConditionalCheckFailedError` | `key`                                        | Your condition was false.                                                           |
| `RaceConditionError`          | `key`                                        | The item changed or was deleted since it was read.                                  |
| `TransactionCanceledError`    | `cancellationReasons`, `cause`               | DynamoDB cancelled a transaction.                                                   |
| `BulkWriteRollbackError`      | `requiresRollback`, `cause`, `rollbackCause` | A bulk write failed and could not be fully undone.                                  |
| `PaginationError`             | `message`                                    | Invalid pagination arguments, or a cursor that is unreadable or from another query. |
| `ItemDecodeError`             | `model`, `key?`, `cause: SchemaError`        | A stored item does not match the model.                                             |
| `ItemEncodeError`             | `model`, `cause: SchemaError`                | An instance holds a value it cannot encode.                                         |
| `DynamoDBError`               | `operation`, `cause`, `code`                 | Any other AWS failure.                                                              |

Each operation's union is a type in the `DynamoDB` namespace:

| Type                        | Errors                                                                                                    |
| --------------------------- | --------------------------------------------------------------------------------------------------------- |
| `DynamoDB.GetError`         | `ItemNotFoundError`, `ItemDecodeError`, `DynamoDBError`                                                   |
| `DynamoDB.QueryError`       | `ItemDecodeError`, `DynamoDBError`                                                                        |
| `DynamoDB.PaginateError`    | `PaginationError`, `ItemDecodeError`, `DynamoDBError`                                                     |
| `DynamoDB.PutError`         | `KeyExistsError`, `ConditionalCheckFailedError`, `ItemEncodeError`, `DynamoDBError`                       |
| `DynamoDB.BulkError`        | `TransactionCanceledError`, `BulkWriteRollbackError`, `ItemEncodeError`, `DynamoDBError`                  |
| `DynamoDB.TransactionError` | `TransactionCanceledError`, `ItemEncodeError`, `DynamoDBError`                                            |
| `DynamoDB.UpdateError`      | `RaceConditionError`, `KeyExistsError`, `TransactionError`                                                |
| `DynamoDB.UpdateRawError`   | `ItemNotFoundError`, `ConditionalCheckFailedError`, `ItemDecodeError`, `ItemEncodeError`, `DynamoDBError` |
| `DynamoDB.DeleteError`      | `ConditionalCheckFailedError`, `DynamoDBError`                                                            |
| `DynamoDB.DeleteItemError`  | `RaceConditionError`, `DynamoDBError`                                                                     |
| `DynamoDB.SoftDeleteError`  | `RaceConditionError`, `TransactionError`                                                                  |

## Tracing

Every operation runs in a span named after its `DynamoDB` static. Model members
such as `Post.load` create the same spans, and `loadMany`,
`stream` and a model's `query` run inside the spans of the operations they call.

| Span                                                                                                                                                          | Attributes                                                                                           |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `DynamoDB.get`, `DynamoDB.load`, `DynamoDB.loadOption`, `DynamoDB.put`, `DynamoDB.update`, `DynamoDB.updateRaw`, `DynamoDB.deleteItem`, `DynamoDB.softDelete` | `model_ts.model` (`"Post \| Comment"` for a union), `model_ts.key` (`"PK / SK"`)                     |
| `DynamoDB.delete`                                                                                                                                             | `model_ts.key`                                                                                       |
| `DynamoDB.query`                                                                                                                                              | `model_ts.models`, `aws.dynamodb.index_name`, `aws.dynamodb.count`, `model_ts.unknown_count`         |
| `DynamoDB.paginate`                                                                                                                                           | none; its Queries are `DynamoDB.query` spans                                                         |
| `DynamoDB.bulk`                                                                                                                                               | `model_ts.operations`, `model_ts.transactions`                                                       |
| `DynamoDB.transactWrite`, one per transaction of `bulk`                                                                                                       | none                                                                                                 |
| `DynamoDB.batchGet`                                                                                                                                           | `batchSize`, and a link to the `DynamoDB.load` or `DynamoDB.loadOption` span of every load it served |

Inside them, every AWS call has a client span named `DynamoDB.<Command>`
(`DynamoDB.GetItem`, `DynamoDB.Query`, …) with the OpenTelemetry attributes for
DynamoDB: `db.system.name`, `db.operation.name`, `aws.dynamodb.table_names`,
`rpc.system`, `rpc.service` and `rpc.method`. Some commands add their own:
`aws.dynamodb.index_name`, `aws.dynamodb.limit` and `aws.dynamodb.scan_forward`
on Query, `aws.dynamodb.keys_count` on BatchGetItem and
`aws.dynamodb.transact_items_count` on TransactWriteItems. Failed calls end
their span with the failure.

Concurrent loads share one BatchGetItem, which runs in the `DynamoDB.batchGet`
span, so each caller's trace leads to the call that answered it.

## Testing with `Sandbox`

`@model-ts/dynamodb/Sandbox` provides tables for tests. Both layers provide
`DynamoDB` and `Sandbox`, and create a fresh table each time they are built.

| Layer                           | Table                                                                                     |
| ------------------------------- | ----------------------------------------------------------------------------------------- |
| `Sandbox.layerMemory(options?)` | In memory, inside the AWS SDK client. Nothing to start.                                   |
| `Sandbox.layerLocal(options?)`  | On DynamoDB Local (`http://localhost:8000` unless `endpoint` is set); deleted afterwards. |

Both run the real AWS SDK client; the in-memory engine answers its requests
instead of the network. Options are the `DynamoDB.layer` options without
`tableName` and `client`. `cursors` is optional: by default, sandboxes encrypt
cursors with a fixed test key, so the same items get the same cursors on every
run and cursors can go into snapshots. Pass `cursors: "plain"` to read them.

| `Sandbox` member | Does                                                                     |
| ---------------- | ------------------------------------------------------------------------ |
| `tableName`      | The generated table name.                                                |
| `seed(...items)` | Writes model instances or raw items directly, without conditions.        |
| `snapshot`       | Every item in the table, keyed by `JSON.stringify([PK, SK])`.            |
| `diff(before)`   | A readable diff between `before` and now, for snapshot tests.            |
| `get(key)`       | The raw stored item, or `undefined`.                                     |
| `changes`        | Subscribes to the table's stream. See [Change streams](#change-streams). |

```ts
import { expect, layer } from "@effect/vitest"
import { Sandbox } from "@model-ts/dynamodb/Sandbox"

layer(Sandbox.layerMemory())((it) => {
  it.effect("renames a post", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(post)
      const before = yield* sandbox.snapshot

      yield* (yield* Post.load(post)).update({ title: "Renamed" })

      expect(yield* sandbox.diff(before)).toMatchSnapshot()
    }),
  )
})
```

The in-memory engine is meant for tests, not as a database. It supports what
this library sends: GetItem, PutItem, UpdateItem, DeleteItem, Query, Scan,
BatchGetItem, BatchWriteItem and TransactWriteItems, with key condition, filter,
condition and update expressions. The repository's conformance suite runs the
same requests against it and DynamoDB Local. It does not support:

- `ProjectionExpression` and `Select`,
- the legacy parameters `KeyConditions`, `QueryFilter`, `AttributesToGet` and
  `Expected`,
- idempotent retries with `ClientRequestToken` (the token is accepted).

A request that uses one of these fails with a `DynamoDBError` that names the
missing feature. Tests that need them use `Sandbox.layerLocal()`.

### Change streams

Both sandboxes emit the records a DynamoDB stream with the
`NEW_AND_OLD_IMAGES` view would carry, shaped as AWS Lambda receives them.
Stream handlers can therefore run against a sandbox unchanged, in tests and in
local development. `yield* sandbox.changes` subscribes and returns the stream
of records for writes committed from then on. The subscription ends with the
scope.

```ts
import { Sandbox } from "@model-ts/dynamodb/Sandbox"
import type { StreamRecord } from "@model-ts/dynamodb/Stream"

it.effect("emits the rename", () =>
  Effect.gen(function* () {
    const changes = yield* (yield* Sandbox).changes

    yield* (yield* Post.load(post)).update({ title: "Renamed" })

    const [record] = yield* changes.pipe(Stream.take(1), Stream.runCollect)
    expect(record.eventName).toBe("MODIFY")
    expect(record.dynamodb.NewImage?.title).toEqual({ S: "Renamed" })
  }),
)
```

The records follow DynamoDB's rules:

- A put of a new item is an `INSERT`, any other change a `MODIFY`, and a
  delete a `REMOVE`. `OldImage` and `NewImage` hold the whole item before and
  after.
- A write that leaves the item unchanged emits nothing, and neither does a
  cancelled transaction.
- Records arrive in commit order and their `SequenceNumber`s increase.
- Attribute values are wire JSON, with binary values as base64 strings. The
  `StreamRecord` and `StreamEvent` types in `@model-ts/dynamodb/Stream`
  describe them.

On DynamoDB Local, `changes` reads the table's real stream, which needs the
optional peer dependency `@aws-sdk/client-dynamodb-streams`. The memory
sandbox needs nothing extra. TTL deletions are not emitted: neither sandbox
expires items.
