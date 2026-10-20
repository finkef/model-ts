# @model-ts/dynamodb

Single-table DynamoDB for `@model-ts/core` models, built on Effect. Reads are
batched, writes are version-checked, every failure is a typed error, every
call is traced, and tests run against an in-memory table. This page is the API
reference; the [main README](../../README.md) explains the concepts.

> [!NOTE]
> Version 7 is built on Effect v4. The io-ts version is **@model-ts/dynamodb 6.x**.

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
| `GSI1`           | `SK`          | `PK`       |
| `GSI2` … `GSI19` | `GSI<n>PK`    | `GSI<n>SK` |

All keys are strings, and every index projects all attributes. Besides the
model's encoded fields, each item stores `_tag` (the model name) and
`_docVersion` (the version used for optimistic locking).

## Models

Give a model the `dynamodb` provider and `PK`/`SK` getters. Add `GSI<n>PK` and
`GSI<n>SK` getters for the secondary indexes it appears in.

```ts
import { generated, Id, model } from "@model-ts/core"
import { dynamodb } from "@model-ts/dynamodb"
import { Schema } from "effect"

const PostId = Id("post_", "PostId")
type PostId = typeof PostId.Type

class Post extends model<Post>()("Post", dynamodb, {
  id: generated(PostId),
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

| On        | Members                                                                                                                         |
| --------- | ------------------------------------------------------------------------------------------------------------------------------- |
| instances | `put`, `update`, `delete`, `softDelete`, `applyUpdate`, `operation`, `keys`, `cursor`                                           |
| the class | `get`, `load`, `loadOption`, `loadMany`, `query`, `stream`, `paginate`, `put`, `updateRaw`, `delete`, `softDelete`, `operation` |
| unions    | `get`, `load`, `loadOption`, `loadMany`, `query`, `stream`, `paginate`                                                          |

Instance members need `PK` and `SK`; calling them on a model without keys is
a type error. Each member also exists as a function in the `Dynamo`
namespace, which works for any model, union or key:
`Dynamo.load(Post, key)` is `Post.load(key)`.

## The `DynamoDB` service

Every operation requires the `DynamoDB` service. Provide it with a layer:

```ts
import { DynamoDB } from "@model-ts/dynamodb"
import { Config, Effect } from "effect"

const DynamoDBLive = DynamoDB.layer({ tableName: "app" })

// or read the options from Config when the layer is built
const DynamoDBFromEnv = DynamoDB.layerConfig(
  Config.all({ tableName: Config.String("TABLE_NAME") }),
)

program.pipe(Effect.provide(DynamoDBLive))
```

| Option                | Default                      | Meaning                                                                              |
| --------------------- | ---------------------------- | ------------------------------------------------------------------------------------ |
| `tableName`           | required                     | The table every model is stored in.                                                  |
| `client`              | `{}`                         | Passed to `new DynamoDBClient(...)`: region, credentials, endpoint, request handler. |
| `cursorEncryptionKey` | none                         | 32 bytes. Obscures pagination cursors (see [Pagination](#pagination)).               |
| `pagination`          | `{ default: 20, limit: 50 }` | Page size without `first`/`last`, and the largest page.                              |
| `maxBatchSize`        | `100`                        | The most keys one BatchGetItem reads.                                                |

The layer creates the client when it is built and destroys it when its scope
closes.

## Reads

| Member                                          | Returns                                            | Fails with                             |
| ----------------------------------------------- | -------------------------------------------------- | -------------------------------------- |
| `Post.get(key, { consistentRead? })`            | `Post`, from one GetItem                           | `Dynamo.GetError`                      |
| `Post.load(key, { consistentRead?, recover? })` | `Post`, batched with concurrent loads              | `Dynamo.GetError`                      |
| `Post.loadOption(key, options?)`                | `Option<Post>`                                     | `GetError` without `ItemNotFoundError` |
| `Post.loadMany(keys, options?)`                 | `Array<Post>`, in key order                        | `Dynamo.GetError`                      |
| `Post.query(params)`                            | `Array<Post>` with `meta.lastEvaluatedKey`         | `Dynamo.QueryError`                    |
| `Post.stream(params)`                           | `Stream<Post>`, one Query per page, on demand      | `Dynamo.QueryError`                    |
| `Post.paginate(input, params)`                  | a Relay connection (see [Pagination](#pagination)) | `Dynamo.PaginateError`                 |
| `Dynamo.query(params, { posts: Post, ... })`    | items grouped by model, plus `_unknown` and `meta` | `Dynamo.QueryError`                    |

`load` is the read to reach for. Loads that run concurrently, for example in
`Effect.forEach(ids, load, { concurrency: "unbounded" })`, are combined into
BatchGetItem calls.

Query parameters are those of the AWS `QueryCommand` without `TableName`.
Pass `FetchAllPages: true` to follow `LastEvaluatedKey` to the end. A model's
`query` leaves out items it cannot decode; `Dynamo.query` decodes each item
with the model its `_tag` names and returns the rest in `_unknown`:

```ts
const { posts, comments, _unknown } =
  yield *
  Dynamo.query(
    {
      KeyConditionExpression: "PK = :pk",
      ExpressionAttributeValues: { ":pk": post.PK },
    },
    { posts: Post, comments: Comment },
  )
```

## Writes

| Member                                     | Does                                                                                                  | Fails with               |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------- | ------------------------ |
| `post.put(options?)`                       | Stores a new item. Fails if the key exists unless `IgnoreExistence`.                                  | `Dynamo.PutError`        |
| `post.update(patch)`                       | Stores `post` with `patch` applied and returns the new instance. Moves the item when its keys change. | `Dynamo.UpdateError`     |
| `Post.updateRaw(key, attributes, params?)` | One UpdateItem without reading first; returns the updated item.                                       | `Dynamo.UpdateRawError`  |
| `post.delete(options?)`                    | Deletes `post` if it was not changed since it was read.                                               | `Dynamo.DeleteItemError` |
| `Post.delete(key, params?)`                | Deletes whatever is at `key`.                                                                         | `Dynamo.DeleteError`     |
| `post.softDelete(options?)`                | Moves `post` to its deleted keys (see [Soft delete](#soft-delete)).                                   | `Dynamo.SoftDeleteError` |

`put` and `updateRaw` accept `ConditionExpression`,
`ExpressionAttributeNames` and `ExpressionAttributeValues`. A failed condition
of your own is a `ConditionalCheckFailedError`.

## Optimistic locking

Each item stores `_docVersion`, starting at `0`. `update`, `delete` and
`softDelete` only write when the stored version still matches the one the
instance was read with, and `update` increments it. A concurrent change fails
the write with a `RaceConditionError` instead of overwriting it. Read the
version with `docVersion(item)`.

```ts
const post = yield * Post.load(key)
yield *
  post
    .update({ title: "New title" })
    .pipe(Effect.catchTag("RaceConditionError", () => retryWithFreshRead(key)))
```

`post.delete({ ignoreVersion: true })` deletes regardless of the version.

## Soft delete

`softDelete` moves an item to the same keys prefixed with `$$DELETED$$` and
sets `_deletedAt`, in one transaction. The deleted item no longer shows up in
reads or queries of the live keys. To read it back:

```ts
const deleted = yield * Post.load(key, { recover: true })
deleted.isDeleted // true when the item came from its deleted keys
```

`deletedKey(key)` returns the deleted keys of a key.

## Bulk writes and transactions

Build operations, then run them together with `Dynamo.bulk`:

```ts
const [renamed, renameOp] = post.applyUpdate({ title: "Renamed" })

yield *
  Dynamo.bulk([
    draft.operation("put"),
    renameOp,
    Post.operation("delete", oldPost),
    Operation.condition(author, {
      ConditionExpression: "attribute_exists(PK)",
    }),
  ])
```

`bulk` writes up to 100 operations per TransactWriteItems call. A nested array
always stays in one transaction. If a transaction fails, or a defect stops the
bulk write, the transactions already committed are rolled back, last to first,
and the original failure is returned. If a rollback fails as well, the error is
a `BulkWriteRollbackError` that lists the operations still in the table.
Interrupting a bulk write waits until it has finished or been rolled back.

`Dynamo.transactWrite(items)` sends raw `TransactItems`. Server errors and
network failures are retried up to three times with the same
`ClientRequestToken`, so DynamoDB applies a retried transaction only once.
Cancellations and client errors fail right away.

## Pagination

`paginate` implements [Relay connections](https://relay.dev/graphql/connections.htm):
`first`/`after` page forward and `last`/`before` page backward.

```ts
const page =
  yield *
  Post.paginate(
    { first: 20, after: args.after },
    {
      IndexName: "GSI2",
      KeyConditionExpression: "GSI2PK = :pk",
      ExpressionAttributeValues: { ":pk": `AUTHOR#${authorId}` },
    },
  )

page.edges // [{ node: Post, cursor: string }, ...]
page.pageInfo // { hasNextPage, hasPreviousPage, startCursor, endCursor }
```

A page is full even when a filter or items of other models leave a Query
short: `paginate` keeps reading until it has the page plus one item, which
tells whether another page follows.

Cursors are base64 strings that encode the item's keys. With
`cursorEncryptionKey`, they are encrypted with AES-256-CTR under a fixed IV,
the format earlier versions used, so existing cursors stay valid. That hides
the keys from casual inspection, but it is not authenticated encryption: do
not rely on it to keep keys secret or to detect tampering. Invalid arguments or
an unreadable cursor fail with a `PaginationError`.

## Errors

| Error                         | Fields                                       | Raised when                                        |
| ----------------------------- | -------------------------------------------- | -------------------------------------------------- |
| `ItemNotFoundError`           | `key`                                        | No item is stored at the key.                      |
| `KeyExistsError`              | `key`                                        | `put` found an existing item.                      |
| `ConditionalCheckFailedError` | `key`                                        | Your condition was false.                          |
| `RaceConditionError`          | `key`                                        | The item changed since it was read.                |
| `TransactionCanceledError`    | `cancellationReasons`, `cause`               | DynamoDB cancelled a transaction.                  |
| `BulkWriteRollbackError`      | `requiresRollback`, `cause`, `rollbackCause` | A bulk write failed and could not be fully undone. |
| `PaginationError`             | `message`                                    | Invalid pagination arguments or cursor.            |
| `ItemDecodeError`             | `model`, `cause: SchemaError`                | A stored item does not match the model.            |
| `ItemEncodeError`             | `model`, `cause: SchemaError`                | An instance holds a value it cannot encode.        |
| `DynamoDBError`               | `operation`, `cause`, `code`                 | Any other AWS failure.                             |

Each operation's union is exported from `Dynamo`:

| Type                     | Errors                                                                                                    |
| ------------------------ | --------------------------------------------------------------------------------------------------------- |
| `Dynamo.GetError`        | `ItemNotFoundError`, `ItemDecodeError`, `DynamoDBError`                                                   |
| `Dynamo.QueryError`      | `DynamoDBError`                                                                                           |
| `Dynamo.PaginateError`   | `PaginationError`, `DynamoDBError`                                                                        |
| `Dynamo.PutError`        | `KeyExistsError`, `ConditionalCheckFailedError`, `ItemEncodeError`, `DynamoDBError`                       |
| `Dynamo.BulkError`       | `TransactionCanceledError`, `BulkWriteRollbackError`, `ItemEncodeError`, `DynamoDBError`                  |
| `Dynamo.UpdateError`     | `RaceConditionError`, `PutError`, `BulkError`                                                             |
| `Dynamo.UpdateRawError`  | `ItemNotFoundError`, `ConditionalCheckFailedError`, `ItemDecodeError`, `ItemEncodeError`, `DynamoDBError` |
| `Dynamo.DeleteError`     | `ConditionalCheckFailedError`, `DynamoDBError`                                                            |
| `Dynamo.DeleteItemError` | `RaceConditionError`, `DynamoDBError`                                                                     |
| `Dynamo.SoftDeleteError` | `RaceConditionError`, `BulkError`                                                                         |

## Tracing

Each operation has a span named `Dynamo.<operation>` (`Dynamo.load`,
`Dynamo.update`, …) with these attributes:

| Attribute        | Value                                          |
| ---------------- | ---------------------------------------------- |
| `model_ts.model` | The model, or `"Post \| Comment"` for a union. |
| `model_ts.key`   | `"PK / SK"` of the item, when there is one.    |

Inside it, every AWS call has a client span named `DynamoDB.<Command>`
(`DynamoDB.GetItem`, `DynamoDB.BatchGetItem`, …) with the OpenTelemetry
attributes for DynamoDB: `db.system.name`, `db.operation.name`,
`aws.dynamodb.table_names`, `rpc.system`, `rpc.service` and `rpc.method`, plus
`aws.dynamodb.index_name` for index queries. Failed calls end their span with
the failure.

Concurrent loads share one BatchGetItem. Its `DynamoDB.BatchGetItem` span sits
in a `Dynamo.batchGet` span that links to the `Dynamo.load` span of every
load it served, so each caller's trace leads to the call that answered it.

## Testing with `Sandbox`

`@model-ts/dynamodb/sandbox` provides tables for tests. Both layers provide
`DynamoDB` and `Sandbox`, and create a fresh table each time they are built.

| Layer                           | Table                                                                                     |
| ------------------------------- | ----------------------------------------------------------------------------------------- |
| `Sandbox.layerMemory(options?)` | In memory, inside the AWS SDK client. Nothing to start.                                   |
| `Sandbox.layerLocal(options?)`  | On DynamoDB Local (`http://localhost:8000` unless `endpoint` is set); deleted afterwards. |

Both run the real AWS SDK client; the in-memory engine answers its requests
instead of the network. Options are the `DynamoDB.layer` options without
`tableName` and `client`.

| `Sandbox` member | Does                                                              |
| ---------------- | ----------------------------------------------------------------- |
| `tableName`      | The generated table name.                                         |
| `seed(...items)` | Writes model instances or raw items directly, without conditions. |
| `snapshot`       | Every item in the table, keyed by `"PK__SK"`.                     |
| `diff(before)`   | A readable diff between `before` and now, for snapshot tests.     |
| `get(key)`       | The raw stored item, or `undefined`.                              |

```ts
import { expect, layer } from "@effect/vitest"
import { Sandbox } from "@model-ts/dynamodb/sandbox"

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

The in-memory engine supports the operations and expressions this library
uses, and the repository's conformance suite runs the same requests against
it and DynamoDB Local. It is meant for tests, not as a database.
