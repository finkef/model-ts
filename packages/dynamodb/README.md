# @model-ts/dynamodb

> model-ts Provider for AWS DynamoDB using AWS SDK for JavaScript v3.

- [Installation](#installation)
- [Usage](#usage)
  - [API](#api)
    - [load](#load)
    - [get](#get)
    - [put](#put)
    - [update](#update)
    - [updateRaw](#updateraw)
    - [delete](#delete)
    - [softDelete](#softdelete)
    - [bulk](#bulk)
  - [Effect v4](#effect-v4)
  - [Testing](#testing)
- [License](#license)

## Installation

```sh
npm install io-ts fp-ts @model-ts/core @model-ts/dynamodb
# or
yarn add io-ts fp-ts @model-ts/core @model-ts/dynamodb
```

This package uses AWS SDK for JavaScript v3. Make sure that you have
`@aws-sdk/client-dynamodb` and `@aws-sdk/lib-dynamodb` installed.

## Usage

```ts
import { model } from "@model-ts/core"
import { Client, getProvider } from "@model-ts/dynamodb"

// Create a DynamoDB client
const client = new Client({ tableName: "my-table" })

// Create
const provider = getProvider(client)

class User extends model(
  "User",
  t.type({ id: t.string, firstName: t.string, lastName: t.string }),
  // Pass in the provider
  provider
) {
  // Add a derived PK property
  get PK() {
    return `USER#${this.id}`
  }

  // Add a derived SK property
  get SK() {
    // Here, we're using the same value as PK and SK, so we can ensure uniqueness.
    return `USER#${this.id}`
  }
}

// Now we can use the User model with DynamoDB!
const user = new User({ id: "1", firstName: "John", lastName: "Doe" })
await user.put()

const anotherUser = await User.load({ PK: "USER#2", SK: "USER#2" }) // User {}
```

## Effect v4

`@model-ts/dynamodb/effect` is a lazy Effect facade over the existing client
and provider. The Promise API from `@model-ts/dynamodb` is unchanged.

### Install and requirements

```sh
npm install @model-ts/core@^0.5.0 @model-ts/dynamodb effect@4.0.0-rc.112
```

The Effect entry point requires TypeScript 5.9 or later with `strict: true`
and `moduleResolution` set to `node16`, `nodenext`, or `bundler`. CommonJS
consumers need Node.js 20.19 or later (or Node.js 22.12 or later) because
Effect is ESM-only. `effect` is an optional peer dependency, so applications
that do not import this subpath do not need it.

### Client and provider

`makeEffectClient` wraps the async `Client` operations. `getEffectProvider`
adds the same operations to model classes and instances, returning Effects
instead of Promises.

```ts
import * as Effect from "effect/Effect"
import { model, t } from "@model-ts/core"
import { Client } from "@model-ts/dynamodb"
import { getEffectProvider, makeEffectClient } from "@model-ts/dynamodb/effect"

const client = new Client({ tableName: "users" })
const provider = getEffectProvider(client)

class User extends model(
  "User",
  t.type({ id: t.string, name: t.string }),
  provider
) {
  get PK() {
    return `USER#${this.id}`
  }

  get SK() {
    return `USER#${this.id}`
  }
}

const item = new User({ id: "1", name: "Ada" })
await Effect.runPromise(item.put())

const user = await Effect.runPromise(
  User.get(item.keys()).pipe(
    Effect.catchTag("ItemNotFoundError", () => Effect.succeed(undefined))
  )
)

// Use the client facade directly when an operation has already been built.
const db = makeEffectClient(client)
const sameUser = await Effect.runPromise(
  db.get({ _model: User, _operation: "get", key: item.keys() })
)
```

`getEffectProvider(client)` is a replacement provider: methods such as
`User.get`, `User.load`, `item.put`, and `item.update` now return Effects, not
Promises. Do not combine it with `getProvider(client)` on one model class,
because the method names collide. Define separate model classes for Promise
and Effect usage, or call `Effect.runPromise` at the application boundary.

### Streams and layers

`iterator` returns a re-runnable `Stream` whose elements are DynamoDB result
pages. Flatten it when processing individual model instances.

```ts
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import { DynamoDB } from "@model-ts/dynamodb/effect"

const pages = User.iterator({
  KeyConditionExpression: "PK = :pk",
  ExpressionAttributeValues: { ":pk": "USER#1" },
  ChunkSize: 25,
})

const users = await Effect.runPromise(
  Stream.runCollect(Stream.flattenIterable(pages))
)

const loadedThroughLayer = await Effect.runPromise(
  Effect.gen(function* () {
    const db = yield* DynamoDB
    return yield* db.get({ _model: User, _operation: "get", key: item.keys() })
  }).pipe(Effect.provide(DynamoDB.layerFromClient(client)))
)
```

`DynamoDB.layer(props)` creates a client from `ClientProps`; use
`DynamoDB.layerFromClient(client)` when the application already owns one. The
service is for standalone client access—models remain bound to the client used
when their provider was created.

### Errors and unchanged synchronous APIs

Known DynamoDB domain errors preserve their class and gain a `_tag`, so they
work with `Effect.catchTag`: `KeyExistsError`, `ItemNotFoundError`,
`ConditionalCheckFailedError`, `RaceConditionError`,
`BulkWriteTransactionError`, `BulkWriteRollbackError`, `PaginationError`, and
`NotSupportedError`. Decode failures are `RuntimeTypeValidationError`.
Unexpected SDK, network, and programming failures become
`DynamoDBClientError` with `_tag === "DynamoDBClientError"`, `operation`, and
`cause`.

| Effect-wrapped async APIs                                                                                                                                           | Unchanged synchronous APIs                                                                                                                                                                                   |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `put`, `get`, `load`, `loadMany`, `updateRaw`, `delete`, `softDelete`, `query`, `paginate`, `batchGet`, and `bulk` return `Effect`s. `iterator` returns a `Stream`. | The provider's `dynamodb`, `operation`, `__dynamoDBDecode`, `__dynamoDBEncode`, `keys`, `cursor`, and `applyUpdate` members are reused as-is. Cursor codecs and configuration mutators are also not wrapped. |

### API

> ⚠️ WIP: This documentation is still a work in progress and will be improved soon.

#### load

Load a single item. Uses [data-loader] under the hood to batch `load` calls within the same frame. This is super handy for writing GraphQL APIs.

##### Example

```ts
// Throws if the item doesn't exist.
const item = await MyModel.load({ PK: "MYMODEL#123", SK: "SOMESK#ABC" }) // MyModel

// Returns `null` if the item doesn't exist.
const item = await MyModel.load(
  { PK: "MYMODEL#234", SK: "SOMESK#NOTEXISTING" },
  { null: true }
) // MyModel | null
```

#### get

Get a single item. Prefer `load`, since it comes with extra features and batches calls under the hood.

##### Example

```ts
// Throws if the item doesn't exist.
const item = await MyModel.get({ PK: "MYMODEL#123", SK: "SOMESK#ABC" })
```

#### put

Puts a single item.

##### Example

```ts
const item = new MyModel({ foo: "Hello World", bar: 42 })
await item.put()
```

#### update

Updates a single item. Under the `update` isses a DynamoDB `put` request, instead of `update`, but checks for a `docVersion` field on the item itself to guarantee additional updates aren't overwritten.

##### Example

```ts
const item = await MyModel.load({ PK: "MYMODEL#123", SK: "SOMESK#ABC" })
const updatedItem = await item.update({ foo: "new foo" })
```

#### updateRaw

Updates a single item using a DynamoDB `update` request, prefer to use `update` instead of `updateRaw`.

##### Example

```ts
const updated = await MyModel.updateRaw(
  { PK: "MYMODEL#123", SK: "SOMESK#ABC" },
  { foo: "new foo" },
  {
    UpdateExpression: "SET bar = :newnum",
    ExpressionAttributeValues: { ":newnum": 123 },
  }
)
```

#### delete

Deletes an item.

##### Example

```ts
const item = await MyModel.load({ PK: "MYMODEL#123", SK: "SOMESK#ABC" })
await item.delete()
```

#### softDelete

Deletes an item, but keeps a copy by prepending `$$DELETED$$` to both PK and SK.

##### Example

```ts
const item = await MyModel.load({ PK: "MYMODEL#123", SK: "SOMESK#ABC" })
await item.softDelete()
```

#### bulk

> TODO

## Testing

> TODO

## License

MIT
