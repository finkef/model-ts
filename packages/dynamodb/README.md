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

Permanently deletes an item. Loaded instances check their `_docVersion` by default,
including whether the live item still exists. A stale or missing instance throws
`RaceConditionError`. Use `{ ignoreVersion: true }` for deliberate force deletion.
Key-only `MyModel.delete(key, params?)` remains unconditional unless you supply
standard DynamoDB `ConditionExpression`, `ExpressionAttributeNames`, and
`ExpressionAttributeValues` params; raw conditional failures retain SDK errors.

Decoded legacy rows without a stored version count as version zero. A manually
constructed instance without version metadata retains unconditional behavior;
load the item first to obtain protection. Checks cannot detect writes that leave
the version unchanged or recreation that reuses the version.

##### Example

```ts
const item = await MyModel.load({ PK: "MYMODEL#123", SK: "SOMESK#ABC" })
await item.delete() // rejects if the stored version changed
await MyModel.delete({ PK: "MYMODEL#123", SK: "SOMESK#ABC" }) // unconditional
// For administrative force deletion: await item.delete({ ignoreVersion: true })
```

#### softDelete

Deletes the live item and archives the supplied snapshot under `$$DELETED$$` keys
in one transaction. Instance, `MyModel.softDelete(item, options?)`, and
`client.softDelete(item, options?)` calls check the item version by default.
`{ ignoreVersion: true }` skips the live-item guard and may archive a stale snapshot;
it never permits overwriting an existing archive.

Direct soft-delete calls throw `RaceConditionError` only when structured
cancellation reasons identify the live version guard alone. Archive collisions,
mixed failures, or unavailable reasons retain `BulkWriteTransactionError`.
Recovery reads via `load(key, { recover: true })` are unchanged; no restore
primitive is provided.

##### Example

```ts
const item = await MyModel.load({ PK: "MYMODEL#123", SK: "SOMESK#ABC" })
await item.softDelete()
```

#### bulk

Instance `operation("delete", options?)` and `operation("softDelete", options?)`
builders retain the same version guards and `ignoreVersion` option. Static
`MyModel.operation("softDelete", item, options?)` also uses the item version;
key-only `MyModel.operation("delete", key, params?)` accepts raw conditions and
otherwise remains unconditional. `client.delete(operation)` forwards conditions
without inferring a version or mapping SDK errors.

```ts
await client.bulk([
  first.operation("delete"),
  second.operation("softDelete"), // keep the pair nested
])
```

Transactions contain at most 100 actions. Each nested array stays together,
including during compensation after a later transaction fails. Groups over 100
are rejected before any writes. Use `bulk([pair])`; `bulk(pair)` or spreading a
pair into the outer array does not preserve its grouping. Soft-delete tuples are
ordered live Delete then archive Put (their declarations now match runtime).

Generic `bulk()` retains `BulkWriteTransactionError` for transaction cancellations,
even for generated version guards. Compensation failures retain
`BulkWriteRollbackError` and `requiresRollback`. Multi-batch bulk is not globally
atomic: only operations with declared rollbacks are compensated. A soft-delete
restore refuses to overwrite a recreated live row, leaving the archive intact.

Version 6 changes the defaults for loaded-instance deletes and item-taking soft
deletes. Existing calls still compile, but stale/missing items now reject. Migrate
intentional force deletion to `ignoreVersion: true` or key-only static deletion;
callers inspecting soft-delete tuple positions should use the corrected order.

## Testing

> TODO

## License

MIT
