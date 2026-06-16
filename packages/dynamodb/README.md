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
