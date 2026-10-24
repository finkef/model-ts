# @model-ts/eventbridge

## 7.2.0

## 7.1.0

## 7.0.0

### Major Changes

- bb6d84e: Rewrite model-ts on Effect v4. Models are `Schema.Class`es built with `Model.Class<Self>()(tag, providers, fields)`, DynamoDB and EventBridge are Effect services provided through layers, every operation fails with typed errors and is traced, and `effect` is a peer dependency. The io-ts versions stay available as @model-ts/core 0.4.x, @model-ts/dynamodb 6.x and @model-ts/eventbridge 3.x. See the README for the migration guide.

  The API follows Effect's naming. Core exports the `Model` and `Provider` namespaces (`Model.Class`, `Model.Union`, `Provider.make`, `Provider.merge`). The providers live on their services as `DynamoDB.provider` and `EventBridge.provider`, and layers are static `layer*` members such as `DynamoDB.layer` and `Sandbox.layerMemory`. The table operations are statics of the `DynamoDB` service (`DynamoDB.load(Post, key)`, `DynamoDB.bulk([...])`), and their error and parameter types are in the `DynamoDB` namespace (`DynamoDB.GetError`, `DynamoDB.QueryParams`). Every module can be imported on its own, as in `@model-ts/dynamodb/Sandbox`.

  Two changes affect existing tables. `GSI1` is now a regular index keyed by `GSI1PK`/`GSI1SK` instead of the inverted primary key, and queries and updates no longer skip items that carry `dynamotorLegacy`.

  Every table now chooses how pagination cursors are written: `DynamoDB.layer` requires `cursors: { encryptionKey }` (authenticated AES-256-GCM, with `previousKeys` for key rotation) or `cursors: "plain"`. Cursors encrypted by the io-ts versions are rejected, and a cursor from another query fails with `PaginationError`. Sandboxes encrypt cursors with a fixed test key, so cursors are the same on every run. `update` no longer writes back an item that was deleted after it was read, and checks the old item's version when it changes keys. Queries fail with `ItemDecodeError` on an item tagged with a requested model that doesn't decode, where they used to skip it. `loadMany` fails as a whole when a key is missing, where it used to return an `Error` in that key's place; load keys one by one and catch `ItemNotFoundError` to keep the others. Loads issued in the same tick still share BatchGetItem calls, also across separate `runPromise` calls such as GraphQL resolvers, as long as they run on one `DynamoDB` layer.

  `updateRaw` now increments `_docVersion`, so an instance read before a raw update fails its next `update` with `RaceConditionError` instead of overwriting the change, and a condition passed to `updateRaw` is added to its existence check instead of replacing it. Bulk rollbacks only delete an item they created while it still has the version they wrote, and a `put` that may overwrite (`ignoreExistence` or a custom condition) has no rollback. Instances written by `bulk` carry their version like those written by `put`. Pagination cursors are built from the keys an item is stored with, a cursor key must be 32 bytes, and an event whose `encode()` is not a JSON object fails with `EventEncodeError` before anything is published.

### Patch Changes

- Updated dependencies [bb6d84e]
  - @model-ts/core@7.0.0

## 3.0.0

### Major Changes

- a4f8401: Move to AWS SDK v3

## 2.0.1

### Patch Changes

- 4653939: Move io-ts and fp-ts from peer dependencies to package dependencies and expose io-ts plus selected io-ts-types helpers through `t` from @model-ts/core. Adds `t.withValidation` for simple predicate-based validation.
- Updated dependencies [4653939]
  - @model-ts/core@0.4.0

## 2.0.0

### Patch Changes

- Updated dependencies [5d9cb44]
  - @model-ts/core@0.3.0

## 1.0.1

### Patch Changes

- 40f26df: loosen publish type

## 1.0.0

### Patch Changes

- Updated dependencies [1271431]
  - @model-ts/core@0.2.0

## 0.2.2

### Patch Changes

- accb836: call event bus stub with individual events

## 0.2.1

### Patch Changes

- dfb123b: stringify eventbridge events

## 0.2.0

### Minor Changes

- 819d457: add proxy client
