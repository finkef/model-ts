# @model-ts/dynamodb

## 7.4.0

## 7.3.0

### Minor Changes

- ce187d1: Add `where` to models, unions and `DynamoDB`: a pipe step that keeps a loaded item only when a predicate holds. After `load`, a rejected item fails with `ItemNotFoundError` for its key; after `loadOption` or any read of an `Option`, it becomes `Option.none()`.

  Add `publicMessages` to `withErrorPolicy`: a public error with a fixed message reaches clients as only that message and its code, without its own message or declared fields.

## 7.2.0

### Minor Changes

- 4770e44: Add `loadOption` to models, unions and `DynamoDB`. It loads like `load`, batched and cached, but returns `Option.none()` for a key that holds no item instead of failing with `ItemNotFoundError`.

## 7.1.0

### Minor Changes

- e4f7cac: Emit DynamoDB Streams records from both sandboxes. `yield* sandbox.changes` subscribes to the table's stream and returns the records of every write committed afterwards, in commit order and shaped as AWS Lambda receives them (`NEW_AND_OLD_IMAGES`, wire JSON attribute values). Writes that change nothing and cancelled transactions emit no records. The memory sandbox produces records itself; `Sandbox.layerLocal()` creates its table with a stream and reads it through the new optional peer dependency `@aws-sdk/client-dynamodb-streams`. The record types live in `@model-ts/dynamodb/Stream`.

## 7.0.0

### Major Changes

- bb6d84e: Rewrite model-ts on Effect v4. Models are `Schema.Class`es built with `Model.Class<Self>()(tag, providers, fields)`, DynamoDB and EventBridge are Effect services provided through layers, every operation fails with typed errors and is traced, and `effect` is a peer dependency. The io-ts versions stay available as @model-ts/core 0.4.x, @model-ts/dynamodb 6.x and @model-ts/eventbridge 3.x. See the README for the migration guide.

  The API follows Effect's naming. Core exports the `Model` and `Provider` namespaces (`Model.Class`, `Model.Union`, `Provider.make`, `Provider.merge`). The providers live on their services as `DynamoDB.provider` and `EventBridge.provider`, and layers are static `layer*` members such as `DynamoDB.layer` and `Sandbox.layerMemory`. The table operations are statics of the `DynamoDB` service (`DynamoDB.load(Post, key)`, `DynamoDB.bulk([...])`), and their error and parameter types are in the `DynamoDB` namespace (`DynamoDB.GetError`, `DynamoDB.QueryParams`). Every module can be imported on its own, as in `@model-ts/dynamodb/Sandbox`.

  Two changes affect existing tables. `GSI1` is now a regular index keyed by `GSI1PK`/`GSI1SK` instead of the inverted primary key, and queries and updates no longer skip items that carry `dynamotorLegacy`.

  Every table now chooses how pagination cursors are written: `DynamoDB.layer` requires `cursors: { encryptionKey }` (authenticated AES-256-GCM, with `previousKeys` for key rotation) or `cursors: "plain"`. Cursors encrypted by the io-ts versions are rejected, and a cursor from another query fails with `PaginationError`. Sandboxes encrypt cursors with a fixed test key, so cursors are the same on every run. `update` no longer writes back an item that was deleted after it was read, and checks the old item's version when it changes keys. Queries fail with `ItemDecodeError` on an item tagged with a requested model that doesn't decode, where they used to skip it. `loadMany` fails as a whole when a key is missing, where it used to return an `Error` in that key's place; load keys one by one and catch `ItemNotFoundError` to keep the others. Loads issued in the same tick still share BatchGetItem calls, also across separate `runPromise` calls such as GraphQL resolvers, as long as they run on one `DynamoDB` layer.

  `updateRaw` now increments `_docVersion`, so an instance read before a raw update fails its next `update` with `RaceConditionError` instead of overwriting the change, and a condition passed to `updateRaw` is added to its existence check instead of replacing it. Bulk rollbacks only delete an item they created while it still has the version they wrote, and a `put` that may overwrite (`ignoreExistence` or a custom condition) has no rollback. Instances written by `bulk` carry their version like those written by `put`. Pagination cursors are built from the keys an item is stored with, a cursor key must be 32 bytes, and an event whose `encode()` is not a JSON object fails with `EventEncodeError` before anything is published.

### Minor Changes

- 7e30f81: Add an opt-in request cache. Wrapping a request in `DynamoDB.withRequestCache` makes `load` and `loadMany` return items the request already loaded or wrote, wait for a read of the same key that is in flight, and send only the keys the cache doesn't know through the batched read. Successful writes, including bulk transactions and their rollbacks, record what they stored as a read would return it. A failed write, or writes of one key that ran at the same time, make the cache forget the key, and `consistentRead: true` always reads the table. `RequestCache.make` creates a cache to share between the effects of one request, such as GraphQL resolvers, and `RequestCache.invalidate(...keys)` and `RequestCache.clear` forget keys written outside model-ts. Loads of the same key in one batch now each get their own copy of the item.

  Options that belong to model-ts are now camelCase, and parameters passed through to DynamoDB keep the AWS casing: `FetchAllPages` is now `fetchAllPages` and `IgnoreExistence` is now `ignoreExistence`.

### Patch Changes

- Updated dependencies [bb6d84e]
  - @model-ts/core@7.0.0

## 6.0.0

### Major Changes

- b4547df: Check loaded item versions by default for hard and soft deletes, including instance
  operation builders and static/client item-taking soft deletes. Stale or missing
  items now reject; use `ignoreVersion: true` for administrative force deletion.
  Key-only deletes remain unconditional and accept standard DynamoDB condition params.

  Direct instance hard deletes report `RaceConditionError`; direct soft deletes do
  so only for an unambiguous live-guard cancellation. Generic bulk errors remain
  unchanged. Keep nested operation groups together across transaction boundaries and
  compensation, rejecting groups over 100 before writing. Correct soft-delete tuple
  declarations to the existing live Delete then archive Put order. No restore API is added.

## 5.0.0

### Major Changes

- a4f8401: Move to AWS SDK v3

## 4.3.1

### Patch Changes

- 4653939: Move io-ts and fp-ts from peer dependencies to package dependencies and expose io-ts plus selected io-ts-types helpers through `t` from @model-ts/core. Adds `t.withValidation` for simple predicate-based validation.
- Updated dependencies [4653939]
  - @model-ts/core@0.4.0

## 4.3.0

### Minor Changes

- cbe817c: Add Model.iterator()

## 4.2.2

### Patch Changes

- 88c4914: Handle list indexes in update expressions

## 4.2.1

### Patch Changes

- ee9693a: Implement list access in expressions for in-memory dynamodb

## 4.2.0

### Minor Changes

- 221e389: Add experimental in-memory DynamoDB sandbox

## 4.1.0

### Minor Changes

- 3cf37a0: Add sandbox rollback option

## 4.0.0

### Major Changes

- e49df55: Use stable, compact diff snapshots

## 3.1.0

### Minor Changes

- 4692d16: Add <model>.query() method

## 3.0.4

### Patch Changes

- 44ad3e2: Revert support for multiple local dynamodb instances due to marginal performance gains and increased complexity.

## 3.0.3

### Patch Changes

- 6163ee5: Allow passing multiple local dynamodb instances

## 3.0.2

### Patch Changes

- ab3d767: Increase transaction chunk size to 100 items
- 354faac: Improve seeding speed

## 3.0.1

### Patch Changes

- 7f272bd: Fix rest/spread operator removing computed fields

## 3.0.0

### Major Changes

- 647be0e: Add support for additional GSIs.

  Note: This is potentially breaking due to cursor sizes growing significantly, hence releasing as a major version.

### Minor Changes

- c148fb9: Allow passing custom pagination options

## 2.0.0

### Patch Changes

- Updated dependencies [5d9cb44]
  - @model-ts/core@0.3.0

## 1.3.0

### Minor Changes

- 01c64a7: replace uuid with crypto.randomBytes

## 1.2.1

### Patch Changes

- abb6c2e: fix: proxy recover flag to client implementation

## 1.2.0

### Minor Changes

- 4fd7913: add option to load soft-deleted items

## 1.1.0

### Minor Changes

- f856f4c: add option to encrypt cursors using AES-SIV

## 1.0.0

### Patch Changes

- Updated dependencies [1271431]
  - @model-ts/core@0.2.0

## 0.2.0

### Minor Changes

- e3c2d39: add GSI4, GSI5 indices

## 0.1.2

### Patch Changes

- fa7f244: Move snapshot-diff to dependencies
- Updated dependencies [fa7f244]
  - @model-ts/core@0.1.2

## 0.1.1

### Patch Changes

- d7cbfbe: Initial Release
- Updated dependencies [d7cbfbe]
  - @model-ts/core@0.1.1
