# @model-ts/dynamodb

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
