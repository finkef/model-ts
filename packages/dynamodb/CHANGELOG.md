# @model-ts/dynamodb

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
