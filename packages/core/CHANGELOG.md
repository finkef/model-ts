# @model-ts/core

## 0.4.2

### Patch Changes

- d1ade55: Re-export named io-ts-types codec interface types alongside curated codec values.

## 0.4.1

### Patch Changes

- 340b188: Export io-ts-types from module subpaths instead of package barrel

## 0.4.0

### Minor Changes

- 4653939: Move io-ts and fp-ts from peer dependencies to package dependencies and expose io-ts plus selected io-ts-types helpers through `t` from @model-ts/core. Adds `t.withValidation` for simple predicate-based validation.

## 0.3.0

### Minor Changes

- 5d9cb44: prettify instance logging

## 0.2.2

### Patch Changes

- 509dafe: fix losing model context in nested codecs

## 0.2.1

### Patch Changes

- 24bca88: fix mergeProviders overwriting previous providers

## 0.2.0

### Minor Changes

- 1271431: Add nested utility combinator

## 0.1.2

### Patch Changes

- fa7f244: Move snapshot-diff to dependencies

## 0.1.1

### Patch Changes

- d7cbfbe: Initial Release
