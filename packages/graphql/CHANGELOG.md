# @model-ts/graphql

## 7.6.0

### Minor Changes

- b22426e: Add `GraphQL.optional`, `GraphQL.optional.nullable` and `GraphQL.annotate` for arguments and input class fields, and remove `GraphQL.arg`, `GraphQL.nullable.arg` and `GraphQL.default` (`defaulted`).

  - `optional(S, options?)` is `Schema.optionalKey(S)`: it may be omitted, and an explicit `null` fails decoding.
  - `optional.nullable(S, options?)` is `Schema.optionalKey(Schema.NullOr(S))`: it may be omitted or `null`, and decoding keeps the two apart.
  - Both take `{ description?, deprecationReason?, default? }`. A `default` is a decoded value, validated and encoded with the schema, and makes the decoded key required. It only replaces an omitted value.
  - `annotate(S, { description })` describes a required argument or input field without changing its type.

  When an `optional` value is sent as `null`, the `UserInputError` message, or the `BAD_USER_INPUT` message when the field declares no `UserInputError`, is now `"<field> cannot be null."`, using the same path as `field`.

  Migrating:

  - `GraphQL.arg(Schema.optionalKey(S), options)` becomes `GraphQL.optional(S, options)`.
  - `GraphQL.arg(S, { description })` becomes `GraphQL.annotate(S, { description })`.
  - `GraphQL.nullable.arg(S, options)` becomes `GraphQL.optional.nullable(S, options)`.
  - `GraphQL.default(S, value)` becomes `GraphQL.optional(S, { default: value })`. Its GraphQL type is now nullable (`Int = 1` instead of `Int! = 1`), and a `null` reports `"<field> cannot be null."`.

## 7.5.0

### Minor Changes

- 75b9da9: Add `GraphQL.arg(schema, { description, deprecationReason })` (also exported as `arg`) to document an argument or input field without changing its type. It sets Effect key annotations, so it works on the fields of input classes and composes with `Schema.optionalKey` and `GraphQL.default`.
- 75b9da9: Add `GraphQL.nullable.arg(schema, options?)` for nullable arguments and input fields: `Schema.optionalKey(Schema.NullOr(schema))`, omittable or `null` like a nullable GraphQL argument, with the same `{ description, deprecationReason }` as `GraphQL.arg`. The error for an explicit `null` sent to an optional-only input now suggests it.

### Patch Changes

- 79a2467: Stop copying a named input type's description onto every argument and input field of that type. The description now appears only on the `input` definition; describe an argument or field with `annotateKey({ description })`. An input type described differently at two uses (for example `Input.annotate({ description })` at one argument) now fails to assemble instead of taking whichever description was assembled first. Key annotations no longer change a named input type's own description.

## 7.4.0

### Minor Changes

- 6032bfa: Let the JSON scalar declare `@specifiedBy`. `jsonScalar({ specifiedByURL, description })` is exported and returns the built-in JSON scalar with that metadata. Registering `"JSON"` with an annotated `JSONValue`, as in `GraphQL.scalar("JSON", JSONValue.annotate({ specifiedByURL }))`, now keeps the built-in JSON parsing (enum literals rejected at any depth, the ECMA-404 description by default) and takes the annotations' metadata. `Register` infers a schema of unknown values, such as `JSONValue`, as a scalar.

## 7.3.0

### Minor Changes

- ce187d1: Add `where` to models, unions and `DynamoDB`: a pipe step that keeps a loaded item only when a predicate holds. After `load`, a rejected item fails with `ItemNotFoundError` for its key; after `loadOption` or any read of an `Option`, it becomes `Option.none()`.

  Add `publicMessages` to `withErrorPolicy`: a public error with a fixed message reaches clients as only that message and its code, without its own message or declared fields.

## 7.2.0

## 7.1.0

## 7.0.0

### Major Changes

- dec9d05: Add @model-ts/graphql to the coordinated model-ts 7 release. Build Effect-native GraphQL APIs from registered model and Schema classes, with plain-object field definitions and no server resolver type generation. Field helpers live on the builder next to the definitions (`GraphQL.string()`, `GraphQL.field(...)`, `GraphQL.connection(...)`); `GraphQL.id()` is the ID output field and `GraphQL.id(schema)` a branded ID argument. Preserve typed request context and collect resolver, authorization, subscription, and connection service requirements in the built schema.

  Support mapped enums, inherited interface fields, tagged and explicitly named unions, nullable and multi-success typed result unions, Schema-decoded inputs with branded IDs and explicit null handling, and custom or adopted scalars. Derive connections over objects, interfaces, and unions; preserve storage pagination defaults and opaque cursors, and resolve total counts only when selected.

  Execute operations with Effect layers, classify public and internal failures, and report original causes through ErrorReporter. Require graphql-js `^16.11.0`. Test operations and snapshots through @model-ts/graphql/Testing with memory or DynamoDB Local layers, using `Testing.client` to fill a default request context per call and the `graphql` tag to normalize documents with `stripIgnoredCharacters`. Include a user guide covering setup, API concepts, testing, and Promise interoperability.

  Return JSON-safe formatted query and subscription results from the executor while retaining the direct execution result API. Apply public/internal error policy to custom and adopted scalar callbacks, clone adopted scalars without changing their behavior or metadata, and report unexpected coercion and serialization failures. Preserve original error class instances for registered result fields, and interrupt subscription event work with awaited finalization when consumption is cancelled.

  Declare executor-owned schema validation so transports can preserve scalar input error policy without validating the same document twice.

  Validate subscriptions by collecting root response keys through fragments. Accept merged single-root selections, reject multiple keys and root introspection, and reject root `@skip`/`@include` without evaluating variables while preserving nested conditional selections.

  Define subscription fields with `GraphQL.subscriptionField(name, type, { subscribe, ... })`, where `subscribe` returns an Effect Stream. Reject `subscribe` on object, interface, query, and mutation fields. Remove the unreleased `GraphQL.events` helper. Preserve per-value authorization, decoded arguments, service requirements, error policy, cancellation and finalizers, subscription-root validation, and the `GraphQL.subscribe` execution API.

  Define roots with `GraphQL.queryField(name, type, config)`, `GraphQL.mutationField(name, type, config)`, and `GraphQL.subscriptionField(name, type, config)`. Accept registered or scalar names and config-less output helpers, including nullable/list combinations and connections. Reuse object field config and resolver shorthand typing, result unions, authorization, pagination options, and service requirements. Require an explicit resolver or subscription config at the root; remove the unreleased root field-map and nested `GraphQL.field(...)` forms. Export one constant per root field through the schema barrel; assembly merges fields by root and rejects duplicate names with the root and field in the diagnostic. Retain request context and schema completeness checking.
