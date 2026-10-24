# @model-ts/graphql

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
