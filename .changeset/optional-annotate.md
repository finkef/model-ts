---
"@model-ts/graphql": minor
---

Add `GraphQL.optional`, `GraphQL.optional.nullable` and `GraphQL.annotate` for arguments and input class fields, and remove `GraphQL.arg`, `GraphQL.nullable.arg` and `GraphQL.default` (`defaulted`).

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
