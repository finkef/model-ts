---
"@model-ts/graphql": minor
---

Add `GraphQL.nullable.arg(schema, options?)` for nullable arguments and input fields: `Schema.optionalKey(Schema.NullOr(schema))`, omittable or `null` like a nullable GraphQL argument, with the same `{ description, deprecationReason }` as `GraphQL.arg`. The error for an explicit `null` sent to an optional-only input now suggests it.
