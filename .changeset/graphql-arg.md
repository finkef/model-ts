---
"@model-ts/graphql": minor
---

Add `GraphQL.arg(schema, { description, deprecationReason })` (also exported as `arg`) to document an argument or input field without changing its type. It sets Effect key annotations, so it works on the fields of input classes and composes with `Schema.optionalKey` and `GraphQL.default`.
