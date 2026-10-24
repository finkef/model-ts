---
"@model-ts/graphql": minor
---

Let the JSON scalar declare `@specifiedBy`. `jsonScalar({ specifiedByURL, description })` is exported and returns the built-in JSON scalar with that metadata. Registering `"JSON"` with an annotated `JSONValue`, as in `GraphQL.scalar("JSON", JSONValue.annotate({ specifiedByURL }))`, now keeps the built-in JSON parsing (enum literals rejected at any depth, the ECMA-404 description by default) and takes the annotations' metadata. `Register` infers a schema of unknown values, such as `JSONValue`, as a scalar.
