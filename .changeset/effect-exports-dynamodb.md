---
"@model-ts/dynamodb": major
---

Add the optional `@model-ts/dynamodb/effect` entry point with
`makeEffectClient`, `getEffectProvider`, `DynamoDBClientError`, and the
`DynamoDB` Effect service/layers. DynamoDB domain errors now have `_tag`
discriminants for `Effect.catchTag`.

This release requires `@model-ts/core` `^0.5.0`, TypeScript 5.9 or later, and
io-ts 2.2.22 or later. The Effect provider replaces asynchronous model methods
with methods returning Effects; do not mix it with the Promise provider on one
model class. The existing Promise API is unchanged.
