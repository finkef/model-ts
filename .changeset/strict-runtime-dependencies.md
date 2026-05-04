---
"@model-ts/core": minor
"@model-ts/dynamodb": patch
"@model-ts/eventbridge": patch
---

Move io-ts and fp-ts from peer dependencies to package dependencies and expose io-ts plus selected io-ts-types helpers through `t` from @model-ts/core. Adds `t.withValidation` for simple predicate-based validation.
