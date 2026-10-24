---
"@model-ts/dynamodb": minor
"@model-ts/graphql": minor
---

Add `where` to models, unions and `DynamoDB`: a pipe step that keeps a loaded item only when a predicate holds. After `load`, a rejected item fails with `ItemNotFoundError` for its key; after `loadOption` or any read of an `Option`, it becomes `Option.none()`.

Add `publicMessages` to `withErrorPolicy`: a public error with a fixed message reaches clients as only that message and its code, without its own message or declared fields.
