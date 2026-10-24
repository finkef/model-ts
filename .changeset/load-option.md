---
"@model-ts/dynamodb": minor
---

Add `loadOption` to models, unions and `DynamoDB`. It loads like `load`, batched and cached, but returns `Option.none()` for a key that holds no item instead of failing with `ItemNotFoundError`.
