---
"@model-ts/core": major
"@model-ts/dynamodb": major
"@model-ts/eventbridge": major
---

Rewrite model-ts on Effect v4. Models are `Schema.Class`es built with `model<Self>()(tag, providers, fields)`, DynamoDB and EventBridge are Effect services provided through layers, every operation fails with typed errors and is traced, and `effect` is a peer dependency. The io-ts versions stay available as @model-ts/core 0.4.x, @model-ts/dynamodb 6.x and @model-ts/eventbridge 3.x. See the README for the migration guide.
