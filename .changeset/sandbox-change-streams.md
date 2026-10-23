---
"@model-ts/dynamodb": minor
---

Emit DynamoDB Streams records from both sandboxes. `yield* sandbox.changes` subscribes to the table's stream and returns the records of every write committed afterwards, in commit order and shaped as AWS Lambda receives them (`NEW_AND_OLD_IMAGES`, wire JSON attribute values). Writes that change nothing and cancelled transactions emit no records. The memory sandbox produces records itself; `Sandbox.layerLocal()` creates its table with a stream and reads it through the new optional peer dependency `@aws-sdk/client-dynamodb-streams`. The record types live in `@model-ts/dynamodb/Stream`.
