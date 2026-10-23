---
"@model-ts/dynamodb": minor
---

Add an opt-in request cache. Wrapping a request in `DynamoDB.withRequestCache` makes `load` and `loadMany` read each key at most once per request: they return items the request already loaded or wrote, and send only the keys the cache doesn't know through the batched read. Successful writes, including bulk transactions and their rollbacks, record what they stored; a failed write forgets the keys it touched, and `consistentRead: true` always reads the table. `RequestCache.invalidate(key)` and `RequestCache.clear` forget keys written outside model-ts.

Options that belong to model-ts are now camelCase, and parameters passed through to DynamoDB keep the AWS casing: `FetchAllPages` is now `fetchAllPages` and `IgnoreExistence` is now `ignoreExistence`.
