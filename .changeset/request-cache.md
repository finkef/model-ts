---
"@model-ts/dynamodb": minor
---

Add an opt-in request cache. Wrapping a request in `DynamoDB.withRequestCache` makes `load` and `loadMany` return items the request already loaded or wrote, wait for a read of the same key that is in flight, and send only the keys the cache doesn't know through the batched read. Successful writes, including bulk transactions and their rollbacks, record what they stored as a read would return it. A failed write, or writes of one key that ran at the same time, make the cache forget the key, and `consistentRead: true` always reads the table. `RequestCache.make` creates a cache to share between the effects of one request, such as GraphQL resolvers, and `RequestCache.invalidate(...keys)` and `RequestCache.clear` forget keys written outside model-ts. Loads of the same key in one batch now each get their own copy of the item.

Options that belong to model-ts are now camelCase, and parameters passed through to DynamoDB keep the AWS casing: `FetchAllPages` is now `fetchAllPages` and `IgnoreExistence` is now `ignoreExistence`.
