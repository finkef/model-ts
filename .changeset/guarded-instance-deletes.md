---
"@model-ts/dynamodb": major
---

Check loaded item versions by default for hard and soft deletes, including instance
operation builders and static/client item-taking soft deletes. Stale or missing
items now reject; use `ignoreVersion: true` for administrative force deletion.
Key-only deletes remain unconditional and accept standard DynamoDB condition params.

Direct instance hard deletes report `RaceConditionError`; direct soft deletes do
so only for an unambiguous live-guard cancellation. Generic bulk errors remain
unchanged. Keep nested operation groups together across transaction boundaries and
compensation, rejecting groups over 100 before writing. Correct soft-delete tuple
declarations to the existing live Delete then archive Put order. No restore API is added.
