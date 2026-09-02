---
"@model-ts/core": minor
---

Add the optional `@model-ts/core/effect` entry point with lazy, typed `decode`
for models, unions, and io-ts codecs. `RuntimeTypeValidationError` now has a
`_tag` of `"RuntimeTypeValidationError"` for `Effect.catchTag`.

The package now exposes an `exports` map and supports Effect as an optional
peer dependency. Builds require TypeScript 5.9 or later and io-ts 2.2.22 or
later. Existing Promise and synchronous APIs are unchanged.
