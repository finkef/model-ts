---
"@model-ts/eventbridge": major
---

Add the optional `@model-ts/eventbridge/effect` entry point with
`makeEffectClient`, `getEffectProvider`, `EventBridgeClientError`, and the
`EventBridge` Effect service/layers. `PublishError` now has a `_tag` of
`"PublishError"` for `Effect.catchTag`.

This release requires `@model-ts/core` `^0.5.0`, TypeScript 5.9 or later, and
io-ts 2.2.22 or later. The Effect provider replaces model `publish()` with an
Effect-returning method; do not mix it with the Promise provider on one model
class. The existing Promise API is unchanged.
