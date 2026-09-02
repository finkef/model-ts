# @model-ts/core

Extensible model framework for [io-ts](https://github.com/gcanti/io-ts).
See the [root guide](https://github.com/finkef/model-ts) for models, unions,
and providers.

## Effect v4

`@model-ts/core/effect` provides a lazy, typed decoder without changing the
existing synchronous core APIs.

### Install and requirements

```sh
npm install @model-ts/core effect@4.0.0-rc.112
```

The Effect entry point requires TypeScript 5.9 or later with `strict: true`
and `moduleResolution` set to `node16`, `nodenext`, or `bundler`. CommonJS
consumers need Node.js 20.19 or later (or Node.js 22.12 or later) because
Effect is ESM-only.

`effect` is an optional peer dependency: applications that only import
`@model-ts/core` do not need to install it.

### Decode models, unions, and codecs

`decode` accepts a model class, a `union`, or any io-ts codec. It does not
decode until the returned Effect is run. Invalid values fail with the typed
`RuntimeTypeValidationError`, whose `_tag` is
`"RuntimeTypeValidationError"` and whose `errors` property contains the io-ts
validation errors.

```ts
import * as Effect from "effect/Effect"
import { decode } from "@model-ts/core/effect"
import { model, t } from "@model-ts/core"

class User extends model("User", t.type({ id: t.string })) {}

const user = await Effect.runPromise(decode(User, { id: "user-1" }))

const userOrUndefined = await Effect.runPromise(
  decode(User, input).pipe(
    Effect.catchTag("RuntimeTypeValidationError", (error) => {
      console.error(error.errors)
      return Effect.succeed(undefined)
    })
  )
)
```

### What is wrapped

| Effect API                                                                                                          | Behavior                                                                                     |
| ------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `decode(modelOrUnionOrCodec, value)`                                                                                | Lazy decode that succeeds with the decoded value or fails with `RuntimeTypeValidationError`. |
| `Model.from`, `new Model(...)`, `encode`, `encodeProp`, `values`, `is`, `asDecoder`, `asEncoder`, `pipe`, and `t.*` | Existing synchronous APIs; unchanged and not wrapped.                                        |

The Effect module is deliberately separate from the root entry point. Continue
to import normal model and io-ts utilities from `@model-ts/core`.
