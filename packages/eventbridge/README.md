# @model-ts/eventbridge

> model-ts Provider for AWS EventBridge using AWS SDK for JavaScript v3.

## Installation

```sh
npm install io-ts fp-ts @model-ts/core @model-ts/eventbridge
```

This package uses AWS SDK for JavaScript v3. Make sure that you have
`@aws-sdk/client-eventbridge` installed.

## Effect v4

`@model-ts/eventbridge/effect` is a lazy Effect facade over the existing
EventBridge client and provider. The Promise API from `@model-ts/eventbridge`
is unchanged.

### Install and requirements

```sh
npm install @model-ts/core@^0.5.0 @model-ts/eventbridge effect@4.0.0-rc.112
```

The Effect entry point requires TypeScript 5.9 or later with `strict: true`
and `moduleResolution` set to `node16`, `nodenext`, or `bundler`. CommonJS
consumers need Node.js 20.19 or later (or Node.js 22.12 or later) because
Effect is ESM-only. `effect` is an optional peer dependency, so applications
that do not import this subpath do not need it.

### Client and provider

`makeEffectClient(client).publish(...events)` returns a lazy Effect. Use
`getEffectProvider(client)` to make a model's `publish()` method return that
same Effect.

```ts
import * as Effect from "effect/Effect"
import { model, t } from "@model-ts/core"
import { Client } from "@model-ts/eventbridge"
import {
  getEffectProvider,
  makeEffectClient,
} from "@model-ts/eventbridge/effect"

const client = new Client({ eventBusName: "application-events" })
const provider = getEffectProvider(client)

class UserCreated extends model(
  "UserCreated",
  t.type({ userId: t.string }),
  provider
) {
  source = "users"
  detailType = "users.user-created"
}

const event = new UserCreated({ userId: "1" })
await Effect.runPromise(event.publish())

const entries = await Effect.runPromise(
  makeEffectClient(client)
    .publish(event)
    .pipe(Effect.catchTag("PublishError", () => Effect.succeed([])))
)
```

`getEffectProvider(client)` replaces the regular provider's asynchronous
`publish()` method. Do not put it together with `getProvider(client)` on one
model class, because both define `publish`. Use separate model classes for
Promise and Effect code, or run the Effect with `Effect.runPromise` at the
boundary.

### Layers, errors, and unchanged synchronous APIs

Use the `EventBridge` service for standalone Effect programs. Models still use
the concrete client provided when their provider is created.

```ts
import * as Effect from "effect/Effect"
import { EventBridge } from "@model-ts/eventbridge/effect"

const entries = await Effect.runPromise(
  Effect.gen(function* () {
    const eventBridge = yield* EventBridge
    return yield* eventBridge.publish(event)
  }).pipe(Effect.provide(EventBridge.layerFromClient(client)))
)
```

`EventBridge.layer(props)` creates a `Client` from `ClientProps`, while
`EventBridge.layerFromClient(client)` reuses an existing client.

`publish` is the only wrapped asynchronous API. Constructing/configuring a
`Client`, model encoding, and the `stubEventBus` test helper remain unchanged
and synchronous where they were synchronous. Failed event entries preserve
`PublishError` with `_tag === "PublishError"` (and its `details`); unknown
SDK, network, or programming failures become `EventBridgeClientError` with
`_tag === "EventBridgeClientError"` and `cause`.

## License

MIT
