# @model-ts/eventbridge

Publish `@model-ts/core` models as Amazon EventBridge events. Publishing is an
Effect that needs an `EventBridge` service, so the bus is chosen with a layer:
the real one in production, an in-memory one in tests. This page is the API
reference; the [main README](../../README.md) explains the concepts.

> [!NOTE]
> Version 7 is built on Effect v4. The io-ts version is **@model-ts/eventbridge 3.x**.

```sh
npm install @model-ts/core @model-ts/eventbridge effect @aws-sdk/client-eventbridge
```

- [Event models](#event-models)
- [Publishing](#publishing)
- [Errors](#errors)
- [Tracing](#tracing)
- [Testing](#testing)

## Event models

An event is a model with `EventBridge.provider` and `source` and `detailType`
getters (or fields). Its encoded form, `_tag` included, becomes the event's
`detail`.

```ts
import { Model } from "@model-ts/core"
import { EventBridge } from "@model-ts/eventbridge"
import { Schema } from "effect"

class OrderPlaced extends Model.Class<OrderPlaced>()(
  "OrderPlaced",
  EventBridge.provider,
  { orderId: OrderId, total: Schema.Number },
) {
  get source() {
    return "shop"
  }
  get detailType() {
    return "shop.order.placed"
  }
}
```

`EventBridge.provider` adds `event.publish()`, an
`Effect<ReadonlyArray<ResultEntry>, PublishErrors, EventBridge>`. It needs
`source` and `detailType` on the model; without them, calling it is a type
error.

## Publishing

```ts
import { EventBridge } from "@model-ts/eventbridge"

const program = Effect.gen(function* () {
  yield* new OrderPlaced({ orderId, total: 42 }).publish()

  // Several events at once, in PutEvents calls of up to 10.
  yield* EventBridge.publish(orderPlaced, invoiceCreated, stockReserved)
})
```

`EventBridge.publish` encodes every event before it sends anything, so an event that cannot
be encoded fails the whole call without publishing the others. Events go out
in order, in PutEvents calls of up to 10 entries. If EventBridge rejects some
entries, every call is still made, and the result is a `PublishError` whose
`entries` has one result per event, in order, so the failed ones can be
retried.

Provide the bus with a layer:

| Layer                                          | Bus                                                             |
| ---------------------------------------------- | --------------------------------------------------------------- |
| `EventBridge.layer({ eventBusName, client? })` | EventBridge; `client` is passed to `new EventBridgeClient(...)` |
| `EventBridge.layerMemory`                      | In memory; also provides `PublishedEvents`                      |

```ts
// The bus name from Config, read when the layer is built
const EventBridgeLive = Layer.unwrap(
  Effect.gen(function* () {
    return EventBridge.layer({
      eventBusName: yield* Config.String("EVENT_BUS_NAME"),
    })
  }),
)
```

The client is created when the layer is built and destroyed when its scope
closes. Anything with `source`, `detailType` and `encode()` can be published,
models or not: that shape is `PublishableEvent`.

## Errors

| Error              | Fields                        | Raised when                                                  |
| ------------------ | ----------------------------- | ------------------------------------------------------------ |
| `EventEncodeError` | `detailType`, `cause`         | An event could not be encoded. Nothing was sent.             |
| `PublishError`     | `failedEntryCount`, `entries` | EventBridge rejected some entries.                           |
| `EventBridgeError` | `cause`                       | The PutEvents call failed: network, permissions, throttling. |

Their union is `PublishErrors`.

## Tracing

| Span                    | Kind     | Attributes                                                                                                                                                |
| ----------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `EventBridge.publish`   | internal | `messaging.batch.message_count`                                                                                                                           |
| `EventBridge.PutEvents` | producer | `messaging.system`, `messaging.operation.name`, `messaging.destination.name` (the bus), `messaging.batch.message_count`, `rpc.*`, `model_ts.detail_types` |

One `EventBridge.PutEvents` span is created per call, inside the
`EventBridge.publish` span. A call whose entries were rejected ends its span
with the `PublishError` for that call.

## Testing

Provide the in-memory bus and read what was published:

```ts
import { expect, layer } from "@effect/vitest"
import { EventBridge, PublishedEvents } from "@model-ts/eventbridge"

layer(EventBridge.layerMemory)((it) => {
  it.effect("announces the order", () =>
    Effect.gen(function* () {
      yield* placeOrder(order)

      const published = yield* PublishedEvents.use((events) => events.all)
      expect(published.map((event) => event.detailType)).toEqual([
        "shop.order.placed",
      ])
    }),
  )
})
```

`EventBridge.layerMemory` encodes and batches events exactly like the real
bus, so encoding failures show up in tests. Each recorded event has
`eventBusName: "memory"` and the decoded `detail`, which you can turn back into
a model with `OrderPlaced.from(event.detail)`.
