# @model-ts/eventbridge

Publish `@model-ts/core` models as Amazon EventBridge events, now or at a
later time. Publishing is an Effect that needs an `EventBridge` service, so
the bus is chosen with a layer: the real one in production, an in-memory one
in tests. This page is the API reference; the [main README](../../README.md)
explains the concepts.

> [!NOTE]
> Version 7 is built on Effect v4. The io-ts version is **@model-ts/eventbridge 3.x**.

```sh
npm install @model-ts/core @model-ts/eventbridge effect @aws-sdk/client-eventbridge
```

- [Event models](#event-models)
- [Publishing](#publishing)
- [Deferring](#deferring)
- [Errors](#errors)
- [Tracing](#tracing)
- [Testing](#testing)

## Event models

An event is a model with the `eventBridge` provider, the `defer` provider, or
both, and `source` and `detailType` getters (or fields). Its encoded form,
`_tag` included, becomes the event's `detail`.

```ts
import { model } from "@model-ts/core"
import { defer, eventBridge } from "@model-ts/eventbridge"
import { Schema } from "effect"

class OrderPlaced extends model<OrderPlaced>()(
  "OrderPlaced",
  [eventBridge, defer],
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

| Provider      | Adds                                                                                |
| ------------- | ----------------------------------------------------------------------------------- |
| `eventBridge` | `event.publish()`: `Effect<ReadonlyArray<ResultEntry>, PublishErrors, EventBridge>` |
| `defer`       | `event.defer(timestamp)`: `Effect<void, PublishErrors, Deferrer>`                   |

Both methods need `source` and `detailType` on the model; without them,
calling them is a type error.

## Publishing

```ts
import { EventBridge, publish } from "@model-ts/eventbridge"

yield * new OrderPlaced({ orderId, total: 42 }).publish()

// Several events at once, in PutEvents calls of up to 10.
yield * publish(orderPlaced, invoiceCreated, stockReserved)
```

`publish` encodes every event before it sends anything, so an event that cannot
be encoded fails the whole call without publishing the others. Events go out
in order, in PutEvents calls of up to 10 entries. If EventBridge rejects some
entries, every call is still made, and the result is a `PublishError` whose
`entries` has one result per event, in order, so the failed ones can be
retried.

Provide the bus with a layer:

| Layer                                          | Bus                                                             |
| ---------------------------------------------- | --------------------------------------------------------------- |
| `EventBridge.layer({ eventBusName, client? })` | EventBridge; `client` is passed to `new EventBridgeClient(...)` |
| `EventBridge.layerConfig(config)`              | The same, with options read from `Config` when it is built      |
| `EventBridge.layerMemory`                      | In memory; also provides `PublishedEvents`                      |

```ts
const EventBridgeLive = EventBridge.layerConfig(
  Config.all({ eventBusName: Config.String("EVENT_BUS_NAME") }),
)
```

The client is created when the layer is built and destroyed when its scope
closes. Anything with `source`, `detailType` and `encode()` can be published,
models or not: that shape is `PublishableEvent`.

## Deferring

`event.defer(timestamp)` asks the `Deferrer` service to publish the event at
`timestamp`. How that happens depends on your infrastructure, so the layer
decides:

| Layer                                 | Does                                                                                    |
| ------------------------------------- | --------------------------------------------------------------------------------------- |
| `Deferrer.layerEventBridge(envelope)` | Publishes `envelope(deferred)` now, for a scheduler to pick up. Requires `EventBridge`. |
| `Deferrer.layerMemory`                | Records deferred events; also provides `DeferredEvents`.                                |

`envelope` receives a `DeferredEvent`, `{ timestamp, event: { source, detailType, detail } }`,
and returns the event to publish, usually a model of your own that a scheduler
(EventBridge Scheduler, a Step Functions wait, a DynamoDB TTL item) consumes:

```ts
class DeferredEnvelope extends model<DeferredEnvelope>()("DeferredEnvelope", {
  timestamp: Schema.DateFromString,
  event: Schema.Struct({
    source: Schema.String,
    detailType: Schema.String,
    detail: Schema.Unknown,
  }),
}) {
  get source() {
    return "scheduler"
  }
  get detailType() {
    return "scheduler.deferred"
  }
}

const DeferrerLive = Deferrer.layerEventBridge(
  (deferred) => new DeferredEnvelope(deferred),
).pipe(Layer.provide(EventBridgeLive))

yield * reminder.defer(new Date(Date.now() + 24 * 60 * 60 * 1000))
```

For a different mechanism, implement the service yourself:
`Layer.succeed(Deferrer, Deferrer.of({ defer: (event, timestamp) => ... }))`.

## Errors

| Error              | Fields                        | Raised when                                                                          |
| ------------------ | ----------------------------- | ------------------------------------------------------------------------------------ |
| `EventEncodeError` | `detailType`, `cause`         | An event could not be encoded, or a deferral timestamp is invalid. Nothing was sent. |
| `PublishError`     | `failedEntryCount`, `entries` | EventBridge rejected some entries.                                                   |
| `EventBridgeError` | `cause`                       | The PutEvents call failed: network, permissions, throttling.                         |

Their union is `PublishErrors`.

## Tracing

| Span                    | Kind     | Attributes                                                                                                                                                |
| ----------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `EventBridge.publish`   | internal | `messaging.batch.message_count`                                                                                                                           |
| `EventBridge.PutEvents` | producer | `messaging.system`, `messaging.operation.name`, `messaging.destination.name` (the bus), `messaging.batch.message_count`, `rpc.*`, `model_ts.detail_types` |
| `Deferrer.defer`        | internal | `model_ts.detail_type`, `model_ts.deferred_until`                                                                                                         |

One `EventBridge.PutEvents` span is created per call, inside the
`EventBridge.publish` span. A call whose entries were rejected ends its span
with the `PublishError` for that call.

## Testing

Provide the in-memory layers and read what was published or deferred:

```ts
import { expect, layer } from "@effect/vitest"
import {
  Deferrer,
  DeferredEvents,
  EventBridge,
  PublishedEvents,
} from "@model-ts/eventbridge"

layer(Layer.merge(EventBridge.layerMemory, Deferrer.layerMemory))((it) => {
  it.effect("announces the order and schedules a reminder", () =>
    Effect.gen(function* () {
      yield* placeOrder(order)

      const published = yield* PublishedEvents.use((events) => events.all)
      expect(published.map((event) => event.detailType)).toEqual([
        "shop.order.placed",
      ])

      const deferred = yield* DeferredEvents.use((events) => events.all)
      expect(deferred[0]?.event.detailType).toBe("shop.order.reminder")
    }),
  )
})
```

`EventBridge.layerMemory` encodes and batches events exactly like the real
bus, so encoding failures show up in tests. Each recorded event has
`eventBusName: "memory"` and the decoded `detail`, which you can turn back into
a model with `OrderPlaced.from(event.detail)`.
