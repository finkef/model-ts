import {
  EventBridgeClient,
  PutEventsCommand,
} from "@aws-sdk/client-eventbridge"
import { model } from "@model-ts/core"
import * as Effect from "effect/Effect"
import * as t from "io-ts"
import { Client } from "../client"
import {
  EventBridge,
  EventBridgeClientError,
  EventBridgeEffectProvider,
  getEffectProvider,
  makeEffectClient,
  PublishError,
} from "../effect"
import { stubEventBus } from "../stub"

const EVENT_BUS_NAME = "any-event-bus"

const makeClient = () => {
  const client = new Client({ eventBusName: EVENT_BUS_NAME })
  client.eventBridgeClient = new EventBridgeClient({})
  client.eventBridgeClient.send = jest.fn()
  return client
}

const mockEvent = (provider: EventBridgeEffectProvider) => {
  const codec = t.type({ foo: t.string, bar: t.string })
  class UserCreatedEvent extends model("UserCreatedEvent", codec, provider) {
    source = "core"
    detailType = "core.user.created"
  }

  return new UserCreatedEvent({ foo: "any-foo", bar: "any-bar" })
}

it("is lazy and sends the expected event when run", async () => {
  const client = makeClient()
  const event = mockEvent(getEffectProvider(client))
  const effect = makeEffectClient(client).publish(event)

  expect(client.eventBridgeClient.send).not.toHaveBeenCalled()
  ;(client.eventBridgeClient.send as jest.Mock).mockResolvedValueOnce({
    Entries: [],
  })
  await expect(Effect.runPromise(effect)).resolves.toEqual([])

  expect(client.eventBridgeClient.send).toHaveBeenCalledTimes(1)
  expect((client.eventBridgeClient.send as jest.Mock).mock.calls[0][0]).toEqual(
    expect.any(PutEventsCommand)
  )
  expect(
    (client.eventBridgeClient.send as jest.Mock).mock.calls[0][0].input
  ).toEqual({
    Entries: [
      {
        EventBusName: EVENT_BUS_NAME,
        Source: event.source,
        DetailType: event.detailType,
        Detail: JSON.stringify(event.encode()),
      },
    ],
  })
})

it("batches more than ten events", async () => {
  const client = makeClient()
  const event = mockEvent(getEffectProvider(client))
  ;(client.eventBridgeClient.send as jest.Mock).mockResolvedValue({
    Entries: [],
  })

  await expect(
    Effect.runPromise(
      makeEffectClient(client).publish(...Array(25).fill(event))
    )
  ).resolves.toEqual([])

  expect(client.eventBridgeClient.send).toHaveBeenCalledTimes(3)
})

it("preserves PublishError failures and their details", async () => {
  const client = makeClient()
  const event = mockEvent(getEffectProvider(client))
  ;(client.eventBridgeClient.send as jest.Mock).mockResolvedValue({
    FailedEntryCount: 1,
    Entries: [{ ErrorCode: "InternalFailure" }],
  })

  const effect = makeEffectClient(client).publish(event)
  const error = await Effect.runPromise(Effect.flip(effect))

  expect(error).toBeInstanceOf(PublishError)
  expect(error).toMatchObject({
    _tag: "PublishError",
    details: {
      failedCount: 1,
      entries: [{ ErrorCode: "InternalFailure" }],
    },
  })
  await expect(
    Effect.runPromise(
      effect.pipe(Effect.catchTag("PublishError", () => Effect.succeed([])))
    )
  ).resolves.toEqual([])
})

it("wraps unknown client failures", async () => {
  const client = makeClient()
  const error = new Error("boom")
  ;(client.eventBridgeClient.send as jest.Mock).mockRejectedValueOnce(error)

  const result = await Effect.runPromise(
    Effect.flip(
      makeEffectClient(client).publish(mockEvent(getEffectProvider(client)))
    )
  )

  expect(result).toBeInstanceOf(EventBridgeClientError)
  expect(result).toMatchObject({
    _tag: "EventBridgeClientError",
    cause: error,
  })
})

it("returns Effects from the provider and keeps stubEventBus interception", async () => {
  const client = makeClient()
  const received: unknown[] = []
  stubEventBus(client, (event) => {
    received.push(event)
    return Promise.resolve([])
  })
  const event = mockEvent(getEffectProvider(client))
  const effect = event.publish()

  expect(received).toEqual([])
  await expect(Effect.runPromise(effect)).resolves.toBeUndefined()
  expect(received).toEqual([event])
})

it("provides an Effect client through the EventBridge layer", async () => {
  const client = makeClient()
  ;(client.eventBridgeClient.send as jest.Mock).mockResolvedValueOnce({
    Entries: [],
  })
  const event = mockEvent(getEffectProvider(client))

  await expect(
    Effect.runPromise(
      Effect.gen(function* () {
        const eventBridge = yield* EventBridge
        return yield* eventBridge.publish(event)
      }).pipe(Effect.provide(EventBridge.layerFromClient(client)))
    )
  ).resolves.toEqual([])
})
