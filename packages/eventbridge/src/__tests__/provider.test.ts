import * as t from "io-ts"
import { model } from "@model-ts/core"
import { EventBridgeProvider, getProvider } from "../provider"
import { EventBridgeClient, PutEventsCommand } from "@aws-sdk/client-eventbridge"
import { Client } from "../client"

const EVENT_BUS_NAME = "any-event-bus"

const makeSut = (eventBridgeClient: EventBridgeClient): EventBridgeProvider => {
  const client = new Client({ eventBusName: EVENT_BUS_NAME })

  client.eventBridgeClient = eventBridgeClient

  return getProvider(client)
}

const mockEvent = (provider: EventBridgeProvider) => {
  const codec = t.type({
    foo: t.string,
    bar: t.string,
  })
  class UserCreatedEvent extends model("UserCreatedEvent", codec, provider) {
    source = "core"
    detailType = "core.user.created"
  }
  const event = new UserCreatedEvent({
    foo: "any-foo",
    bar: "any-bar",
  })
  return {
    event,
    provider,
  }
}

it("should call provider.publish()", async () => {
  const client = new EventBridgeClient({})
  client.send = jest.fn().mockResolvedValueOnce({
    Entries: [],
  })
  const provider = makeSut(client)
  provider.instanceProps.publish = jest.fn()
  const { event } = mockEvent(provider)
  await event.publish()
  expect(provider.instanceProps.publish).toHaveBeenCalled()
})

it("should call EventBridgeClient.send() with correct values", async () => {
  const client = new EventBridgeClient({})
  const { event } = mockEvent(makeSut(client))
  client.send = jest.fn().mockResolvedValueOnce({
    Entries: [],
  })
  await event.publish()
  expect(client.send).toHaveBeenCalledWith(expect.any(PutEventsCommand))
  expect((client.send as jest.Mock).mock.calls[0][0].input).toEqual({
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

it("should succeed if no results", async () => {
  const client = new EventBridgeClient({})
  client.send = jest.fn().mockResolvedValueOnce({})
  const { event } = mockEvent(makeSut(client))
  const promise = event.publish()
  await expect(promise).resolves.toEqual([])
})

it("should throw if FailedEntryCount > 0", async () => {
  const client = new EventBridgeClient({})
  client.send = jest.fn().mockResolvedValueOnce({
    FailedEntryCount: 1,
  })
  const { event } = mockEvent(makeSut(client))
  const promise = event.publish()
  await expect(promise).rejects.toThrow()
})

it("should return Entries", async () => {
  const client = new EventBridgeClient({})
  const { event } = mockEvent(makeSut(client))
  client.send = jest.fn().mockResolvedValueOnce({
    Entries: [
      {
        EventBusName: EVENT_BUS_NAME,
        Source: "core",
        DetailType: "core.user.created",
        Detail: JSON.stringify(event.encode()),
      },
    ],
  })
  const results = await event.publish()
  expect(results).toEqual([
    {
      EventBusName: EVENT_BUS_NAME,
      Source: "core",
      DetailType: "core.user.created",
      Detail: JSON.stringify(event.encode()),
    },
  ])
})
