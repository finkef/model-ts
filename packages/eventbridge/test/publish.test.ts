import { assert, describe, expect } from "@effect/vitest"
import { Config, ConfigProvider, Effect, Layer, Option } from "effect"
import {
  EventBridge,
  EventBridgeError,
  EventEncodeError,
  publish,
  PublishedEvents,
  PublishError,
} from "../src/index.ts"
import { FakeBus } from "./fake-eventbridge.ts"
import { PostPublished, postPublished, unencodable } from "./fixtures.ts"
import { recordSpans, testEffect } from "./utils.ts"

describe("EventBridge.layerMemory", () => {
  const it = testEffect(EventBridge.layerMemory)

  it("records published events with their encoded detail", () =>
    Effect.gen(function* () {
      yield* postPublished(1).publish()
      yield* publish(postPublished(2), postPublished(3))

      const published = yield* PublishedEvents.use((events) => events.all)
      expect(published).toEqual(
        [1, 2, 3].map((n) => ({
          eventBusName: "memory",
          source: "blog",
          detailType: "blog.post.published",
          detail: postPublished(n).encode(),
        })),
      )
      expect(PostPublished.from(published[0]!.detail)).toEqual(postPublished(1))
    }))

  it("fails like the real bus when an event cannot be encoded", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(publish(postPublished(1), unencodable()))
      expect(error).toBeInstanceOf(EventEncodeError)
      expect(yield* PublishedEvents.use((events) => events.all)).toEqual([])
    }))
})

describe("EventBridge.layer", () => {
  const it = testEffect(FakeBus.layer)

  it("sends PutEvents calls of at most 10 entries, in order", () =>
    Effect.gen(function* () {
      const events = Array.from({ length: 23 }, (_, i) => postPublished(i))
      const results = yield* publish(...events)
      const fake = yield* FakeBus

      expect(fake.requests.map((request) => request.Entries.length)).toEqual([
        10, 10, 3,
      ])
      expect(fake.requests.flatMap((request) => request.Entries)).toEqual(
        events.map((event) => ({
          EventBusName: "test-bus",
          Source: "blog",
          DetailType: "blog.post.published",
          Detail: JSON.stringify(event.encode()),
        })),
      )
      expect(results).toHaveLength(23)
      expect(results[22]).toEqual({ EventId: "evt-23" })
    }))

  it("fails with PublishError when entries are rejected", () =>
    Effect.gen(function* () {
      const fake = yield* FakeBus
      fake.rejectDetailTypes.add("blog.post.published")

      const { spans, value: error } = yield* recordSpans(
        Effect.flip(postPublished(1).publish()),
      )
      expect(error).toBeInstanceOf(PublishError)
      expect(error).toMatchObject({
        _tag: "PublishError",
        failedEntryCount: 1,
        entries: [
          { ErrorCode: "InternalFailure", ErrorMessage: "rejected by fake" },
        ],
        message: "EventBridge rejected 1 of 1 events",
      })
      // The call that was rejected fails its own span.
      const putEvents = spans.find(
        (span) => span.name === "EventBridge.PutEvents",
      )!
      expect(
        putEvents.status._tag === "Ended" && putEvents.status.exit._tag,
      ).toBe("Failure")
    }))

  it("sends every batch and reports every result when some are rejected", () =>
    Effect.gen(function* () {
      const fake = yield* FakeBus
      fake.rejectDetailTypes.add("blog.post.published")
      const events = Array.from({ length: 12 }, (_, i) => postPublished(i))

      const error = yield* Effect.flip(publish(...events))
      expect(fake.requests).toHaveLength(2)
      assert.instanceOf(error, PublishError)
      expect(error.failedEntryCount).toBe(12)
      expect(error.entries).toHaveLength(12)
    }))

  it("sends nothing when one event cannot be encoded", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(publish(postPublished(1), unencodable()))
      expect(error).toBeInstanceOf(EventEncodeError)
      expect(error).toMatchObject({ detailType: "blog.post.published" })
      expect((yield* FakeBus).requests).toEqual([])
    }))

  it("fails with EventBridgeError when the call fails", () =>
    Effect.gen(function* () {
      const fake = yield* FakeBus
      fake.failWithStatus = 400

      const error = yield* Effect.flip(postPublished(1).publish())
      expect(error).toBeInstanceOf(EventBridgeError)
      expect(error.message).toBe(
        "EventBridge PutEvents failed: fake access denied",
      )
    }))

  it("traces each PutEvents call as a producer span inside the publish span", () =>
    Effect.gen(function* () {
      const events = Array.from({ length: 12 }, (_, i) => postPublished(i))
      const { spans } = yield* recordSpans(publish(...events))

      expect(spans.map((span) => span.name)).toEqual([
        "EventBridge.publish",
        "EventBridge.PutEvents",
        "EventBridge.PutEvents",
      ])
      const [parent, first, second] = spans
      expect(Object.fromEntries(parent!.attributes)).toEqual({
        "messaging.batch.message_count": 12,
      })
      for (const child of [first!, second!]) {
        expect(child.kind).toBe("producer")
        expect(Option.getOrUndefined(child.parent)?.spanId).toBe(parent!.spanId)
      }
      expect(Object.fromEntries(first!.attributes)).toEqual({
        "rpc.system": "aws-api",
        "rpc.service": "EventBridge",
        "rpc.method": "PutEvents",
        "messaging.system": "aws.eventbridge",
        "messaging.operation.name": "PutEvents",
        "messaging.destination.name": "test-bus",
        "messaging.batch.message_count": 10,
        "model_ts.detail_types": ["blog.post.published"],
      })
      expect(second!.attributes.get("messaging.batch.message_count")).toBe(2)
    }))
})

describe("EventBridge.layerConfig", () => {
  const it = testEffect(
    EventBridge.layerConfig(
      Config.all({ eventBusName: Config.String("EVENT_BUS_NAME") }),
    ).pipe(
      Layer.provide(
        ConfigProvider.layer(
          ConfigProvider.fromUnknown({ EVENT_BUS_NAME: "orders" }),
        ),
      ),
    ),
  )

  it("reads its options from Config", () =>
    Effect.gen(function* () {
      expect((yield* EventBridge).eventBusName).toBe("orders")
    }))
})
