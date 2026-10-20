import { describe, expect } from "@effect/vitest"
import { Effect, Layer } from "effect"
import {
  DeferredEvents,
  Deferrer,
  EventBridge,
  EventEncodeError,
  PublishedEvents,
} from "../src/index.ts"
import { envelope, postPublished, unencodable } from "./fixtures.ts"
import { recordSpans, testEffect } from "./utils.ts"

const tomorrow = new Date("2026-01-02T00:00:00.000Z")

describe("Deferrer.layerMemory", () => {
  const it = testEffect(Deferrer.layerMemory)

  it("records the event, its encoded detail and the timestamp", () =>
    Effect.gen(function* () {
      const { spans } = yield* recordSpans(postPublished(1).defer(tomorrow))

      expect(yield* DeferredEvents.use((events) => events.all)).toEqual([
        {
          timestamp: tomorrow,
          event: {
            source: "blog",
            detailType: "blog.post.published",
            detail: postPublished(1).encode(),
          },
        },
      ])
      expect(spans.map((span) => span.name)).toEqual(["Deferrer.defer"])
      expect(Object.fromEntries(spans[0]!.attributes)).toEqual({
        "model_ts.detail_type": "blog.post.published",
        "model_ts.deferred_until": "2026-01-02T00:00:00.000Z",
      })
    }))

  it("fails with EventEncodeError and records nothing", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(unencodable().defer(tomorrow))
      expect(error).toBeInstanceOf(EventEncodeError)
      expect(yield* DeferredEvents.use((events) => events.all)).toEqual([])
    }))
})

describe("Deferrer.layerEventBridge", () => {
  const it = testEffect(
    Deferrer.layerEventBridge(envelope).pipe(
      Layer.provideMerge(EventBridge.layerMemory),
    ),
  )

  it("publishes the envelope right away", () =>
    Effect.gen(function* () {
      const { spans } = yield* recordSpans(postPublished(1).defer(tomorrow))

      expect(yield* PublishedEvents.use((events) => events.all)).toEqual([
        {
          eventBusName: "memory",
          source: "scheduler",
          detailType: "scheduler.deferred",
          detail: {
            _tag: "DeferredEnvelope",
            timestamp: "2026-01-02T00:00:00.000Z",
            event: {
              source: "blog",
              detailType: "blog.post.published",
              detail: postPublished(1).encode(),
            },
          },
        },
      ])
      expect(spans.map((span) => span.name)).toEqual([
        "Deferrer.defer",
        "EventBridge.publish",
        "EventBridge.PutEvents",
      ])
    }))
})
