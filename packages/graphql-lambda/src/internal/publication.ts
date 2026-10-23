import { randomUUID } from "node:crypto"
import { Clock, Effect } from "effect"
import { badRequest } from "../Errors.ts"
import { EventId, type Topic } from "../Identifiers.ts"
import { PublishedEvent } from "../Models.ts"

export const createPublishedEvent = Effect.fn("GraphQL.serializeEvent")(
  function* (topic: Topic, payload: unknown, ttlSeconds = 7200) {
    if (!topic || !Number.isFinite(ttlSeconds) || ttlSeconds <= 0)
      return yield* badRequest("A topic and positive TTL are required")
    const serialized = yield* Effect.try({
      try: () => {
        const value = JSON.stringify(payload)
        if (value === undefined)
          throw new TypeError("Payload is not JSON serializable")
        return value
      },
      catch: () => badRequest("Payload is not JSON serializable"),
    })
    const now = yield* Clock.currentTimeMillis
    return new PublishedEvent({
      id: EventId.make(randomUUID()),
      topic,
      payload: serialized,
      ttl: Math.floor(now / 1000) + ttlSeconds,
    })
  },
)
