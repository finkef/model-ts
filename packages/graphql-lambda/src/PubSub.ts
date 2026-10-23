import type { Schema } from "effect"
import { Effect, Stream } from "effect"
import { badRequest } from "./Errors.ts"
import { type Topic } from "./Identifiers.ts"
import { createPublishedEvent } from "./internal/publication.ts"

/** Supplied to the context factory for this operation, never shared between requests. */
export interface PubSub {
  readonly subscribe: (
    topics: Topic | ReadonlyArray<Topic>,
  ) => AsyncIterableIterator<Schema.Json>
  /** Effect-native source for builders whose subscription resolvers return Streams. */
  readonly stream: (
    topics: Topic | ReadonlyArray<Topic>,
  ) => Stream.Stream<Schema.Json>
}

/** Persist an event for asynchronous delivery by the DynamoDB stream processor. */
export const publish = Effect.fn("GraphQL.publish")(function* (
  topic: Topic,
  payload: unknown,
  ttlSeconds = 7200,
) {
  const event = yield* createPublishedEvent(topic, payload, ttlSeconds)
  return yield* event.put()
})

/** Strict-true filtering, with iterator cancellation forwarded on completion/error. */
export async function* withFilter<A>(
  source: AsyncIterable<A>,
  predicate: (payload: A) => boolean | Promise<boolean>,
): AsyncGenerator<A> {
  for await (const payload of source)
    if ((await predicate(payload)) === true) yield payload
}

/** During registration only topics are collected; delivery has one finite event. */
export const operationPubSub = (event?: {
  readonly topic: Topic
  readonly payload: Schema.Json
}) => {
  const topics = new Set<Topic>()
  const select = (names: Topic | ReadonlyArray<Topic>) => {
    const selected = typeof names === "string" ? [names] : names
    if (selected.length === 0 || selected.some((name) => name.length === 0))
      throw badRequest("A subscription topic is required")
    for (const name of selected) topics.add(name)
    return selected
  }
  const pubSub: PubSub = {
    stream(names) {
      const selected = select(names)
      return event && selected.includes(event.topic)
        ? Stream.succeed(event.payload)
        : Stream.empty
    },
    subscribe(names) {
      const selected = select(names)
      return (async function* () {
        if (event && selected.includes(event.topic)) yield event.payload
      })()
    },
  }
  return { pubSub, topics }
}
