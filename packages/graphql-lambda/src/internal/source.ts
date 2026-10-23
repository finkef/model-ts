import { type Schema, Stream } from "effect"
import { badRequest } from "../Errors.ts"
import type { Topic } from "../Identifiers.ts"
import type { Source } from "../PubSub/Source.ts"

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
  const pubSub: Source = {
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
