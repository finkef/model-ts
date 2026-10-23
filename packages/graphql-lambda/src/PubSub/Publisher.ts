import { Context, Effect, type Schema } from "effect"
import type { StorageError, TransportError } from "../Errors.ts"
import type { EventId, Topic } from "../Identifiers.ts"

export interface PublishOptions {
  readonly ttlSeconds?: number
}

export interface PublisherService {
  readonly publish: (
    topic: Topic,
    payload: Schema.Json,
    options?: PublishOptions,
  ) => Effect.Effect<EventId, StorageError | TransportError>
}

export class Publisher extends Context.Service<Publisher, PublisherService>()(
  "@model-ts/graphql-lambda/PubSub/Publisher",
) {}

export const publish = Effect.fn("GraphQL.publish")(function* (
  topic: Topic,
  payload: Schema.Json,
  options?: PublishOptions,
) {
  return yield* (yield* Publisher).publish(topic, payload, options)
})
