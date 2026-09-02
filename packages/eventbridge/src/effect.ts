import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type { PutEventsCommandOutput } from "@aws-sdk/client-eventbridge"
import type { ModelInstance, Provider } from "@model-ts/core"
import { Client, ClientProps } from "./client"
import { PublishError } from "./errors"

export { PublishError }

export class EventBridgeClientError extends Data.TaggedError(
  "EventBridgeClientError"
)<{ readonly cause: unknown }> {}

export type PublishEvent = ModelInstance<string, any> & {
  source: string
  detailType: string
}
export type PublishEntries = PutEventsCommandOutput["Entries"]
export type PublishEffectError = PublishError | EventBridgeClientError

export interface EffectClient {
  readonly client: Client
  readonly publish: (
    ...events: PublishEvent[]
  ) => Effect.Effect<PublishEntries, PublishEffectError>
}

export const makeEffectClient = (client: Client): EffectClient => ({
  client,
  publish: Effect.fn("@model-ts/eventbridge/publish")(
    (...events: PublishEvent[]) =>
      Effect.tryPromise({
        try: async () => client.publish(...events),
        catch: (cause): PublishEffectError =>
          cause instanceof PublishError
            ? cause
            : new EventBridgeClientError({ cause }),
      })
  ),
})

export interface EventBridgeEffectProvider extends Provider {
  instanceProps: {
    publish<T extends PublishEvent>(
      this: T
    ): Effect.Effect<PublishEntries, PublishEffectError>
  }
}

export const getEffectProvider = (
  client: Client
): EventBridgeEffectProvider => {
  const effectClient = makeEffectClient(client)
  return {
    instanceProps: {
      publish(this) {
        return effectClient.publish(this)
      },
    },
  }
}

export class EventBridge extends Context.Service<EventBridge, EffectClient>()(
  "@model-ts/eventbridge/EventBridge"
) {
  static readonly layer = (props: ClientProps) =>
    Layer.sync(EventBridge, () => makeEffectClient(new Client(props)))

  static readonly layerFromClient = (client: Client) =>
    Layer.succeed(EventBridge, makeEffectClient(client))
}
