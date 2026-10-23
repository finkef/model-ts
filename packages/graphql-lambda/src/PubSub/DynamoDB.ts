import { DynamoDB, type DynamoDBOptions } from "@model-ts/dynamodb"
import { Effect, Layer } from "effect"
import { StorageError } from "../Errors.ts"
import { createPublishedEvent } from "../internal/publication.ts"
import { storageLayer } from "../internal/storage.ts"
import type { Storage } from "../Storage.ts"
import { Publisher } from "./Publisher.ts"

export * from "../internal/delivery.ts"

const publisherLayer = Layer.effect(
  Publisher,
  Effect.gen(function* () {
    const database = yield* DynamoDB
    return Publisher.of({
      publish: Effect.fn("GraphQL.DynamoDB.publish")(
        function* (topic, payload, options) {
          const event = yield* createPublishedEvent(
            topic,
            payload,
            options?.ttlSeconds,
          )
          yield* event.put().pipe(
            Effect.provideService(DynamoDB, database),
            Effect.mapError(
              (cause) => new StorageError({ operation: "publish", cause }),
            ),
          )
          return event.id
        },
      ),
    })
  }),
)

/** Captures the supplied table without exporting its services. */
export const layerFrom = <A, E, R>(
  database: Layer.Layer<DynamoDB | A, E, R>,
): Layer.Layer<Publisher | Storage, E, R> =>
  Layer.merge(storageLayer, publisherLayer).pipe(Layer.provide(database))

export type Options = Pick<DynamoDBOptions, "tableName" | "client">

export const layer = (options: Options) =>
  layerFrom(DynamoDB.layer({ ...options, cursors: "plain" }))
