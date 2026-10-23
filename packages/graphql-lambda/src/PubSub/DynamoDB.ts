import { DynamoDB, type DynamoDBOptions } from "@model-ts/dynamodb"
import {
  Sandbox as TableSandbox,
  type SandboxService,
  type SandboxOptions,
  type LocalSandboxOptions,
} from "@model-ts/dynamodb/Sandbox"
import { Context, Effect, Layer } from "effect"
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

export class Sandbox extends Context.Service<Sandbox, SandboxService>()(
  "@model-ts/graphql-lambda/PubSub/DynamoDB/Sandbox",
) {}

const sandboxLayer = (database: Layer.Layer<DynamoDB | TableSandbox>) =>
  Layer.merge(
    layerFrom(database),
    Layer.effect(Sandbox, TableSandbox).pipe(Layer.provide(database)),
  )

export const layerMemory = (options: SandboxOptions = {}) =>
  sandboxLayer(TableSandbox.layerMemory(options))

export const layerLocal = (options: LocalSandboxOptions = {}) =>
  sandboxLayer(TableSandbox.layerLocal(options))

export type Options = Pick<DynamoDBOptions, "tableName" | "client">

export const layer = (options: Options) =>
  layerFrom(DynamoDB.layer({ ...options, cursors: "plain" }))
