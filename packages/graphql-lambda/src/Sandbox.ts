/** Development and test layers. Import this subpath explicitly. */
import { DynamoDB } from "@model-ts/dynamodb"
import type { Key } from "@model-ts/dynamodb/Keys"
import {
  Sandbox as TableSandbox,
  type SandboxService as TableSandboxService,
  type SandboxOptions,
  type LocalSandboxOptions,
} from "@model-ts/dynamodb/Sandbox"
import { Context, Effect, Layer } from "effect"
import { GatewayError } from "./Errors.ts"
import { Gateway } from "./Gateway.ts"
import type { ConnectionId } from "./Identifiers.ts"
import * as DynamoDBPubSub from "./PubSub/DynamoDB.ts"

export interface SandboxService extends TableSandboxService {
  /** Removes a raw sandbox row and emits its normal REMOVE change record. */
  readonly remove: (key: Key) => Effect.Effect<void>
}

export class Sandbox extends Context.Service<Sandbox, SandboxService>()(
  "@model-ts/graphql-lambda/Sandbox",
) {
  static readonly layerMemory = (options: SandboxOptions = {}) =>
    sandboxLayer(TableSandbox.layerMemory(options))

  static readonly layerLocal = (options: LocalSandboxOptions = {}) =>
    sandboxLayer(TableSandbox.layerLocal(options))

  /** No AWS calls. Set a connection as gone to exercise production cleanup. */
  static readonly layerGateway: Layer.Layer<Gateway | GatewayMessages> =
    Layer.effectContext(
      Effect.sync(() => {
        const messages: Array<GatewayMessage> = []
        const closed: Array<ConnectionId> = []
        const missing = new Set<ConnectionId>()
        return Context.make(
          Gateway,
          Gateway.of({
            post: Effect.fn("ApiGatewayManagementApi.PostToConnection")(
              (
                endpoint,
                connectionId,
                data,
              ): Effect.Effect<void, GatewayError> =>
                missing.has(connectionId)
                  ? Effect.fail(
                      new GatewayError({
                        operation: "PostToConnection",
                        gone: true,
                        cause: new Error("Gone"),
                      }),
                    )
                  : Effect.sync(() => {
                      messages.push({ endpoint, connectionId, data })
                    }),
            ),
            close: Effect.fn("ApiGatewayManagementApi.DeleteConnection")(
              (_endpoint, connectionId) =>
                Effect.sync(() => {
                  closed.push(connectionId)
                }),
            ),
          }),
        ).pipe(
          Context.add(
            GatewayMessages,
            GatewayMessages.of({
              all: Effect.sync(() => [...messages]),
              closed: Effect.sync(() => [...closed]),
              markGone: (id) =>
                Effect.sync(() => {
                  missing.add(id)
                }),
            }),
          ),
        )
      }),
    )
}

const sandboxLayer = (database: Layer.Layer<DynamoDB | TableSandbox>) =>
  Layer.merge(
    DynamoDBPubSub.layerFrom(database),
    Layer.effect(
      Sandbox,
      Effect.gen(function* () {
        const sandbox = yield* TableSandbox
        const table = yield* DynamoDB
        return Sandbox.of({
          ...sandbox,
          remove: (key) =>
            DynamoDB.delete(key).pipe(
              Effect.provideService(DynamoDB, table),
              Effect.orDie,
            ),
        })
      }),
    ).pipe(Layer.provide(database)),
  )

export interface GatewayMessage {
  readonly endpoint: string
  readonly connectionId: ConnectionId
  readonly data: string
}
export class GatewayMessages extends Context.Service<
  GatewayMessages,
  {
    readonly all: Effect.Effect<ReadonlyArray<GatewayMessage>>
    readonly closed: Effect.Effect<ReadonlyArray<ConnectionId>>
    readonly markGone: (id: ConnectionId) => Effect.Effect<void>
  }
>()("@model-ts/graphql-lambda/GatewayMessages") {}
