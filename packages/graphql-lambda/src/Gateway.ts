import {
  ApiGatewayManagementApiClient,
  DeleteConnectionCommand,
  PostToConnectionCommand,
  type ApiGatewayManagementApiClientConfig,
} from "@aws-sdk/client-apigatewaymanagementapi"
import { Context, Effect, Layer, Predicate } from "effect"
import { GatewayError } from "./Errors.ts"
import type { ConnectionId } from "./Identifiers.ts"

export interface GatewayService {
  readonly post: (
    endpoint: string,
    connectionId: ConnectionId,
    data: string,
  ) => Effect.Effect<void, GatewayError>
  readonly close: (
    endpoint: string,
    connectionId: ConnectionId,
  ) => Effect.Effect<void, GatewayError>
}

const gone = (cause: unknown) =>
  Predicate.hasProperty(cause, "$metadata") &&
  Predicate.hasProperty(cause.$metadata, "httpStatusCode") &&
  cause.$metadata.httpStatusCode === 410

/** Clients are cached per endpoint and destroyed with the layer's scope. */
export class Gateway extends Context.Service<Gateway, GatewayService>()(
  "@model-ts/graphql-lambda/Gateway",
) {
  static readonly layer = (
    options: Omit<ApiGatewayManagementApiClientConfig, "endpoint"> = {},
  ): Layer.Layer<Gateway> =>
    Layer.effect(
      Gateway,
      Effect.gen(function* () {
        const clients = yield* Effect.acquireRelease(
          Effect.sync(() => new Map<string, ApiGatewayManagementApiClient>()),
          (clients) =>
            Effect.sync(() => {
              for (const client of clients.values()) client.destroy()
            }),
        )
        const client = (endpoint: string) => {
          let value = clients.get(endpoint)
          if (!value) {
            value = new ApiGatewayManagementApiClient({ ...options, endpoint })
            clients.set(endpoint, value)
          }
          return value
        }
        return Gateway.of({
          post: Effect.fn("ApiGatewayManagementApi.PostToConnection")(
            (endpoint, connectionId, data) =>
              Effect.tryPromise({
                try: (signal) =>
                  client(endpoint)
                    .send(
                      new PostToConnectionCommand({
                        ConnectionId: connectionId,
                        Data: Buffer.from(data),
                      }),
                      { abortSignal: signal },
                    )
                    .then(() => undefined),
                catch: (cause) =>
                  new GatewayError({
                    operation: "PostToConnection",
                    gone: gone(cause),
                    cause,
                  }),
              }),
          ),
          close: Effect.fn("ApiGatewayManagementApi.DeleteConnection")(
            (endpoint, connectionId) =>
              Effect.tryPromise({
                try: (signal) =>
                  client(endpoint)
                    .send(
                      new DeleteConnectionCommand({
                        ConnectionId: connectionId,
                      }),
                      { abortSignal: signal },
                    )
                    .then(() => undefined),
                catch: (cause) =>
                  new GatewayError({
                    operation: "DeleteConnection",
                    gone: gone(cause),
                    cause,
                  }),
              }),
          ),
        })
      }),
    )
}
