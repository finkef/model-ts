import { marshall } from "@aws-sdk/util-dynamodb"
import { Sandbox } from "@model-ts/dynamodb/Sandbox"
import { Effect, Layer, Schema } from "effect"
import {
  GraphQLNonNull,
  GraphQLObjectType,
  GraphQLSchema,
  GraphQLString,
} from "graphql"
import { inject } from "vitest"
import {
  Topic,
  type ConnectionId,
  type OperationId,
} from "../src/Identifiers.ts"
import {
  Gateway,
  Storage,
  ConnectionRejected,
  TransportError,
  createHttpHandler,
  createWebSocketHandler,
  createStreamHandler,
  graphqlExecutor,
  withFilter,
  type ContextRequest,
  type HttpEvent,
  type PubSub,
  type PublishedEvent,
  type StreamEvent,
  type WebSocketEvent,
} from "../src/index.ts"
import { header } from "../src/internal/request.ts"
import type { OperationRequest } from "../src/Models.ts"

declare module "vitest" {
  export interface ProvidedContext {
    readonly graphqlDynamoDB: "memory" | "local"
  }
}
export const testLayer = () =>
  Storage.layer.pipe(
    Layer.provideMerge(
      Layer.merge(
        inject("graphqlDynamoDB") === "local"
          ? Sandbox.layerLocal()
          : Sandbox.layerMemory(),
        Gateway.layerMemory,
      ),
    ),
  )

export interface AppContext {
  readonly pubSub: PubSub
  readonly user: string
  readonly allowed: boolean
}
export interface LambdaContext {
  readonly user?: string
}
const Payload = Schema.Struct({
  domainId: Schema.String,
  message: Schema.String,
})
export const schema = new GraphQLSchema({
  query: new GraphQLObjectType<unknown, AppContext>({
    name: "Query",
    fields: {
      me: {
        type: GraphQLString,
        resolve: (_source, _args, context) => context.user,
      },
      echo: {
        type: GraphQLString,
        args: { text: { type: new GraphQLNonNull(GraphQLString) } },
        resolve: (_source, args: { readonly text?: unknown }) =>
          Schema.decodeUnknownSync(Schema.String)(args.text),
      },
      secret: {
        type: GraphQLString,
        resolve: () => {
          throw new Error("database password=secret")
        },
      },
      publicError: {
        type: GraphQLString,
        resolve: () => {
          throw new TransportError({
            message: "No access",
            code: "FORBIDDEN",
            status: 403,
          })
        },
      },
    },
  }),
  mutation: new GraphQLObjectType({
    name: "Mutation",
    fields: { update: { type: GraphQLString, resolve: () => "updated" } },
  }),
  subscription: new GraphQLObjectType<Schema.Json, AppContext>({
    name: "Subscription",
    fields: {
      message: {
        type: GraphQLString,
        args: { domain: { type: new GraphQLNonNull(GraphQLString) } },
        subscribe: (_source, args: { readonly domain?: unknown }, context) =>
          withFilter(
            context.pubSub.subscribe(
              ["user", "organization"].map((name) => Topic.make(name)),
            ),
            (payload) =>
              Schema.decodeUnknownSync(Payload)(payload).domainId ===
              Schema.decodeUnknownSync(Schema.String)(args.domain),
          ),
        resolve: (source, _args, context) => {
          if (!context.allowed)
            throw new TransportError({
              message: "Access revoked",
              code: "FORBIDDEN",
              status: 403,
            })
          return Schema.decodeUnknownSync(Payload)(source).message
        },
      },
    },
  }),
})
export const context = (request: ContextRequest<LambdaContext>) =>
  Effect.sync((): AppContext => {
    if ("headers" in request.event) {
      const cookie = header(request.event, "cookie")
      if (cookie)
        request.response.cookies.push(
          "session=rotated; HttpOnly",
          "refresh=rotated; Secure",
        )
    }
    return {
      pubSub: request.pubSub,
      user:
        request.lambdaContext.user ?? String(request.persisted.user ?? "guest"),
      allowed: request.persisted.allowed !== false,
    }
  })
export const options = {
  executor: graphqlExecutor<AppContext>(schema),
  context,
  onConnect: (payload: { readonly [key: string]: Schema.Json }) =>
    payload.token === "valid"
      ? Effect.succeed({ user: "ada", allowed: true })
      : Effect.fail(new ConnectionRejected({})),
}
export const http = createHttpHandler({
  ...options,
  cors: { origin: ["https://app.example"], credentials: true },
})
export const websocket = createWebSocketHandler(options)
export const stream = createStreamHandler(options)
export const httpEvent = (
  query: unknown,
  overrides: Partial<HttpEvent> = {},
): HttpEvent => ({
  httpMethod: "POST",
  path: "/v1/graphql",
  headers: { "content-type": "application/json" },
  body: JSON.stringify(query),
  requestContext: {
    requestId: "http-test",
    identity: { sourceIp: "127.0.0.1" },
  },
  ...overrides,
})
export const wsEvent = (
  id: string,
  routeKey: string,
  message?: unknown,
): WebSocketEvent => ({
  headers: { "sEc-WeBsOcKeT-PrOtOcOl": "other, graphql-ws" },
  requestContext: {
    connectionId: id,
    routeKey,
    domainName: "example.execute-api.local",
    stage: "ws",
  },
  body: message === undefined ? undefined : JSON.stringify(message),
})
export const connect = (id: string) =>
  Effect.gen(function* () {
    yield* websocket(wsEvent(id, "$connect"), {})
    yield* websocket(
      wsEvent(id, "$default", {
        type: "connection_init",
        payload: { token: "valid" },
      }),
      {},
    )
  })
export const start = (id: string, operationId = "1", domain = "a") =>
  websocket(
    wsEvent(id, "$default", {
      type: "start",
      id: operationId,
      payload: {
        query: "subscription ($domain: String!) { message(domain: $domain) }",
        variables: { domain },
      },
    }),
    {},
  )
export const streamEvent = (
  event: PublishedEvent,
  sequence = "1",
): StreamEvent => ({
  Records: [
    {
      eventName: "INSERT",
      dynamodb: {
        NewImage: marshall(event.encode()),
        SequenceNumber: sequence,
      },
    },
  ],
})

export const register = Effect.fn(function* (
  id: ConnectionId,
  operationId: OperationId,
  request: OperationRequest,
  topics: ReadonlyArray<Topic>,
  maxOperations: number,
) {
  const storage = yield* Storage
  const generation = yield* storage.reserve(
    id,
    operationId,
    request,
    maxOperations,
  )
  return yield* storage.activate(id, operationId, generation, topics)
})
