import { marshall } from "@aws-sdk/util-dynamodb"
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
  ConnectionRejected,
  TransportError,
  createHttpHandler,
  createWebSocketHandler,
  graphqlExecutor,
  type ContextRequest,
  type HttpEvent,
  PubSub,
  PublishedEvent,
  type StreamEvent,
  type WebSocketEvent,
} from "../src/index.ts"
import { header } from "../src/internal/request.ts"
import { JsonObject } from "../src/Models.ts"
import type { OperationRequest } from "../src/Models.ts"
import { withFilter } from "../src/PubSub.ts"
import { Sandbox } from "../src/PubSub/DynamoDB.ts"
import { streamHandler } from "../src/PubSub/DynamoDB.ts"
import { Storage } from "../src/Storage.ts"

declare module "vitest" {
  export interface ProvidedContext {
    readonly graphqlDynamoDB: "memory" | "local"
  }
}
export const testLayer = () =>
  Layer.merge(
    inject("graphqlDynamoDB") === "local"
      ? PubSub.DynamoDB.layerLocal()
      : PubSub.DynamoDB.layerMemory(),
    Gateway.layerMemory,
  )

export const loadConnection = (id: ConnectionId) =>
  Storage.use((storage) => storage.liveConnection(id))
export const loadOperation = (id: ConnectionId, operationId: OperationId) =>
  Storage.use((storage) => storage.operation(id, operationId)).pipe(
    Effect.flatMap((operation) =>
      operation
        ? Effect.succeed(operation)
        : Effect.die(new Error("Expected an operation")),
    ),
  )

export const publish = Effect.fn(function* (
  topic: Topic,
  payload: Schema.Json,
  ttlSeconds = 7200,
) {
  const id = yield* PubSub.publish(topic, payload, { ttlSeconds })
  return yield* (yield* Sandbox)
    .get({ PK: `GRAPHQL#EVENT#${id}`, SK: "EVENT" })
    .pipe(Effect.flatMap(Schema.decodeUnknownEffect(PublishedEvent)))
})

export interface AppContext {
  readonly pubSub: PubSub.Source
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
  persistedContext: JsonObject,
  executor: graphqlExecutor<AppContext>(schema),
  context,
  onInit: (payload: { readonly [key: string]: Schema.Json }) =>
    payload.token === "valid"
      ? Effect.succeed({ user: "ada", allowed: true })
      : Effect.fail(new ConnectionRejected({})),
}
export const http = createHttpHandler({
  ...options,
  cors: { origin: ["https://app.example"], credentials: true },
})
export const websocket = createWebSocketHandler(options)
export const stream = streamHandler(options)
export const httpEvent = (
  query: unknown,
  overrides: Omit<Partial<HttpEvent>, "requestContext"> & {
    readonly requestContext?: {
      readonly requestId?: string
      readonly http?: Partial<HttpEvent["requestContext"]["http"]>
    }
  } = {},
): HttpEvent => ({
  version: "2.0",
  routeKey: "$default",
  rawPath: "/graphql",
  rawQueryString: "",
  headers: { "content-type": "application/json" },
  body: JSON.stringify(query),
  isBase64Encoded: false,
  ...overrides,
  requestContext: {
    requestId: "http-test",
    ...overrides.requestContext,
    http: {
      method: "POST",
      path: overrides.rawPath ?? "/graphql",
      sourceIp: "127.0.0.1",
      userAgent: "test-client",
      ...overrides.requestContext?.http,
    },
  },
})
export const wsEvent = (
  id: string,
  routeKey: string,
  message?: unknown,
): WebSocketEvent => ({
  headers: { "sEc-WeBsOcKeT-PrOtOcOl": "other, graphql-transport-ws" },
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
      type: "subscribe",
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
