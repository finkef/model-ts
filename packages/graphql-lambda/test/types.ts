import type { DynamoDB } from "@model-ts/dynamodb"
import { Sandbox } from "@model-ts/dynamodb/Sandbox"
import { Context, Effect, Layer, Schema, Stream } from "effect"
import type { HttpServer } from "effect/http"
import { GraphQLSchema } from "graphql"
import { expectTypeOf } from "vitest"
import { PubSub } from "../src/index.ts"
import type * as GraphQLLambda from "../src/index.ts"
import type { Gateway } from "../src/index.ts"
import {
  ConnectionId,
  OperationId,
  EventId,
  Topic,
  Connection,
  SubscriptionOperation,
  PublishedEvent,
  ConnectionRejected,
  type ContextRequest,
  type GatewayService,
  createHttpHandler,
  createWebSocketHandler,
  type Executor,
  type ProxyResponse,
  type HttpResponse,
  type HttpEvent,
  type GraphiQLOptions,
} from "../src/index.ts"
import * as GraphQLLocal from "../src/Local.ts"
import { JsonObject } from "../src/Models.ts"
import { publish } from "../src/PubSub.ts"
import { streamHandler } from "../src/PubSub/DynamoDB.ts"
import type * as Request from "../src/Request.ts"
import { Sandbox as PubSubSandbox } from "../src/Sandbox.ts"
import type { Storage, StorageService } from "../src/Storage.ts"
import {
  builderOptions,
  type BuilderContext,
  type Greeting,
  type Identity,
} from "./builder.ts"

declare const pubSub: PubSub.Source
declare const gateway: GatewayService

class ResolverService extends Context.Service<
  ResolverService,
  { readonly value: string }
>()("ResolverService") {}
class IdentityService extends Context.Service<
  IdentityService,
  { readonly user: string }
>()("IdentityService") {}
const executor: Executor<{ user: string }, never, ResolverService> = {
  schema: new GraphQLSchema({}),
  execute: () =>
    ResolverService.use((service) =>
      Effect.succeed({ data: { value: service.value } }),
    ),
  subscribe: () => Stream.succeed({ data: {} }),
}
const handler = createHttpHandler({
  executor,
  context: () =>
    IdentityService.use((service) => Effect.succeed({ user: service.user })),
})
declare const request: Parameters<typeof handler>[0]
expectTypeOf<typeof request>().toEqualTypeOf<HttpEvent>()
expectTypeOf<
  Effect.Success<ReturnType<typeof handler>>
>().toEqualTypeOf<HttpResponse>()
expectTypeOf<HttpEvent["version"]>().toEqualTypeOf<"2.0">()
expectTypeOf<HttpEvent["cookies"]>().toEqualTypeOf<
  ReadonlyArray<string> | undefined
>()
expectTypeOf<HttpResponse["cookies"]>().toEqualTypeOf<
  Array<string> | undefined
>()
expectTypeOf<
  Extract<keyof HttpEvent, "httpMethod" | "path" | "multiValueHeaders">
>().toEqualTypeOf<never>()
expectTypeOf<
  Extract<keyof HttpResponse, "multiValueHeaders">
>().toEqualTypeOf<never>()
// @ts-expect-error HTTP handlers require an explicit payload version.
handler({ headers: {}, body: "", requestContext: {} }, {})
const graphiql: GraphiQLOptions = {
  credentials: "include",
  endpoint: "/graphql",
  subscriptionUrl: "wss://api.example.test/graphql",
  headers: { authorization: "Bearer example" },
  query: "{ __typename }",
  title: "Example explorer",
  nonce: "example-nonce",
}
createHttpHandler({
  executor,
  context: () => Effect.succeed({ user: "ada" }),
  graphiql,
})
// @ts-expect-error Credentials are a closed browser fetch policy.
const invalidCredentials: GraphiQLOptions = { credentials: "always" }
const invalidSubscription: GraphiQLOptions = {
  // @ts-expect-error Subscription URLs use secure WebSockets.
  subscriptionUrl: "https://api.example.test",
}
void invalidCredentials
void invalidSubscription
export const requirement: Effect.Effect<
  ProxyResponse,
  never,
  ResolverService | IdentityService
> = handler(request, {})
// @ts-expect-error Resolver/context service requirements must survive transport construction.
export const missingLayers: Effect.Effect<ProxyResponse> = handler(request, {})
// @ts-expect-error Typed context must match the executor's context, locally at the option.
createHttpHandler({ executor, context: () => Effect.succeed({ user: 123 }) })
const websocket = createWebSocketHandler({
  persistedContext: JsonObject,
  executor,
  context: () => Effect.succeed({ user: "ada" }),
  onInit: () =>
    IdentityService.use((identity) => Effect.succeed({ user: identity.user })),
})
declare const wsRequest: Parameters<typeof websocket>[0]
// @ts-expect-error Authentication, resolver, Storage and Gateway requirements cannot be erased.
export const websocketWithoutLayers: Effect.Effect<ProxyResponse, unknown> =
  websocket(wsRequest, {})

// Callbacks infer their errors and requirements independently, even when they differ.
class UpgradeService extends Context.Service<
  UpgradeService,
  { origin: string }
>()("UpgradeService") {}
class InitService extends Context.Service<InitService, { token: string }>()(
  "InitService",
) {}
const independentOptions = {
  persistedContext: JsonObject,
  executor,
  context: (request: ContextRequest<{ cookie: string }>) =>
    IdentityService.use((identity) =>
      Effect.succeed({ user: identity.user + request.lambdaContext.cookie }),
    ),
  onConnect: () =>
    UpgradeService.use(() =>
      Effect.fail(new Error("origin store unavailable")),
    ),
  onInit: () => InitService.use(() => Effect.fail(new ConnectionRejected({}))),
}
const allHooks = createWebSocketHandler(independentOptions)
export const allHookServices: Effect.Effect<
  ProxyResponse,
  never,
  | ResolverService
  | IdentityService
  | UpgradeService
  | InitService
  | Storage
  | Gateway
> = allHooks(wsRequest, { cookie: "session" })
// @ts-expect-error Upgrade authentication requirements cannot be silently lost.
export const missingUpgrade: Effect.Effect<
  ProxyResponse,
  never,
  ResolverService | IdentityService | InitService | Storage | Gateway
> = allHooks(wsRequest, { cookie: "session" })
// @ts-expect-error Invocation context must retain its inferred middleware shape.
allHooks(wsRequest, { cookie: 123 })
const allHttp = createHttpHandler(independentOptions)
// HTTP does not require unused WebSocket authentication services.
export const httpServices: Effect.Effect<
  ProxyResponse,
  never,
  ResolverService | IdentityService
> = allHttp(request, { cookie: "session" })
const delivery = streamHandler(independentOptions)
// Delivery does not require authentication services from the same options object.
export const deliveryServices: Effect.Effect<
  { batchItemFailures: Array<{ itemIdentifier: string }> },
  unknown,
  ResolverService | IdentityService | Storage | Gateway
> = delivery({ Records: [] }, { cookie: "session" })

declare const storage: StorageService
const connectionId = ConnectionId.make("connection")
const operationId = OperationId.make("operation")
const eventId = EventId.make("event")
const topic = Topic.make("topic")
Connection.key(connectionId)
SubscriptionOperation.key(connectionId, operationId)
publish(topic, {})
// @ts-expect-error Raw strings must be branded at the boundary.
Connection.key("connection")
// @ts-expect-error Connection and operation IDs are distinct despite both being strings.
Connection.key(operationId)
// @ts-expect-error Events cannot be addressed as connections.
Connection.key(eventId)
// @ts-expect-error The subscription operation key order is enforced by its brands.
SubscriptionOperation.key(operationId, connectionId)
// @ts-expect-error Publishing requires a topic, not an event ID.
publish(eventId, {})
// @ts-expect-error Public storage APIs cannot accept an operation ID as the connection ID.
storage.reserve(operationId, operationId, { query: "{ me }" }, 10)
// @ts-expect-error The operation ID cannot be a connection ID.
storage.stop(connectionId, connectionId)
// @ts-expect-error A connection ID is not an event ID in the stored model.
new PublishedEvent({ id: connectionId, topic, payload: "{}", ttl: 7200 })
// @ts-expect-error PubSub topics must remain branded through resolver context.
pubSub.subscribe("topic")
// @ts-expect-error AWS gateway calls retain the connection ID brand.
gateway.close("https://example.test/ws", eventId)

createHttpHandler({
  executor,
  context: () => Effect.succeed({ user: "ada" }),
  // @ts-expect-error Credentialed CORS cannot reflect arbitrary origins.
  cors: { origin: true, credentials: true },
})
createHttpHandler({
  executor,
  context: () => Effect.succeed({ user: "ada" }),
  // @ts-expect-error Safelisted headers cannot be used to disable the CSRF boundary.
  csrfHeaders: ["accept"],
})

// Exercise the actual builder seam, including resolver and context requirements.
const builderHttp = createHttpHandler(builderOptions)
const builderWebSocket = createWebSocketHandler(builderOptions)
const builderDelivery = streamHandler(builderOptions)
expectTypeOf(builderOptions.executor).toExtend<
  Executor<BuilderContext, never, Greeting>
>()
expectTypeOf<Effect.Services<ReturnType<typeof builderHttp>>>().toEqualTypeOf<
  Greeting | Identity
>()
expectTypeOf<
  Effect.Services<ReturnType<typeof builderWebSocket>>
>().toEqualTypeOf<Greeting | Identity | Storage | Gateway>()
expectTypeOf<
  Effect.Services<ReturnType<typeof builderDelivery>>
>().toEqualTypeOf<Greeting | Identity | Storage | Gateway>()
createHttpHandler({
  executor: builderOptions.executor,
  // @ts-expect-error The builder's typed context cannot be replaced with an incomplete value.
  context: () => Effect.succeed({ user: "Ada" }),
})
// @ts-expect-error Invocation context remains typed through builder integration.
builderHttp(request, { requestId: 123 })

// Transport parsing/validation helpers and their failure types are not public API.
type InternalRequestExports =
  | "header"
  | "parseJson"
  | "decodeRequest"
  | "prepare"
  | "RequestValidationError"
expectTypeOf<
  Extract<keyof typeof GraphQLLambda, InternalRequestExports>
>().toEqualTypeOf<never>()
expectTypeOf<
  Extract<keyof typeof Request, InternalRequestExports>
>().toEqualTypeOf<never>()

const persistedSchema = Schema.Struct({
  user: Schema.String,
  since: Schema.DateFromString,
})
const typedPersistedOptions = {
  executor,
  persistedContext: persistedSchema,
  context: (
    request: ContextRequest<{ cookie: string }, typeof persistedSchema.Type>,
  ) => {
    expectTypeOf(request.persisted.since).toEqualTypeOf<Date>()
    return Effect.succeed({ user: request.persisted.user })
  },
  onConnect: () => Effect.succeed({ user: "Ada", since: new Date() }),
  onInit: (
    _payload: JsonObject,
    previous: typeof persistedSchema.Type | undefined,
  ) => Effect.succeed({ user: previous?.user ?? "Ada", since: new Date() }),
}
const typedPersistedWebsocket = createWebSocketHandler(typedPersistedOptions)
const typedPersistedStream = PubSub.DynamoDB.streamHandler(
  typedPersistedOptions,
)
expectTypeOf<
  Effect.Services<ReturnType<typeof typedPersistedWebsocket>>
>().toEqualTypeOf<ResolverService | Storage | Gateway>()
expectTypeOf<
  Effect.Services<ReturnType<typeof typedPersistedStream>>
>().toEqualTypeOf<ResolverService | Storage | Gateway>()
createWebSocketHandler({
  executor,
  persistedContext: persistedSchema,
  context: (request) => {
    expectTypeOf(request.persisted.since).toEqualTypeOf<Date>()
    return Effect.succeed({ user: request.persisted.user })
  },
  onInit: (_payload, previous) => {
    expectTypeOf(previous).toEqualTypeOf<
      typeof persistedSchema.Type | undefined
    >()
    return Effect.succeed({ user: "Ada", since: new Date() })
  },
})
createWebSocketHandler({
  ...typedPersistedOptions,
  // @ts-expect-error The encoded side must be JSON; a Date requires a JSON codec.
  persistedContext: Schema.Struct({ user: Schema.String, since: Schema.Date }),
})
PubSub.DynamoDB.streamHandler({
  ...typedPersistedOptions,
  // @ts-expect-error Stream delivery enforces the same JSON boundary.
  persistedContext: Schema.Struct({ user: Schema.String, since: Schema.Date }),
})
createWebSocketHandler({
  ...typedPersistedOptions,
  // @ts-expect-error Hooks return decoded P, not its encoded representation.
  onInit: () => Effect.succeed({ user: "Ada", since: "2026-01-01" }),
})
const publication = PubSub.publish(topic, {})
expectTypeOf<
  Effect.Services<typeof publication>
>().toEqualTypeOf<PubSub.Publisher>()
const privateTable = PubSub.DynamoDB.layerFrom(Sandbox.layerMemory())
expectTypeOf<Layer.Success<typeof privateTable>>().toEqualTypeOf<
  PubSub.Storage | PubSub.Publisher
>()
expectTypeOf<
  Extract<Layer.Success<typeof privateTable>, DynamoDB>
>().toEqualTypeOf<never>()

class ContextDecoder extends Context.Service<
  ContextDecoder,
  { readonly enabled: boolean }
>()("ContextDecoder") {}
class ContextEncoder extends Context.Service<
  ContextEncoder,
  { readonly enabled: boolean }
>()("ContextEncoder") {}
declare const serviceCodec: Schema.Codec<
  typeof persistedSchema.Type,
  typeof persistedSchema.Encoded,
  ContextDecoder,
  ContextEncoder
>
const schemaWebsocket = createWebSocketHandler({
  ...typedPersistedOptions,
  persistedContext: serviceCodec,
})
const schemaDelivery = PubSub.DynamoDB.streamHandler({
  ...typedPersistedOptions,
  persistedContext: serviceCodec,
})
expectTypeOf<
  Effect.Services<ReturnType<typeof schemaWebsocket>>
>().toEqualTypeOf<
  ResolverService | Storage | Gateway | ContextDecoder | ContextEncoder
>()
expectTypeOf<
  Effect.Services<ReturnType<typeof schemaDelivery>>
>().toEqualTypeOf<ResolverService | Storage | Gateway | ContextDecoder>()
createWebSocketHandler({
  ...typedPersistedOptions,
  // @ts-expect-error Unknown encoded values cannot cross the JSON persistence boundary.
  persistedContext: Schema.Unknown,
})

const memoryPubSub = PubSubSandbox.layerMemory()
expectTypeOf<Layer.Success<typeof memoryPubSub>>().toEqualTypeOf<
  PubSub.Storage | PubSub.Publisher | PubSubSandbox
>()
expectTypeOf<
  Extract<Layer.Success<typeof memoryPubSub>, DynamoDB | Sandbox>
>().toEqualTypeOf<never>()

// @ts-expect-error A supplied layer must provide the DynamoDB table service.
PubSub.DynamoDB.layerFrom(Layer.empty)

// Local adapters retain every application requirement and provide their own Gateway.
const localHttp = GraphQLLocal.layer({ http: handler })
expectTypeOf<Layer.Services<typeof localHttp>>().toEqualTypeOf<
  ResolverService | IdentityService | HttpServer.HttpServer
>()
const localSocket = GraphQLLocal.layer({ webSocket: websocket })
expectTypeOf<Layer.Services<typeof localSocket>>().toEqualTypeOf<
  ResolverService | IdentityService | Storage | HttpServer.HttpServer
>()
const localAll = GraphQLLocal.layer({
  http: allHttp,
  webSocket: allHooks,
  stream: PubSub.DynamoDB.streamHandler(independentOptions),
  lambdaContext: ({ context }) => ({ cookie: context.awsRequestId }),
})
expectTypeOf<Layer.Services<typeof localAll>>().toEqualTypeOf<
  | ResolverService
  | IdentityService
  | UpgradeService
  | InitService
  | Storage
  | PubSubSandbox
  | HttpServer.HttpServer
>()
// @ts-expect-error Middleware contexts cannot be replaced with the default Lambda context.
GraphQLLocal.layer({ http: allHttp })
GraphQLLocal.layer({
  webSocket: allHooks,
  // @ts-expect-error A custom context factory must supply the middleware fields.
  lambdaContext: () => ({ cookie: 123 }),
})
const localCodec = GraphQLLocal.layer({
  webSocket: schemaWebsocket,
  stream: schemaDelivery,
  lambdaContext: () => ({ cookie: "local" }),
})
expectTypeOf<Layer.Services<typeof localCodec>>().toEqualTypeOf<
  | ResolverService
  | ContextDecoder
  | ContextEncoder
  | Storage
  | PubSubSandbox
  | HttpServer.HttpServer
>()
const localDelivery = GraphQLLocal.delivery(schemaDelivery, {
  lambdaContext: () => ({ cookie: "local" }),
})
expectTypeOf<Layer.Services<typeof localDelivery>>().toEqualTypeOf<
  ResolverService | ContextDecoder | Storage | Gateway | PubSubSandbox
>()
// @ts-expect-error Standalone delivery retains its Gateway requirement.
const missingLocalGateway: Layer.Layer<
  GraphQLLocal.Delivery,
  never,
  ResolverService | ContextDecoder | Storage | PubSubSandbox
> = localDelivery
void missingLocalGateway
// @ts-expect-error Standalone delivery also requires a factory for custom middleware context.
GraphQLLocal.delivery(PubSub.DynamoDB.streamHandler(independentOptions))
const customLocalDelivery = GraphQLLocal.delivery(
  PubSub.DynamoDB.streamHandler(independentOptions),
  { lambdaContext: () => ({ cookie: "local" }) },
)
expectTypeOf<Layer.Services<typeof customLocalDelivery>>().toEqualTypeOf<
  ResolverService | IdentityService | Storage | Gateway | PubSubSandbox
>()

expectTypeOf<
  Layer.Success<typeof localAll>
>().toEqualTypeOf<GraphQLLocal.Delivery>()
// @ts-expect-error Local's socket Gateway is private to its handlers.
const exportedLocalGateway: Layer.Layer<
  Gateway,
  never,
  Layer.Services<typeof localAll>
> = localAll
void exportedLocalGateway
// @ts-expect-error Local paths are absolute.
GraphQLLocal.layer({ path: "graphql", http: handler })

// A latency function's services carry into the layer; a fixed latency adds none.
const localFixedLatency = GraphQLLocal.layer({
  http: handler,
  latency: "1 second",
})
expectTypeOf<Layer.Services<typeof localFixedLatency>>().toEqualTypeOf<
  ResolverService | IdentityService | HttpServer.HttpServer
>()
const localLatency = GraphQLLocal.layer({
  http: handler,
  latency: () => UpgradeService.use(() => Effect.succeed("1 second" as const)),
})
expectTypeOf<Layer.Services<typeof localLatency>>().toEqualTypeOf<
  ResolverService | IdentityService | UpgradeService | HttpServer.HttpServer
>()
GraphQLLocal.layer({
  http: handler,
  // @ts-expect-error A latency function cannot fail.
  latency: () => Effect.fail(new Error("no latency")),
})

expectTypeOf<
  Extract<keyof typeof GraphQLLambda, "Local" | "Sandbox" | "GatewayMessages">
>().toEqualTypeOf<never>()
expectTypeOf<
  Extract<
    keyof typeof PubSub.DynamoDB,
    "Sandbox" | "layerMemory" | "layerLocal"
  >
>().toEqualTypeOf<never>()
