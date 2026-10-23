import { Context, Effect, Stream } from "effect"
import { GraphQLSchema } from "graphql"
import { expectTypeOf } from "vitest"
import type * as GraphQLLambda from "../src/index.ts"
import type { Gateway, Storage } from "../src/index.ts"
import {
  ConnectionId,
  OperationId,
  EventId,
  Topic,
  Connection,
  SubscriptionOperation,
  PublishedEvent,
  ConnectionRejected,
  TransportError,
  createStreamHandler,
  createDirectPublisher,
  publish,
  type StorageService,
  type ContextRequest,
  type PubSub,
  type GatewayService,
  createHttpHandler,
  createWebSocketHandler,
  type Executor,
  type ProxyResponse,
} from "../src/index.ts"
import type * as Request from "../src/Request.ts"
import {
  builderOptions,
  type BuilderContext,
  type Greeting,
  type Identity,
} from "./builder.ts"

declare const pubSub: PubSub
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
  executor,
  context: () => Effect.succeed({ user: "ada" }),
  onConnect: () =>
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
class HealthService extends Context.Service<
  HealthService,
  { healthy: boolean }
>()("HealthService") {}
const independentOptions = {
  executor,
  context: (request: ContextRequest<{ cookie: string }>) =>
    IdentityService.use((identity) =>
      Effect.succeed({ user: identity.user + request.lambdaContext.cookie }),
    ),
  onWebsocketConnect: () =>
    UpgradeService.use(() =>
      Effect.fail(new Error("origin store unavailable")),
    ),
  onConnect: () =>
    InitService.use(() => Effect.fail(new ConnectionRejected({}))),
  healthCheck: HealthService.use(() =>
    Effect.fail(
      new TransportError({
        message: "unhealthy",
        code: "UNAVAILABLE",
        status: 503,
      }),
    ),
  ),
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
  ResolverService | IdentityService | HealthService
> = allHttp(request, { cookie: "session" })
// @ts-expect-error Health-check requirements cannot be silently lost.
export const missingHealth: Effect.Effect<
  ProxyResponse,
  never,
  ResolverService | IdentityService
> = allHttp(request, { cookie: "session" })
const delivery = createStreamHandler(independentOptions)
// Delivery does not require authentication or health services from the same options object.
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

const direct = createDirectPublisher(independentOptions)
// Inline publication needs the storage abstraction, not the DynamoDB implementation.
export const directServices: Effect.Effect<
  PublishedEvent,
  unknown,
  ResolverService | IdentityService | Storage | Gateway
> = direct(topic, {}, { cookie: "session" })
// @ts-expect-error Resolver requirements survive direct publication.
export const missingDirectResolver: Effect.Effect<
  PublishedEvent,
  unknown,
  IdentityService | Storage | Gateway
> = direct(topic, {}, { cookie: "session" })
// @ts-expect-error Direct publication retains its invocation context.
direct(topic, {}, { cookie: 123 })
// @ts-expect-error Inline publishing requires a branded topic.
direct("topic", {}, { cookie: "session" })

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
const builderDelivery = createStreamHandler(builderOptions)
const builderDirect = createDirectPublisher(builderOptions)
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
expectTypeOf<Effect.Services<ReturnType<typeof builderDirect>>>().toEqualTypeOf<
  Greeting | Identity | Storage | Gateway
>()
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
