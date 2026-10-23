# @model-ts/graphql-lambda

Effect-native GraphQL transport for API Gateway HTTP API payload v1.0 and
API Gateway WebSockets. Replaces `aws-lambda-graphql` and Apollo Server Lambda
with an independent executor interface. Requires Effect
4.0.5+, model-ts 7, graphql-js ^16.11.0, and Node 22+. Use one shared
graphql-js installation for the schema, executor and transport.

## Using with @model-ts/graphql

Pass `executor(schema)` from `@model-ts/graphql` directly as the executor. It preserves the
builder's declared public errors, `FORBIDDEN` codes, messages and source
locations. Internal failures are masked by the builder and reported through
`ErrorReporter`; transport formatting does not reinterpret them.

This complete example exposes public greetings and announcements. Add your
application's authentication when exposing private data: the context factory
rebuilds the authenticated identity for each request or delivery, and
`onConnect` explicitly maps accepted init parameters into persisted context.

```ts
import { DynamoDB } from "@model-ts/dynamodb"
import { createBuilder, executor, ErrorReporter } from "@model-ts/graphql"
import {
  Gateway,
  Storage,
  Topic,
  createHttpHandler,
  createWebSocketHandler,
  createStreamHandler,
  createDirectPublisher,
  type ContextRequest,
  type PubSub,
} from "@model-ts/graphql-lambda"
import { Context, Effect, Layer, ManagedRuntime, Schema, Stream } from "effect"

interface AppContext {
  readonly requestId: string
  readonly pubSub: PubSub
}
interface LambdaContext {
  readonly awsRequestId: string
}
class Greeting extends Context.Service<
  Greeting,
  { readonly message: string }
>()("app/Greeting") {}

const GraphQL = createBuilder<{}, AppContext>()
const announcements = Topic.make("public/announcements")
const greeting = GraphQL.queryField(
  "greeting",
  GraphQL.field("String", (_, __, context) =>
    Effect.map(Greeting, ({ message }) => message + " " + context.requestId),
  ),
)
const announcement = GraphQL.subscriptionField(
  "announcement",
  GraphQL.field("String", {
    subscribe: (_, __, context) =>
      context.pubSub
        .stream(announcements)
        .pipe(
          Stream.map((payload) =>
            Schema.decodeUnknownSync(Schema.String)(payload),
          ),
        ),
  }),
)
const schema = GraphQL.schema({ greeting, announcement })
const options = {
  executor: executor(schema),
  context: (request: ContextRequest<LambdaContext>) =>
    Effect.succeed({
      requestId: request.lambdaContext.awsRequestId,
      pubSub: request.pubSub,
    }),
}
const http = createHttpHandler(options)
const websocket = createWebSocketHandler(options)
const delivery = createStreamHandler(options)
const publishDirect = createDirectPublisher(options)

const runtime = ManagedRuntime.make(
  Layer.mergeAll(
    Storage.layer.pipe(
      Layer.provideMerge(
        DynamoDB.layer({
          tableName: process.env.GRAPHQL_TABLE_NAME!,
          cursors: "plain",
        }),
      ),
    ),
    Gateway.layer(),
    Layer.succeed(Greeting, { message: "Hello" }),
    Layer.succeed(ErrorReporter, {
      report: ({ cause, coordinate }) => Effect.logError(coordinate, cause),
    }),
  ),
)

export const httpHandler = (
  event: Parameters<typeof http>[0],
  context: LambdaContext,
) => runtime.runPromise(http(event, context))

export const wsHandler = (
  event: Parameters<typeof websocket>[0],
  context: LambdaContext,
) => runtime.runPromise(websocket(event, context))

// Optional DynamoDB stream Lambda for events written with publish().
export const streamHandler = (
  event: Parameters<typeof delivery>[0],
  context: LambdaContext,
) => runtime.runPromise(delivery(event, context))

// Call from an EventBridge projector for bounded, inline fan-out.
export const announce = (message: string, context: LambdaContext) =>
  runtime.runPromise(publishDirect(announcements, message, context))
```

The runtime lives outside Lambda exports and retains all application services.
Resolver requirements such as `Greeting`, and context/authentication Effect
requirements, flow into handler types. Missing layers fail compilation. Only
the tests depend on `@model-ts/graphql`; applications can still supply a plain
graphql-js schema via `graphqlExecutor`.

The builder's subscription must use `context.pubSub.stream` (or the equivalent
finite iterable source), so registration can discover topics and finish before
Lambda returns. Both direct and queued publication recreate this source with
one event and rerun the builder's filtering and authorization. Tests run this
wiring with graphql-js 16 on memory and DynamoDB Local.

## Handlers and executor

`createHttpHandler`, `createWebSocketHandler` and `createStreamHandler` return
functions `(event, lambdaContext) => Effect`. Run them in the application's
shared `ManagedRuntime`; do not build a runtime per request. Middleware can
continue adding typed data to `lambdaContext` and inspecting the proxy response.
No AWS client is constructed until its layer is built.

```ts
import { DynamoDB } from "@model-ts/dynamodb"
import {
  Gateway,
  Storage,
  createHttpHandler,
  createWebSocketHandler,
  createStreamHandler,
  graphqlExecutor,
} from "@model-ts/graphql-lambda"
import { Layer, ManagedRuntime } from "effect"
import { schema, makeContext, authenticateInit } from "./api.ts"
import type { AppContext, LambdaContext } from "./context.ts"

const runtime = ManagedRuntime.make(
  Layer.merge(
    Storage.layer.pipe(
      Layer.provideMerge(
        DynamoDB.layer({
          tableName: process.env.GRAPHQL_TABLE_NAME!,
          cursors: "plain", // Transport only uses internal, key-based pagination.
        }),
      ),
    ),
    Gateway.layer(),
  ),
)
const options = {
  executor: graphqlExecutor<AppContext>(schema),
  context: makeContext,
  onConnect: authenticateInit,
  introspection: true,
}
const http = createHttpHandler(options)
const websocket = createWebSocketHandler(options)
const delivery = createStreamHandler(options)

export const httpHandler = (
  event: Parameters<typeof http>[0],
  context: LambdaContext,
) => runtime.runPromise(http(event, context))
export const wsHandler = (
  event: Parameters<typeof websocket>[0],
  context: LambdaContext,
) => runtime.runPromise(websocket(event, context))
export const streamHandler = (
  event: Parameters<typeof delivery>[0],
  context: LambdaContext,
) => runtime.runPromise(delivery(event, context))
```

Add application service layers to this runtime. The handler's requirement type
retains requirements from the executor, context factory and authentication
hooks. `graphqlExecutor<C>(schema)` adapts a plain graphql-js `GraphQLSchema`.
An Effect-aware builder implements the small `Executor<C, E, R>` interface:

- `schema: GraphQLSchema`, for operation selection and transport validation rules.
- Optional `validation: "executor"`, declaring that execute/subscribe own standard schema validation.
- `execute({ document, request, context }): Effect<Result, E, R>`.
- `subscribe(...): Stream.Stream<Result, E, R>`.

`Result` is a JSON-safe, `FormattedExecutionResult`-shaped object. The executor
owns result error policy and formatting. Errors must already contain safe
messages, paths, locations and extensions; no raw `GraphQLError` instances
cross this seam. The transport forwards these results unchanged on HTTP,
registration and both delivery paths, so builder public codes survive.

`executor(schema)` declares `validation: "executor"`. The transport
then leaves standard schema validation to the executor, including scalar literal
parsing, so literals and variables share its public/internal error policy.
Parsing, operation selection, GET restrictions, query limits, introspection
restrictions, subscription root selection checks and application `validationRules`
remain transport responsibilities. Application rules run only after standard
validation succeeds. When an executor owns validation and application rules are
configured, the transport runs standard validation as a gate: it discards those
errors and skips application rules on failure, letting the executor report the
original errors under its own policy. This gate may parse scalar literals again;
without application rules, no transport standard-validation pass occurs.
A throwing application rule produces a masked internal error with its cause
logged on both HTTP and WebSockets. There is no built-in maximum-depth option;
use application validation rules for depth limits.
Without that capability (including `graphqlExecutor`), the transport performs
standard validation before context creation, preserving the plain-schema path.

For a plain schema use `graphqlExecutor(schema, { formatError })`. It wraps
custom scalar input callbacks in a copied type graph (without mutating the
application schema), distinguishes their failures from graphql-js diagnostics,
and formats/JSON-normalizes execution results. Handler `formatError` only
handles transport failures, including request validation, context and gateway
failures; it does not reformat executor results.

The transport consumes Effect Streams directly; `graphqlExecutor` alone adapts
graphql-js async iterables with `Stream.fromAsyncIterable`. A builder can return
its existing subscription Stream without another iterable bridge.
`executor(schema)` satisfies this interface directly.

Registration runs the subscription Stream to its first result or completion.
The operation-local pub/sub source is **finite and empty** during registration:
this opens even lazy Effect subscription resolvers, collects their topics and
closes their scopes without waiting for a future event. Errors are emitted as
results; successful empty completion registers the collected topics. During
delivery the same source contains one event, and `Stream.runHead` consumes at
most one result. Subscription resolvers must use these finite sources; an
unrelated live or never-ending source cannot be registered durably by Lambda.
Stream finalizers run on completion, failure and interruption. There is no live
in-process subscription fiber between Lambda invocations; stop/disconnect
remove the durable state that future deliveries consult.

Integration with `@model-ts/graphql` uses this structural interface; the package
has no dependency on that builder's internals.

The context factory returns an Effect with typed errors and receives:

- `event`: original HTTP event, WebSocket event, stream record, or inline
  `{ delivery: "direct", publishedEvent }` descriptor.
- `lambdaContext`: the invocation's typed middleware context.
- `persisted`: the connection's JSON context, empty for HTTP.
- `connection`: current connection on WebSocket and delivery paths.
- `pubSub`: an operation-local source, described below.
- `response.headers` and `response.cookies`: mutable HTTP response output.

Unlike the old implicit context spread, the application explicitly incorporates
`persisted` into its typed context. HTTP batches share one context factory call
and response container; operations execute in order. Duplicate cookies are
returned as `multiValueHeaders["Set-Cookie"]`. Input headers are preserved, and
`header(event, name)` looks them up case-insensitively.

### HTTP

POST accepts one GraphQL request or a nonempty array, including base64 proxy
bodies. GET accepts `query`, JSON `variables`, JSON `extensions` and
`operationName` query parameters. Mutations over GET return 405 with `Allow:
POST`. Subscriptions use WebSockets. Invalid requests/validation return 400;
resolver errors return execution results. Each batch element has its own result;
the batch response has status 200.

Defaults: maximum 20 requests per batch, 1 MiB decoded body, 64 KiB query;
introspection enabled. `validationRules`, `maxBatchSize`, `maxBodyBytes`,
`maxQueryBytes` and `introspection` configure these.

CORS is disabled by default. For credentialed cross-origin requests, configure
an explicit exact-origin policy, for example
`cors: { origin: ["https://app.example"], credentials: true }`.
Neither unrestricted reflection nor `"*"` is accepted with credentials.
Unmatched origins receive no allow-origin or allow-credentials grant.
With CORS configured, preflight echoes requested headers and returns 204.

Before parsing/validating GraphQL or creating application context, both GET
and POST require `Content-Type: application/json` (parameters such as charset
are allowed), or a non-empty `Apollo-Require-Preflight` /
`X-Apollo-Operation-Name` header. Browsers must preflight these requests;
CORS alone is not CSRF prevention. `csrfHeaders` can replace the accepted
header names with explicit `x-...` headers or `apollo-require-preflight`;
an empty list permits only JSON Content-Type. Safelisted headers cannot
disable this check. Configure clients to send JSON or an accepted header.
GET never executes mutations or subscriptions and does not create context
for those rejected operations.

HTTP defects, including context/executor failures, serialization errors and
throwing formatters, return a fixed safe 500 GraphQL response and log the
cause. Interruption remains interruption.

`explorerHtml` serves an application-owned GraphiQL/Playground document on GET
with `Accept: text/html`. Bundle or self-host its assets in the application;
this package does not fetch a third-party script at runtime. Configure its
fetcher for credentials and its subscription client for the legacy protocol.
An optional `healthCheck` Effect enables `/.well-known/apollo/server-health`;
route that path explicitly in API Gateway and send a JSON content type or
accepted preflight header when checking it. OPTIONS and the static explorer
do not invoke application context or resolvers. Warmers remain application
middleware.

### WebSocket authentication and protocol

Only the legacy **`graphql-ws` subprotocol** used by
`subscriptions-transport-ws` 0.11 is implemented. This is not the modern
`graphql-transport-ws` protocol. Clients send `connection_init`, `start`,
`stop`, `connection_terminate`; responses are `connection_ack`, `error`,
`data`, `complete`, and optional `ka`.

`$connect` writes a connection, calls optional
`onWebsocketConnect(connection, event, lambdaContext)`, and echoes an offered
`graphql-ws` subprotocol case-insensitively. The hook returns an Effect of a
JSON object to persist, or `false`. Returning `false` or failing with the typed
`ConnectionRejected` error rejects with 401 and removes the connection. Other
typed failures and defects return 500, log the cause, and remove failed-upgrade
state. Origin and cookie authentication belongs here.

`connection_init` calls optional
`onConnect(payload, connection, event, lambdaContext)`. It merges the returned
JSON object into persisted context, marks initialized and posts the ack.
Rejection posts an error, removes subscriptions and calls DeleteConnection.
Infrastructure failures and defects return 500 with their cause logged; the
uninitialized connection remains available for a retry, without an auth error
frame or acknowledgment. Subscription JWT authentication belongs here. With neither hook,
connections are unauthenticated. Init payload is always stored separately as
`connection.initParams`; without `onConnect`, persisted authenticated context
is unchanged. Only an explicit `onConnect` mapping can promote init fields
into that context.
Repeated init acknowledges without replacing authenticated state.

A start racing init retries the consistent connection read ten times at 50 ms
intervals (`initRetries`, `initDelayMillis`). An uninitialized connection is
closed. Every start reserves a generation before context/resolver setup. Stop removes
pending and active operations; activation is conditional on that generation.
Reusing an operation ID invalidates the old setup and pauses delivery for that
ID. Activation atomically replaces its old topic memberships with the new ones.
A stale setup cannot activate or overwrite its replacement. Default
maximum is 100 operations per connection (`maxOperations`); concurrent starts
are serialized by a connection version check. Each operation supports 1–20
unique topics. Topic names are application-owned, including tenant prefixes.
Responses above 32 KiB (encoded UTF-8 bytes) fail with `MessageTooLarge` and
are logged before sending, matching the single-frame limit in the
[API Gateway quotas](https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-execution-service-websocket-limits-table.html). `connection_terminate` closes immediately; disconnect and
Gone cleanup also work when the connection row was already TTL-deleted.

No keep-alive timer runs inside Lambda. An application scheduler may call
`keepAlive(connectionId)` for each known active connection at an interval
below its client timeout. Keep-alive is opt-in: scheduling it once and then
stopping would activate the legacy client's liveness checks without feeding
them. The package does not scan all connections to discover these IDs.

Connection IDs, operation IDs, event IDs and topics use distinct Schema brands:
`ConnectionId`, `OperationId`, `EventId`, and `Topic`. Decode them at external
boundaries or use their `.make(string)` constructors for application-created
values. Their wire and stored representations remain strings. Handler factories
infer the union of requirements from individual callbacks; options have only
the context, Lambda context, error and requirement type parameters.

## Publishing, filtering and delivery

```ts
import { publish, Topic, withFilter } from "@model-ts/graphql-lambda"

// Run this Effect in the existing EventBridge projector:
const publication = publish(Topic.make(`USER#${userId}/APP_EVENTS`), payload)

// A graphql-js subscribe resolver:
subscribe: (_parent, args, context) =>
  withFilter(
    context.pubSub.subscribe([
      Topic.make(`USER#${userId}`),
      Topic.make(`ORG#${orgId}`),
    ]),
    (payload) => EventPayload.decode(payload).domainId === args.id,
  )
```

Decode JSON payloads with your schema before using their properties. The code
above's `EventPayload.decode` is an application decoder; model-ts models also
have `.from` for synchronous decoding. Effect-based subscription resolvers can
use `context.pubSub.stream(topics)` and `Stream.filter` / `Stream.filterEffect`.
Topics are collected eagerly when calling either source method. `withFilter`
accepts a source iterable and a plain/Promise boolean predicate, uses strict
`true`, and forwards iterator cancellation.

`publish(topic, payload, ttlSeconds = 7200)` JSON-stringifies once and writes an
event model, returning it. Dates become strings and models become plain JSON,
matching the old production serialization. Invalid JSON/empty topics fail
before a write. Local tests use exactly the same JSON boundary.

The existing stream path remains available:

```
EventBridge domain event → application projector → publish → DynamoDB INSERT
→ stream Lambda → topic subscribers → PostToConnection
```

For inline delivery, construct a publisher once alongside the handlers:

```ts
import { createDirectPublisher, Topic } from "@model-ts/graphql-lambda"

const publishDirect = createDirectPublisher(options)

// Run this Effect in the EventBridge-triggered projector:
const publication = publishDirect(
  Topic.make(`USER#${userId}/APP_EVENTS`),
  payload,
  lambdaContext,
)
```

`publishDirect(topic, payload, lambdaContext, ttlSeconds = 7200)` serializes
identically to `publish`, delivers to current subscribers, and returns a
`PublishedEvent` without persisting it. The direct path is:

```
EventBridge domain event → application projector → publishDirect
→ topic subscribers → PostToConnection
```

Use inline delivery for bounded fan-out when the projector has enough remaining
Lambda duration for the sequential reads, resolver work and gateway calls.
The publisher's Effect completes only after delivery completes. Carry any
original EventBridge metadata needed by the context factory in the typed
`lambdaContext`; its `event` is the direct-delivery descriptor above.

Use the Events-table/stream path when fan-out may outlast the publishing
Lambda, or when delivery needs separate concurrency and retry settings.
The stream Lambda still needs enough duration for one event's fan-out; this
package does not shard a large event across workers. The event storage role
and its stream trigger are optional for applications using only direct
delivery. Choose one path per publication; doing both sends duplicates.
The existing `@model-ts/eventbridge` service continues to own domain-event
publishing.

Both entry points call the same subscriber delivery loop. The stream adapter
only processes `INSERT` records tagged `GraphQLPublishedEvent`; other models
and TTL deletes on the shared table are ignored. Expired event, connection and
subscription rows are hidden before DynamoDB TTL deletion.

Delivery uses consistent reads and 50-row topic pages. It fetches the current
connection and operation, rebuilds application context, validates the query,
and reruns subscribe guards, filters and resolve authorization for each
subscriber. Subscribers run sequentially: existing app context factories use
invocation-level AsyncLocalStorage; parallel execution would overwrite their
origin state. There is no identity cache or stored identity copy in topic
rows. This costs per-subscriber reads/context work, as in the old transport.

Gone (HTTP 410) removes the connection and all its operations/topics. Other AWS
failures retain state and return the DynamoDB **sequence number** in
`batchItemFailures`. Enable `ReportBatchItemFailures` on the event source and
configure retries, maximum record age and an on-failure destination in CDK.
Inline delivery attempts every subscriber and fails with `DeliveryError`
containing the event ID and each failed connection/operation ID and original
cause. A subscriber-query failure surfaces as `StorageError`. The publishing
Lambda owns retries (for example, let its invocation fail for EventBridge to
retry); there is no fallback event write. Interruption stops delivery without
removing active subscriptions.

A failing subscriber does not prevent attempts for the remaining subscribers
or stream records. Successful subscribers may see duplicates when either
delivery path retries; clients need idempotent event handling. There is no replay on reconnect or
exactly-once delivery contract. Context/resolver failures become formatted
GraphQL results, not infrastructure retries. Synchronous context throws, Effect
defects and formatter defects are logged with their cause and produce a fixed
`INTERNAL_SERVER_ERROR` data frame for that subscriber. Other subscribers still
receive the publication; no private exception or custom formatter runs in that
fallback. Interruption remains interruption and awaits context cleanup.

This boundary covers request preparation, application context reconstruction and
executor work. Transport storage reads and gateway sends are outside it: their
typed failures retain the retry behavior described above, including when sending
the safe error frame fails. A lookup performed inside an application's context
factory belongs to context execution and follows its GraphQL error policy.

## Errors and AWS services

Default formatting masks resolver/infrastructure errors as
`INTERNAL_SERVER_ERROR`, including prelocated GraphQLErrors and custom scalar
exceptions. Missing `originalError` or a supplied extension code is not proof
that an error is public. Ordinary graphql-js parse, validation and built-in
coercion diagnostics retain their safe messages/codes.
`TransportError({ message, code, status })` explicitly marks a public error;
application validation rules must use this policy too.

Configure executor-result formatting at `graphqlExecutor(schema, { formatError })`
and transport-failure formatting on the handlers. Custom formatters are trusted
application policy and may report original causes to Sentry before choosing a
public message. Builder-formatted errors and declared result unions pass through
unchanged. Authentication rejections use a generic UNAUTHORIZED error;
infrastructure failures during authentication remain private server errors.

`Gateway` is a `Context.Service` with `post`/`close`. `Gateway.layer(config)`
uses AWS SDK v3 clients cached **per endpoint**, destroys them with its scope,
passes interruption signals, and produces typed `GatewayError` values with
`gone`, `operation`, and `cause`. Calls have PostToConnection/DeleteConnection
spans. `Gateway.layerMemory` records sends/closes in `GatewayMessages` and
supports `markGone` without contacting AWS. Supply your own Gateway layer to
simulate throttling or permission failures. Storage uses the existing traced
DynamoDB service; transport operations have their own Effect spans as well.

## Storage service

WebSocket handlers, keep-alive and both delivery paths require `Storage`, a
`Context.Service`, rather than accessing model persistence themselves.
`Storage.layer` supplies the model-ts DynamoDB implementation using the
application's existing `DynamoDB` service. Compose it with
`Layer.provideMerge` as above to also retain DynamoDB for queued `publish()`
and application model operations. `Layer.provide` suffices when only the
storage service is needed. For tests, supply `Sandbox.layerMemory()` or
`Sandbox.layerLocal()` to that same layer.

The service owns connection creation/update/live lookup, operation lookup,
generation reservation and conditional activation with limits, stop, full
disconnect cleanup and paged topic subscriber streams. Its contract includes TTL visibility and consistency:
a future implementation must preserve these semantics. Backend failures use
`StorageError({ operation, cause })`; expected missing/expired connections
and invalid registrations use public `TransportError` values. The default
implementation retains transaction retries, version checks, keyed cleanup
and the model layout below. No other storage implementation is included.

## Storage layout and infrastructure migration

Four model roles (three with direct delivery only) share the application's
model-ts table, with primary keys
`PK` and `SK`, no secondary indexes, TTL attribute **`ttl`**, and a NEW_IMAGE
stream. Prefixes are reserved to this package; connection/topic/operation
components use JSON encoding to avoid delimiter collisions.

| Model tag                    | PK                             | SK                                 | Contents                                                           |
| ---------------------------- | ------------------------------ | ---------------------------------- | ------------------------------------------------------------------ |
| GraphQLConnection            | `GRAPHQL#CONNECTION#<JSON id>` | `CONNECTION`                       | endpoint, context, initParams, initialized, createdAt, ttl         |
| GraphQLSubscriptionOperation | same connection PK             | `OPERATION#<JSON id>`              | connectionId, operationId, generation, state, request, topics, ttl |
| GraphQLSubscription          | `GRAPHQL#TOPIC#<JSON topic>`   | JSON `[connectionId, operationId]` | membership and ttl                                                 |
| GraphQLPublishedEvent        | `GRAPHQL#EVENT#<UUID>`         | `EVENT`                            | topic, JSON payload text, ttl                                      |

All models also carry `_tag` and `_docVersion`. Connection TTL is fixed at
connect time, defaults to two hours and cannot exceed two hours. Operations
and memberships inherit that exact TTL; activity and keep-alive never extend
it beyond API Gateway's maximum connection duration. Events have their own
publish-time TTL. Disconnect queries the connection partition rather than
scanning subscriptions. Registration/replacement/stop are atomic transactions,
so the old ignored BatchWrite `UnprocessedItems` issue does not apply.

Migration from the four old tables **requires a CDK change**, not a table-name
rename. Do not point this package at a legacy table with `id` or `event` keys.
Configure one model-ts table (or reuse the app table), TTL and grants; enable
its stream only when using queued publication.
Choose its name at `DynamoDB.layer({ tableName })`. The four
`AWS_GRAPHQL_*_TABLE` environment variables are replaced by whichever one
application setting supplies that layer (the example uses
`GRAPHQL_TABLE_NAME`). Preserve POST/DELETE ManageConnections permissions on
WebSocket Lambdas and whichever Lambdas perform delivery (stream consumers or
inline publishers). HTTP API payload must remain v1.0. Keep
GET+POST `/v1/graphql`, `$connect`/`$disconnect`/`$default`, cookie forwarding and
application credentialed CORS settings. Management endpoints are derived from
API Gateway's domainName/stage, not the browser's CloudFront URL.

Deploy new transport state separately, force clients to reconnect/resubscribe,
and let old connection/subscription/event rows expire. Existing rows are not
migrated, and pending old events are not replayed. Coordinate projector event
writes and the stream handler rollout. Keep the old event consumer alive long
enough to drain any desired pending old events before retiring its tables.

## Parity and deliberate differences

Kept: legacy protocol, init race wait, upgrade/init auth hooks, middleware
context, JSON publish semantics, strict filters, current-context delivery
reauthorization, two-hour lifetime, HTTP batching/GET/CORS/cookies, optional
introspection/explorer/health settings.

Changed deliberately:

- No default cross-origin grants; credentialed CORS requires explicit origins,
  and GET/POST enforce a JSON or non-simple-header CSRF boundary.
- Init parameters remain separate from trusted connection context.
- Pending operation generations prevent stopped or superseded starts from
  activating; replacement memberships change atomically.
- Executors own result formatting; transport failures have a separate policy.
- Single WebSocket sends are limited to 32 KiB; legacy clients have no chunk
  reassembly protocol, so the 128 KiB message quota cannot be used here.
- Optional inline publication shares filtering, authorization and cleanup with
  stream delivery; it avoids event writes for bounded fan-out.
- A storage service separates handlers from the default DynamoDB implementation.
- Explicit auth rejection is distinct from infrastructure failures and defects;
  the latter return 500 with their causes logged.
- Multi-topic subscriptions register and deliver every selected topic.
- One model-ts table replaces four tables; keyed cleanup replaces a scan.
- Atomic writes replace best-effort BatchWrite; operation limits are enforced
  across concurrent starts. Subscriptions cannot outlive their connection.
- Current connection context replaces per-membership snapshots; no stale
  persisted identity copy. Duplicate init cannot replace authenticated state.
- Formatters and validation rules run on delivery and WebSockets too.
- Subscription root fields are collected through fragments on graphql-js 16,
  rejecting multiple response keys and introspection roots. Following the
  [current GraphQL draft](https://spec.graphql.org/draft/#sec-Single-root-field),
  root `@skip`/`@include` directives are rejected without evaluating their values,
  including on root fragment spreads and inline fragments. This deliberately
  differs from graphql-js 16; directives below the root remain allowed.
- WebSocket query/mutation results send `complete`; terminate closes now.
- Header names and offered protocol tokens are matched correctly.
- Optional keep-alive, body/batch/query/message/operation/topic limits.
- Endpoint-aware client caches, partial-batch retry reporting and sequential
  subscriber execution. Applications must configure stream retries/DLQ.
- Event IDs are UUIDs rather than ULIDs; neither app uses their ordering.
- No APQ, uploads, Apollo reporting/cache-control plugin, Redis, implicit
  playground CDN, old `$$internal` context, or modern graphql-transport-ws.

## Testing and remaining evidence gaps

Tests use `@effect/vitest`, `Sandbox.layerMemory` and `Sandbox.layerLocal`,
plus `Gateway.layerMemory`. Local tests create and delete only their own
isolated tables at `localhost:8000`; they do not start/stop DynamoDB Local.
Both projects use graphql-js 16. Storage snapshots document key layout and TTL
alignment.

```sh
node_modules/.bin/tsc -b packages/graphql-lambda/tsconfig.test.json
node_modules/.bin/vitest run --project graphql-lambda:memory
node_modules/.bin/vitest run --project graphql-lambda:local
```

The source and type fixtures also compile with the TypeScript 7 native CLI;
there is no programmatic compiler API or `baseUrl` configuration.

Transport requirements §13 evidence status:

- Actual deployed header casing remains uninspected. Mixed-case cookie and
  protocol tests make this package independent of casing.
- Whether API Gateway intercepts every OPTIONS request remains uninspected;
  the handler's own preflight path is tested.
- The legacy transport's production multi-topic failure remains code-derived, with no
  deployed logs inspected. New multi-topic registration and delivery pass
  against both memory and DynamoDB Local.
- Account quota overrides and deployed Apollo reporting environment keys
  remain uninspected. No deployment/account inspection was performed.

The two apps and the old fork were read only. Actual app migration,
deployed edge authentication, CDK deployment, and a real
browser/managed API Gateway smoke test remain integration work; local tests do
not establish deployed-network parity.
