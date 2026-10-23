# @model-ts/graphql-lambda

Effect-native GraphQL transport for API Gateway HTTP API or Lambda function URL
with payload format 2.0, and API Gateway WebSockets, with an independent executor
interface. Requires Effect
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
`onInit` explicitly maps accepted init parameters into persisted context.

```ts
import { createBuilder, executor, ErrorReporter } from "@model-ts/graphql"
import {
  Gateway,
  PubSub,
  Topic,
  createHttpHandler,
  createWebSocketHandler,
  type ContextRequest,
} from "@model-ts/graphql-lambda"
import { Context, Effect, Layer, ManagedRuntime, Schema, Stream } from "effect"

interface AppContext {
  readonly requestId: string
  readonly pubSub: PubSub.Source
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
  persistedContext: Schema.Struct({}),
  executor: executor(schema),
  context: (request: ContextRequest<LambdaContext>) =>
    Effect.succeed({
      requestId: request.lambdaContext.awsRequestId,
      pubSub: request.pubSub,
    }),
}
const http = createHttpHandler(options)
const websocket = createWebSocketHandler(options)
const delivery = PubSub.DynamoDB.streamHandler(options)

const runtime = ManagedRuntime.make(
  Layer.mergeAll(
    PubSub.DynamoDB.layer({
      tableName: process.env.GRAPHQL_TABLE_NAME!,
    }),
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

// DynamoDB stream Lambda for events written with PubSub.publish().
export const streamHandler = (
  event: Parameters<typeof delivery>[0],
  context: LambdaContext,
) => runtime.runPromise(delivery(event, context))

// Call PubSub.publish from wherever your application publishes.
export const announce = (message: string) =>
  runtime.runPromise(PubSub.publish(announcements, message))
```

The runtime lives outside Lambda exports and retains all application services.
Resolver requirements such as `Greeting`, and context/authentication Effect
requirements, flow into handler types. Missing layers fail compilation. Only
the tests depend on `@model-ts/graphql`; applications can still supply a plain
graphql-js schema via `graphqlExecutor`.

The builder's subscription must use `context.pubSub.stream` (or the equivalent
finite iterable source), so registration can discover topics and finish before
Lambda returns. Stream delivery recreates this source with
one event and rerun the builder's filtering and authorization. Tests run this
wiring with graphql-js 16 on memory and DynamoDB Local.

## Handlers and executor

`createHttpHandler`, `createWebSocketHandler` and `PubSub.DynamoDB.streamHandler` return
functions `(event, lambdaContext) => Effect`. Run them in the application's
shared `ManagedRuntime`; do not build a runtime per request. Middleware can
continue adding typed data to `lambdaContext` and inspecting the proxy response.
No AWS client is constructed until its layer is built.

```ts
import {
  Gateway,
  PubSub,
  createHttpHandler,
  createWebSocketHandler,
  graphqlExecutor,
} from "@model-ts/graphql-lambda"
import { Layer, ManagedRuntime } from "effect"
import {
  schema,
  persistedContext,
  makeContext,
  makeHttpContext,
  authenticateInit,
} from "./api.ts"
import type { AppContext, LambdaContext } from "./context.ts"

const runtime = ManagedRuntime.make(
  Layer.merge(
    PubSub.DynamoDB.layer({
      tableName: process.env.GRAPHQL_TABLE_NAME!,
    }),
    Gateway.layer(),
  ),
)
const options = {
  persistedContext,
  executor: graphqlExecutor<AppContext>(schema),
  context: makeContext,
  onInit: authenticateInit,
  introspection: true,
}
const http = createHttpHandler({ ...options, context: makeHttpContext })
const websocket = createWebSocketHandler(options)
const delivery = PubSub.DynamoDB.streamHandler(options)

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
registration and delivery, so builder public codes survive.

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

- `event`: original HTTP event, WebSocket event, or DynamoDB stream record.
- `lambdaContext`: the invocation's typed middleware context.
- `persisted`: decoded `P` for WebSockets and delivery; HTTP supplies an empty object.
- `connection`: current connection on WebSocket and delivery paths.
- `pubSub`: an operation-local source, described below.
- `response.headers` and `response.cookies`: mutable HTTP response output.

The application explicitly incorporates `persisted` into its typed context.
HTTP batches share one context factory call and response container; operations
execute in order. HTTP input cookies are available as `event.cookies` after
narrowing the event with `"version" in event`. Join them with `"; "` when a
Cookie header value is needed. Append each outgoing Set-Cookie value to
`response.cookies`; the handler returns them in the payload's `cookies` array.

### HTTP

Use an HTTP API or function URL with **payload format 2.0**. The package defines
`HttpEvent` and `HttpResponse` without an `aws-lambda` types dependency.
`HttpEvent` requires `version: "2.0"`, `routeKey`, `rawPath`, `rawQueryString`,
`headers`, `requestContext.http` (`method`, `path`, `sourceIp`, `userAgent`) and
`isBase64Encoded`. `body`, `cookies` and `queryStringParameters` are optional.
API Gateway supplies lowercase, single-valued headers with comma-joined
duplicates; cookies arrive separately in `event.cookies`. The original event
reaches the context factory unchanged. Responses contain `statusCode`,
`headers`, `body`, `isBase64Encoded: false` and optional `cookies`, one
Set-Cookie value per array entry.

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

Set `graphiql: true` to serve GraphiQL on GET requests that accept `text/html`.
It is off by default. **Introspection must be enabled for GraphiQL to be useful**;
enabling the page does not override `introspection: false`. Configure it with
`GraphiQLOptions` when defaults are insufficient:

```ts
const http = createHttpHandler({
  ...options,
  introspection: true,
  graphiql: {
    credentials: "same-origin",
    endpoint: "/graphql",
    subscriptionUrl: "wss://api.example.test/graphql",
    headers: { "x-example-client": "graphiql" },
    query: "{ __typename }",
    title: "Example GraphQL API",
  },
})
```

`credentials` accepts `"include"`, `"same-origin"` (default), or `"omit"` and
applies to all HTTP fetches, including introspection. `endpoint` defaults to the
request's `rawPath`; set it explicitly when an API mapping or proxy adds an
external path prefix. Optional `subscriptionUrl` uses the `graphql-ws` client
with the `graphql-transport-ws` protocol. Default `headers` apply to HTTP
requests and are sent as WebSocket `connection_init` parameters; the application
must authenticate those parameters. `query` initializes the editor and `title`
defaults to `"GraphiQL"`. Header persistence is disabled initially. Do not embed
server-only credentials in a page delivered to browsers.

The page loads GraphiQL 5.4.0 and pinned ESM entry points from esm.sh, following
GraphiQL's [official CDN example](https://github.com/graphql/graphiql/blob/main/examples/graphiql-cdn/index.html)
and [fetcher API](https://github.com/graphql/graphiql/blob/main/packages/graphiql-toolkit/docs/create-fetcher.md).
It includes the Monaco worker setup and passes an explicit `graphql-ws` client
to `createGraphiQLFetcher`. Options are escaped for HTML and script contexts.
Optional `nonce` applies to the inline scripts and style. The application owns
CSP headers and must allow the CDN assets, Monaco workers (including blob
workers), and configured HTTP/WebSocket endpoints; a nonce alone does not
supply that policy.

An optional `healthCheck` Effect enables `/.well-known/apollo/server-health`;
route that path explicitly and send a JSON content type or accepted preflight
header when checking it. OPTIONS and GraphiQL HTML responses do not invoke
application context or resolvers. CORS still applies to the page; executing
operations from it follows the normal CSRF and validation checks. Warmers
remain application middleware.

### WebSocket authentication and protocol

The [graphql-transport-ws protocol](https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md)
is required. `$connect` rejects clients that do not offer `graphql-transport-ws`
and echoes that exact token in `Sec-WebSocket-Protocol`. Header names are
case-insensitive; subprotocol tokens are case-sensitive.

`persistedContext` is a required `Schema.Codec<P, JsonEncoded>` whose encoded
side must be JSON. For example, `Schema.Struct({ user: Schema.String,
since: Schema.DateFromString })` exposes a `Date` to application code and stores
its string encoding. A schema that encodes to a `Date`, `Map`, function or
`unknown` fails compilation. Schema service requirements flow through the handler
requirements: WebSockets require decoding and encoding services; delivery needs
decoding services only.

`onConnect(connection, event, lambdaContext)` runs during the `$connect`
handshake. It can return a decoded `P`, accept without context by returning
`void`, or reject with `false` / `ConnectionRejected`. Origin and cookie
authentication belongs here. Produced values are encoded before storage.

`connection_init` atomically claims initialization and calls
`onInit(payload, persisted, connection, event, lambdaContext)`. Its second
argument is the decoded handshake value, or `undefined` when absent. It returns
the final decoded `P` or rejects with `false` / `ConnectionRejected`. Its result
**replaces** handshake context; there is no implicit merge. Init parameters are
stored separately in `connection.initParams` and are never promoted automatically.
A successful init encodes the final value, stores it and sends `connection_ack`.
`acknowledgementPayload` optionally adds public JSON to that frame; hook return
values are never echoed.

Without `onInit`, initialization retains the decoded handshake value. If no
hook produced context, it decodes `{}` through `persistedContext`, then encodes
and stores that result. A public API can use `Schema.Struct({})` and no hooks.
A schema requiring identity fields rejects that empty default. A subscribe
before acknowledgement, or with an initialized row missing context, closes as
unauthorized without invoking application context or resolvers.

Every WebSocket read and delivery decodes stored JSON before using it as
`ContextRequest<L, P>.persisted`. Encode/decode failures are typed
`PersistedContextError` values. During handshake/init or another WebSocket
message they are logged and close with internal-error intent. During delivery,
a value that no longer decodes is logged and its connection and operations are
removed, like Gone cleanup; the stream batch continues to other subscribers
without retrying that invalid value. Other hook failures are logged and close
with internal-error intent. HTTP has no connection context; supply a separate
HTTP context factory when it differs from the typed WebSocket factory.

Clients wait for `connection_ack`, then send `subscribe { id, payload }` with
a GraphQL request. Subscriptions register durable topic membership. Queries and
mutations send `next { id, payload }` followed by `complete { id }`. Parsing,
validation, subscription-root and registration failures send
`error { id, payload: GraphQLFormattedError[] }`, which terminates that operation
without a subsequent `complete`. Execution results, including field errors,
use `next`. Client `complete { id }` cancels pending or active operations without
an acknowledgement; unknown IDs are ignored. IDs may be reused after completion.
An active duplicate ID closes the entire connection.

`ping` receives `pong`, echoing its optional payload, including `null`. Client
`pong` is ignored. Both are valid before initialization. An optional application
scheduler can call `keepAlive(connectionId)` to send `ping` for initialized
connections. No timers or connection scans run inside Lambda.

API Gateway's `DeleteConnection` accepts neither a close code nor a reason,
and `$disconnect` runs after the socket has closed. The transport records the
intended code and reason on the `GraphQL.close` span, removes connection and
operation state, and calls `DeleteConnection`. Clients receive API Gateway's
close behavior rather than these application codes; no substitute protocol
frames are sent.

| Intended code | Condition                        | API Gateway behavior                                         |
| ------------- | -------------------------------- | ------------------------------------------------------------ |
| 4400          | Invalid message or message size  | Cleanup and DeleteConnection                                 |
| 4401          | Subscribe before acknowledgement | Cleanup and DeleteConnection                                 |
| 4403          | Authentication rejection         | Reject handshake or cleanup and DeleteConnection             |
| 4406          | Subprotocol not offered          | Reject handshake, cleanup and DeleteConnection               |
| 4409          | Duplicate active operation ID    | Cleanup and DeleteConnection                                 |
| 4429          | Repeated or concurrent init      | Cleanup and DeleteConnection                                 |
| 4500          | Internal failure                 | Cleanup and DeleteConnection                                 |
| 4408          | Init timeout                     | No Lambda timer; enforce init ordering on the next subscribe |

Every operation reserves a generation before application work. Completion removes
pending and active operations; activation checks that generation. A cancelled
setup cannot resurrect itself or overwrite a later reuse of its ID. The default
limit is 100 operations per connection (`maxOperations`), enforced transactionally
across concurrent invocations. Each subscription supports 1–20 unique topics.
Topic names are application-owned, including tenant prefixes.

Decoded messages and queries default to a 64 KiB limit (`maxQueryBytes`). Encoded
outbound messages are limited to 32 KiB UTF-8 bytes, raising `MessageTooLarge`
before a send. Connection lifetime defaults to two hours and cannot exceed it.
`$disconnect` and Gone cleanup also remove operations when the connection row
has already expired through DynamoDB TTL.

Connection IDs, operation IDs, event IDs and topics use distinct Schema brands:
`ConnectionId`, `OperationId`, `EventId`, and `Topic`. Decode them at external
boundaries or use their `.make(string)` constructors for application-created
values. Their wire and stored representations remain strings. Handler factories
infer the union of requirements from individual callbacks; options have only
the application context, Lambda context, persisted context, error and service
requirement type parameters.

## Publishing, filtering and delivery

`PubSub` is the backend-agnostic namespace. `PubSub.Publisher` is a
`Context.Service`; `PubSub.publish(topic, payload, options?)` requires only
that service and returns the published `EventId`. Payloads are `Schema.Json`.
`options.ttlSeconds` defaults to 7200 and must be positive. Invalid topics,
TTLs and non-serializable runtime inputs fail before any event is written.
Call `PubSub.publish` from wherever your application publishes.

```ts
import { PubSub, Topic } from "@model-ts/graphql-lambda"

const publication = PubSub.publish(
  Topic.make("public/announcements"),
  { message: "Hello" },
  { ttlSeconds: 3600 },
)

// A graphql-js subscription resolver:
subscribe: (_parent, args, context) =>
  PubSub.withFilter(
    context.pubSub.subscribe(Topic.make("public/announcements")),
    (payload) => EventPayload.decode(payload).category === args.category,
  )
```

Resolvers receive `PubSub.Source` on `ContextRequest.pubSub`. Its `stream(topics)`
is Effect-native; `subscribe(topics)` returns an async iterator. Both collect
topics during registration and produce one matching event during delivery.
`withFilter` requires a strictly true predicate result and forwards cancellation
to its source iterator. Decode JSON payloads before reading application fields.

`PubSub.DynamoDB.layer({ tableName, client? })` supplies both `Publisher` and
`Storage` using a dedicated transport table. Publication writes a
`GraphQLPublishedEvent`; the table's `NEW_IMAGE` stream triggers
`PubSub.DynamoDB.streamHandler(options)`. Publishing completes after persistence;
delivery runs in the stream Lambda.

The handler only processes event `INSERT` records; other model writes and TTL
deletes are ignored. Expired events, connections and memberships are hidden
before TTL deletion. Delivery uses consistent reads and 50-row topic pages,
reconstructs current application context, validates the saved request, and
reruns subscription filters and authorization. Subscribers run sequentially to
keep invocation-scoped context isolated. A generation check before sending
discards results from operations completed or reused during context execution.

Gone (HTTP 410) removes the connection and all its operations and memberships.
Other infrastructure failures retain retry behavior: the handler attempts the
remaining subscribers and returns failed records' DynamoDB **sequence numbers**
in `batchItemFailures`. Enable `ReportBatchItemFailures` and configure retry
count, maximum record age and an on-failure destination. The stream Lambda must
have enough duration for one event's fan-out. Successful subscribers can receive
duplicates on retry; clients should handle events idempotently. There is no
replay on reconnect or exactly-once guarantee.

Context and resolver failures become formatted `next` results. Context defects
and throwing formatters are logged and produce a fixed safe
`INTERNAL_SERVER_ERROR` result for that subscriber. Storage reads and gateway
sends retain infrastructure retry behavior. Interruption remains interruption
and awaits context cleanup.

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
unchanged. WebSocket authentication rejections close the connection;
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

WebSocket handlers, keep-alive and stream delivery require `PubSub.Storage`.
Its contract owns connection creation, atomic init claims, updates and live
lookup; generation reservations, activation and limits; completion, disconnect
cleanup and paged subscriber streams. Backend failures use `StorageError`;
invalid registrations and missing/expired connections use `TransportError`.
Implementations must preserve consistency, generation checks and TTL visibility.

`PubSub.DynamoDB.layer` captures a private DynamoDB service and provides it
internally to every transport model operation, including publication. It never
consumes or exports the application's `DynamoDB` service. Application model
layers can be composed alongside it without changing either table's ownership.
Use `layer({ tableName, client? })` for production options or
`layerFrom(tableLayer)` for an explicitly supplied DynamoDB layer. Both export
only `Publisher` and `Storage`; no auxiliary service from the table layer escapes.
Transport pagination uses internal keys and exposes no cursors.

For tests and local development, use the PubSub-specific sandbox constructors:

```ts
import { Sandbox } from "@model-ts/dynamodb/Sandbox"

const layers = Layer.merge(
  Sandbox.layerMemory(), // Application DynamoDB + application Sandbox.
  PubSub.DynamoDB.layerMemory(), // Publisher + Storage + PubSub.DynamoDB.Sandbox.
)

const inspect = Effect.gen(function* () {
  const app = yield* Sandbox
  const transport = yield* PubSub.DynamoDB.Sandbox
  return { app: yield* app.snapshot, pubSub: yield* transport.snapshot }
})
```

`PubSub.DynamoDB.Sandbox` has a distinct service tag and wraps the table sandbox's
`seed`, `get`, `snapshot`, and `diff` operations. It cannot replace the application's
Sandbox when layers are merged, in either order. `layerLocal(options?)` provides
the same PubSub services using an isolated DynamoDB Local table; it does not
start or stop the local server. `layerMemory(options?)` uses an in-process table.

## Storage layout

Four model roles share the dedicated PubSub table. Provision string `PK` and
`SK` primary keys, no secondary indexes, TTL attribute **`ttl`**, and a
**`NEW_IMAGE`** stream. Connection/topic/operation components use JSON encoding
to avoid delimiter collisions.

| Model tag                    | PK                             | SK                                 | Contents                                                                         |
| ---------------------------- | ------------------------------ | ---------------------------------- | -------------------------------------------------------------------------------- |
| GraphQLConnection            | `GRAPHQL#CONNECTION#<JSON id>` | `CONNECTION`                       | endpoint, encoded context, initParams, initReceived, initialized, createdAt, ttl |
| GraphQLSubscriptionOperation | same connection PK             | `OPERATION#<JSON id>`              | connectionId, operationId, generation, state, request, topics, ttl               |
| GraphQLSubscription          | `GRAPHQL#TOPIC#<JSON topic>`   | JSON `[connectionId, operationId]` | membership and ttl                                                               |
| GraphQLPublishedEvent        | `GRAPHQL#EVENT#<UUID>`         | `EVENT`                            | topic, JSON payload text, ttl                                                    |

All models also carry `_tag` and `_docVersion`. Connection TTL is fixed at
connect time, defaults to two hours and cannot exceed two hours. Operations
and memberships inherit that exact TTL; activity and keep-alive never extend
it beyond API Gateway's maximum connection duration. Events have their own
publish-time TTL. Disconnect queries the connection partition rather than
scanning subscriptions. Reservation, activation and completion use atomic
transactions. Grant the WebSocket and stream Lambdas POST/DELETE
ManageConnections permissions. Management endpoints derive from API Gateway's
`domainName` and `stage`.
Configure the HTTP API or function URL integration with payload format 2.0.

## Testing and remaining evidence gaps

Tests use `@effect/vitest`, `PubSub.DynamoDB.layerMemory` and `PubSub.DynamoDB.layerLocal`,
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

A browser and managed API Gateway smoke test remains integration work; local
tests do not establish deployed network behavior.
