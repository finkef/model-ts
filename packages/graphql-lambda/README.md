# @model-ts/graphql-lambda

An Effect v4 GraphQL transport for AWS Lambda. It serves queries and mutations
through API Gateway HTTP APIs or Lambda function URLs using payload format 2.0,
and queries, mutations and durable subscriptions through API Gateway WebSocket
APIs using `graphql-transport-ws`. `PubSub.DynamoDB` stores connection state,
subscriptions and published events in a dedicated table; a DynamoDB Streams
Lambda delivers events by rebuilding each subscriber's context and executing
its subscription.

## Requirements

- Node.js 22 or later and Effect `^4.0.5`.
- `@model-ts/core` and `@model-ts/dynamodb` `^7.0.0`.
- `graphql` `^16.11.0`, with one shared installation for schema, executor and
  transport.
- AWS SDK v3 peers: `@aws-sdk/client-apigatewaymanagementapi` and
  `@aws-sdk/util-dynamodb`.
- `@model-ts/graphql` when using its Effect schema builder.

This package is versioned independently: `@model-ts/graphql-lambda` uses major
version 6 with model-ts core and DynamoDB major version 7.

## Quick start

This example exposes a public greeting and an announcement subscription. Set
`GRAPHQL_TABLE_NAME` to a dedicated table configured as described under
[PubSub.DynamoDB](#pubsubdynamodb). All handlers use one runtime created outside
the Lambda exports.

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
import { Effect, Layer, ManagedRuntime, Schema, Stream } from "effect"

interface LambdaContext {
  readonly awsRequestId: string
}
interface AppContext {
  readonly requestId: string
  readonly pubSub: PubSub.Source
}

const announcements = Topic.make("public/announcements")
const GraphQL = createBuilder<{}, AppContext>()
const greeting = GraphQL.queryField("greeting", "String", (_, __, context) =>
  Effect.succeed(`Hello from ${context.requestId}`),
)
const announcement = GraphQL.subscriptionField("announcement", "String", {
  subscribe: (_, __, context) =>
    context.pubSub
      .stream(announcements)
      .pipe(
        Stream.map((payload) =>
          Schema.decodeUnknownSync(Schema.String)(payload),
        ),
      ),
})
const schema = GraphQL.schema({ greeting, announcement })
const options = {
  executor: executor(schema),
  persistedContext: Schema.Struct({}),
  context: (request: ContextRequest<LambdaContext>) =>
    Effect.succeed({
      requestId: request.lambdaContext.awsRequestId,
      pubSub: request.pubSub,
    }),
}
const http = createHttpHandler(options)
const websocket = createWebSocketHandler(options)
const delivery = PubSub.DynamoDB.streamHandler(options)
const errorReporting = Layer.succeed(ErrorReporter, {
  report: ({ cause, coordinate }) => Effect.logError(coordinate, cause),
})
const runtime = ManagedRuntime.make(
  Layer.mergeAll(
    PubSub.DynamoDB.layer({ tableName: process.env.GRAPHQL_TABLE_NAME! }),
    Gateway.layer(),
    errorReporting,
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

export const streamHandler = (
  event: Parameters<typeof delivery>[0],
  context: LambdaContext,
) => runtime.runPromise(delivery(event, context))

export const announce = (message: string) =>
  runtime.runPromise(PubSub.publish(announcements, message))
```

Route HTTP requests to `httpHandler`, WebSocket `$connect`, `$default` and
`$disconnect` to `wsHandler`, and the table's stream to `streamHandler`. Protocol
messages must reach `$default`. A client subscribes with
`subscription { announcement }`; calling `announce("Hello")` writes an event
for asynchronous delivery.

Each factory returns `(event, lambdaContext) => Effect`. Handler types retain
service requirements from the executor, context factory, codecs and relevant
hooks. Add application service layers to the runtime; missing requirements are
type errors. A publisher in another Lambda can use `PubSub.DynamoDB.layer` with
the same table name and call `PubSub.publish` without constructing a handler.

The remaining TypeScript examples extend this module and reuse its imports and
definitions.

## HTTP

### Requests and responses

Configure the HTTP API integration for payload format `2.0`; Lambda function
URLs use the same event format. `HttpEvent` includes `version`, `routeKey`,
`rawPath`, `rawQueryString`, `headers`, `isBase64Encoded` and
`requestContext.http`, plus optional `body`, `cookies` and
`queryStringParameters`. Other payload versions return 400.

POST accepts a JSON object with `query` and optional `variables`, `operationName`
and `extensions`. Variables and extensions are JSON objects or `null`.
Base64-encoded request bodies are decoded before parsing and size checks.

A POST array executes a batch sequentially. Its context factory runs at most
once, when an operation first needs context; all entries share the resulting
context and response headers/cookies. Invalid entries produce individual
errors. An accepted batch returns HTTP 200 with an array of results, even when
individual operations fail. Empty or oversized batches return 400.

GET reads those same fields from `queryStringParameters`; encode `variables`
and `extensions` as JSON strings. GET supports queries only. Mutations and
subscriptions over GET return 405 with `Allow: POST`. POST subscriptions return 400. Unsupported methods return 405 with `Allow: GET, POST, OPTIONS`.

For a single operation, a result with data returns 200, including partial data
with field errors. A result containing only errors returns 400. A failed context
Effect containing a `TransportError` uses its status; other unexpected failures
return 500. Responses contain a JSON body and `isBase64Encoded: false`.

### CORS and CSRF

CORS is disabled by default. Set `cors.origin` to an explicit origin or an array
of allowed origins. Without credentials, `true` reflects the request origin and
`"*"` allows any origin. With `credentials: true`, explicit origins are required;
reflection and `"*"` are rejected. CORS controls browser access to responses and
does not authenticate requests or prevent execution for disallowed origins.

OPTIONS returns 204 without creating context. With CORS enabled, responses
advertise `GET, POST, OPTIONS`, echo `Access-Control-Request-Headers` and set
`Vary: Origin, Access-Control-Request-Headers`.

GraphQL requests, including GET, must send `Content-Type: application/json`.
Browsers preflight that content type, so a cross-site page cannot send a request
the handler accepts. To also accept requests without it, for example GET
queries from a client that sends no content type, list custom headers in
`csrfHeaders`, such as `csrfHeaders: ["x-graphql-client"]`. A request then
passes when one of them is non-empty. Names are matched case-insensitively and
must be `x-*` headers, because safelisted headers do not trigger a preflight.

### Limits

| Option            | Default    | Applies to                                         |
| ----------------- | ---------- | -------------------------------------------------- |
| `maxBodyBytes`    | 1 MiB      | Decoded POST body, measured in UTF-8 bytes         |
| `maxBatchSize`    | 20         | Number of operations in a POST batch               |
| `maxQueryBytes`   | 64 KiB     | Each GraphQL query string, measured in UTF-8 bytes |
| `introspection`   | Enabled    | Set `false` to reject schema introspection         |
| `validationRules` | None added | Additional GraphQL validation rules                |

There is no built-in depth limit; supply a validation rule when needed. The
query limit, introspection setting and validation rules also apply to WebSocket
registration and stream delivery.

### Cookies and response headers

Read HTTP cookies from `event.cookies`, narrowing the event first with
`"version" in event`. Join them with `"; "` if your cookie parser expects a
Cookie header. WebSocket handshake cookies are in the event's headers.

Append each outgoing Set-Cookie value separately to `response.cookies`; the
HTTP handler returns the payload format 2.0 `cookies` array. Mutate
`response.headers` for other headers. These containers are isolated per HTTP
invocation and shared by its batch. Response mutations during WebSocket
operations or delivery do not set browser cookies.

```ts
export const httpWithCookies = createHttpHandler({
  ...options,
  context: (request: ContextRequest<LambdaContext>) =>
    Effect.gen(function* () {
      const cookieHeader =
        "version" in request.event
          ? (request.event.cookies ?? []).join("; ")
          : ""
      request.response.headers["cache-control"] = "no-store"
      if (!cookieHeader) {
        request.response.cookies.push(
          "theme=light; Path=/; Secure; SameSite=Lax",
        )
      }
      return yield* options.context(request)
    }),
})
```

## GraphiQL

Set `graphiql: true` or provide options to serve GraphiQL on GET requests that
explicitly accept `text/html` with nonzero quality. GraphiQL is disabled by
default. HTML rendering happens before GraphQL CSRF checks and context creation;
protect access at the integration if needed. Queries sent by the UI still pass
through the normal HTTP checks.

```ts
export const httpWithGraphiQL = createHttpHandler({
  ...options,
  graphiql: {
    endpoint: "/graphql",
    subscriptionUrl: "wss://api.example.test/ws",
    credentials: "same-origin",
    headers: { "x-client-name": "graphiql" },
    query: "query { greeting }",
    title: "Example API",
  },
})
```

`endpoint` defaults to the request's `rawPath`; `credentials` defaults to
`"same-origin"` and also accepts `"include"` or `"omit"`. `subscriptionUrl`
enables a `graphql-ws` client and must use `wss://` or `ws://`.

Configured `headers` are HTTP defaults and WebSocket `connection_init`
parameters. They are not WebSocket handshake headers, and they are embedded in
the HTML: do not configure server secrets there. Header persistence in the
editor is disabled.

The page loads browser dependencies from `esm.sh`. Optional `nonce` is applied
to inline scripts and style; the application owns CSP headers and must allow
the required scripts, styles, workers and network endpoints.

## WebSockets

### Connection lifecycle and authentication

The client must offer the case-sensitive subprotocol `graphql-transport-ws`.
The `$connect` response selects that token. A successful handshake creates an
uninitialized connection; the client sends `connection_init` and waits for
`connection_ack` before starting operations.

| Hook        | Arguments                                                | Result                                                                       |
| ----------- | -------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `onConnect` | `(connection, event, lambdaContext)`                     | Decoded persisted context, `void` to accept without it, or `false` to reject |
| `onInit`    | `(payload, persisted, connection, event, lambdaContext)` | Final decoded persisted context, or `false` to reject                        |

Both hooks return Effects. Returning `false` or failing with
`ConnectionRejected` explicitly rejects authentication. Other failures,
including a failed credential lookup, are internal errors and are logged.
Validate origins and handshake credentials in `onConnect` or a `$connect`
authorizer.

`persistedContext` is required and must be an Effect `Schema.Codec` whose
encoded representation is JSON. Its decoded type is the application's
persisted context type. For example, a codec can expose a `Date` while storing
a string. Values produced by hooks are encoded before storage and decoded
before use on subsequent messages and deliveries.

`onInit` receives the decoded `onConnect` value, or `undefined` if absent. Its
result replaces that value; there is no implicit merge. Init parameters are
stored separately as `connection.initParams` and are never automatically trusted
as context. `acknowledgementPayload` optionally adds public JSON to
`connection_ack`; hook results are not echoed.

Without `onInit`, initialization retains the handshake context. If none was
provided, it decodes `{}` through `persistedContext` and stores the result. Use
`Schema.Struct({})` for a public API with no hooks, as in the quick start. A codec
with required identity fields needs a hook to supply those fields.

### Rebuilding application context

The context factory runs for each accepted `subscribe` message, including
queries and mutations, and again for each subscriber on every delivered event.
It does not run for `connection_init`, `ping`, `pong` or `complete`. Stored JSON
is decoded on `$default` messages before processing their protocol action.

| Context field   | Meaning                                                                     |
| --------------- | --------------------------------------------------------------------------- |
| `event`         | HTTP event, WebSocket message event, or individual DynamoDB stream record   |
| `lambdaContext` | Context of the current Lambda invocation, including typed middleware fields |
| `persisted`     | Decoded connection context for WebSockets/delivery; `{}` for HTTP           |
| `connection`    | Current connection for WebSocket operations and delivery                    |
| `pubSub`        | Source for the current operation or delivered event                         |
| `response`      | Mutable headers and cookies for HTTP responses                              |

Persist stable identity references and load current permissions in the context
factory. Delivery has the stream invocation's context and record, not the
original handshake event. If HTTP authentication needs a different context
shape, give `createHttpHandler` a separate context factory.

Codec failures during WebSocket handling close with internal-error intent.
During delivery, an undecodable persisted value is logged and its connection
state is removed; that value is not retried.

### Operations, limits and lifetime

Queries and mutations send `next`, then `complete`. Subscription registration
stores the request and topic membership for later deliveries. Request or
registration errors send an operation `error` array and terminate that operation
without a following `complete`. Results with data, including field errors, use
`next`.

Client `complete` cancels a pending or active operation without an acknowledgement;
unknown IDs are ignored. IDs can be reused after completion. Reusing an ID that
is still pending or active closes the connection. Reservations and activation
use generations so a cancelled setup cannot restore a completed operation.

| Limit                   | Default or fixed value                                                      |
| ----------------------- | --------------------------------------------------------------------------- |
| `maxOperations`         | 100 pending/active operations per connection, enforced transactionally      |
| Topics per subscription | 1–20 unique, non-empty topics                                               |
| Operation ID            | 1–128 characters                                                            |
| `maxQueryBytes`         | 64 KiB for the decoded incoming message and separately for the query string |
| Outbound message        | 32 KiB of UTF-8 JSON, including the protocol envelope                       |
| `ttlSeconds`            | 7200 seconds; must be positive and at most 7200                             |

An oversized outbound message raises `MessageTooLarge` before sending. During
message handling it closes the connection with internal-error intent; during
stream delivery it participates in record retry handling.

Connection TTL is fixed at connect time. Operations and topic memberships
inherit it; activity does not extend it. Expired state is rejected even before
DynamoDB TTL physically removes rows. `$disconnect` and a 410 Gone response
remove connection and operation state, including memberships.

`ping` receives `pong` with the same optional payload; client `pong` is ignored.
Both are accepted before initialization. An external scheduler can call
`keepAlive(ConnectionId.make(id))` for known connections; it sends `ping` only
when initialized. There is no built-in timer or connection scan. API Gateway
also imposes a two-hour connection duration and a ten-minute idle timeout. See
[AWS WebSocket quotas](https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-execution-service-websocket-limits-table.html).

## API Gateway deviations

API Gateway's `DeleteConnection` cannot specify a protocol close code or reason.
The handler records the intended code and reason in logs and on the
`GraphQL.close` span (`graphql.close.code`, `graphql.close.reason`), removes
connection state and calls `DeleteConnection`. No substitute protocol frame is
sent. A Lambda status returned after the handshake is not a client-visible
WebSocket close code.

| Protocol close code | Condition                                                      | What happens                                                                                 |
| ------------------- | -------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| 4400                | Invalid message or incoming message too large                  | Cleanup and `DeleteConnection`; Lambda returns 400                                           |
| 4401                | Subscribe before initialization, missing or expired connection | Cleanup and `DeleteConnection`; Lambda returns 401                                           |
| 4403                | Explicit authentication rejection                              | `$connect` fails with 401, or an established socket is cleaned up and deleted                |
| 4406                | Required subprotocol absent                                    | `$connect` fails with 400; cleanup is attempted                                              |
| 4409                | Duplicate pending or active operation ID                       | Cleanup and `DeleteConnection`; Lambda returns 400                                           |
| 4429                | Repeated or concurrent initialization                          | Cleanup and `DeleteConnection`; Lambda returns 400                                           |
| 4500                | Internal failure                                               | Logged, cleanup and `DeleteConnection`; Lambda returns 500                                   |
| 4408                | Initialization timeout                                         | No server timer implements this close; a subscribe before initialization is rejected as 4401 |

Reject invalid credentials during `$connect`, using cookies or a short-lived
query token validated by an authorizer or handshake authentication. This makes
the handshake fail before the socket is established. Configure the
`graphql-ws` client's `shouldRetry` policy and a finite `retryAttempts` for
failed connections.

A rejected `connection_init` instead deletes an already-open socket. If that
arrives as a normal close (1000), `graphql-ws` immediately reconnects while
subscriptions are pending. Its normal-close branch runs before `shouldRetry`
and `retryAttempts`, so those options alone do not stop this loop. If init-time
rejection is necessary, track acknowledgement on the client and dispose the
client or cancel pending subscriptions after a pre-acknowledgement close before
explicitly reconnecting with new credentials. See the
[graphql-ws client implementation](https://github.com/enisdenjo/graphql-ws/blob/master/src/client.ts).

There is no `connectionInitWaitTimeout` option. Use client acknowledgement
timeouts and external enforcement if an initialization deadline is required;
Lambda does not remain running to wait for the next message.

## PubSub

### Publishing and subscription sources

Create a branded `Topic` with `Topic.make("public/announcements")`. Topic names
are application-owned; use tenant or resource prefixes where needed and
authorize access in application code.

`PubSub.publish(topic, payload, { ttlSeconds? })` returns an Effect requiring
`PubSub.Publisher`. Call it from resolvers, jobs or any other application code
with that service available. With `PubSub.DynamoDB`, it writes one event and
returns its `EventId`; it does not wait for subscribers. Payloads must be JSON.
Event TTL defaults to 7200 seconds and can be set to any positive finite value.
This is independent of the connection TTL.

Put the provided `PubSub.Source` on application context. Its two source methods
accept one topic or an array of topics:

| Method                                 | Result                               | Use                                                        |
| -------------------------------------- | ------------------------------------ | ---------------------------------------------------------- |
| `stream(topics)`                       | `Stream<Schema.Json>`                | Effect subscription resolvers                              |
| `subscribe(topics)`                    | `AsyncIterableIterator<Schema.Json>` | graphql-js subscription resolvers                          |
| `PubSub.withFilter(source, predicate)` | Async generator                      | Filter an async source with a boolean or Promise predicate |

```ts
export const nonEmptyAnnouncements = (context: AppContext) =>
  context.pubSub.stream(announcements).pipe(
    Stream.map((payload) => Schema.decodeUnknownSync(Schema.String)(payload)),
    Stream.filter((message) => message.length > 0),
  )

export const filteredAnnouncementIterator = (context: AppContext) =>
  PubSub.withFilter(
    context.pubSub.subscribe(announcements),
    (payload) => typeof payload === "string" && payload.length > 0,
  )
```

`withFilter` accepts only a strict `true` result and forwards iterator cleanup
on completion or failure. Validate payload shape in the subscription resolver;
`Topic` does not specify a payload schema.

During registration, sources are finite and empty. Running the subscription
collects its topics and closes its resources. During delivery, a matching
source contains one event and the transport consumes at most one result. Build
subscriptions from these sources; a never-ending external source cannot be
registered durably. No subscription fiber remains alive between invocations.

### Delivery and retries

Delivery is at-least-once. Records are processed in input order and subscribers
are attempted sequentially. A failed subscriber does not prevent attempts for
the others. Successful subscribers can receive the event again on retry; include
an application event identifier in the payload when consumers need deduplication.

For every event, delivery reloads the connection and operation, rebuilds context,
validates the saved request and reruns the subscription, including authorization
and filtering. A filtered event sends nothing. Context or execution failures
become GraphQL results for that subscriber instead of retrying the event; a
permission denial therefore does not become an infrastructure retry. The
operation remains registered unless completed or cleaned up.

Expired events and memberships are ignored. A 410 Gone response triggers
connection cleanup. Storage failures and non-Gone management API failures are
collected for retry. A failed record with a `SequenceNumber` produces
`batchItemFailures: [{ itemIdentifier: sequenceNumber }]`; a failure without a
sequence number fails the handler Effect.

Configure the DynamoDB event source mapping explicitly:

| Setting                                              | Purpose                                              |
| ---------------------------------------------------- | ---------------------------------------------------- |
| `FunctionResponseTypes: ["ReportBatchItemFailures"]` | Enables the handler's partial batch response         |
| `MaximumRetryAttempts`                               | Bounds retries for persistent failures               |
| `MaximumRecordAgeInSeconds`                          | Bounds how long delivery is retried                  |
| `DestinationConfig.OnFailure`                        | Retains discarded records for inspection or recovery |
| `BatchSize` and Lambda timeout                       | Allow enough time for sequential subscriber delivery |

Lambda resumes from the lowest failed sequence number, so later successful
records can also be repeated. See
[AWS partial batch response behavior](https://docs.aws.amazon.com/lambda/latest/dg/services-ddb-batchfailurereporting.html).
Choose retry age/count and an on-failure destination together; event TTL also
limits how long the transport will attempt delivery.

## PubSub.DynamoDB

Provision a dedicated table with string partition key `PK`, string sort key
`SK`, TTL enabled on the numeric `ttl` attribute, and a stream with
`StreamViewType: NEW_IMAGE`. No secondary index is required.

`PubSub.DynamoDB.layer({ tableName, client? })` supplies `PubSub.Publisher` and
`PubSub.Storage`. It captures its own table service, so merging it with an
application's DynamoDB layer does not change which table either service uses.
`layerFrom(databaseLayer)` accepts an explicitly configured DynamoDB layer and
likewise keeps its table services private to the backend.

### Key layout

In the table below, `json(x)` means `JSON.stringify(x)`, including string quotes
and escaping. Array keys encode the entire tuple. All rows also include model-ts
`_tag` and `_docVersion` metadata.

| Row              | PK                                        | SK                                  | Stored data                                                                          |
| ---------------- | ----------------------------------------- | ----------------------------------- | ------------------------------------------------------------------------------------ |
| Connection       | `GRAPHQL#CONNECTION#<json(connectionId)>` | `CONNECTION`                        | Endpoint, encoded context, init parameters, initialization flags, creation time, TTL |
| Operation        | `GRAPHQL#CONNECTION#<json(connectionId)>` | `OPERATION#<json(operationId)>`     | GraphQL request, generation, pending/active state, topics, TTL                       |
| Topic membership | `GRAPHQL#TOPIC#<json(topic)>`             | `json([connectionId, operationId])` | Topic, connection ID, operation ID, TTL                                              |
| Published event  | `GRAPHQL#EVENT#<eventId>`                 | `EVENT`                             | Topic, JSON payload text, TTL                                                        |

Connection creation time is in milliseconds; TTL values are Unix seconds.
Disconnect queries the connection partition to remove operations and memberships.
Reservation, activation and completion use transactional writes.

The stream handler processes only `INSERT` records whose new image has
`_tag: "GraphQLPublishedEvent"`. It ignores other row types and update/delete
records. An event source filter can restrict delivery to that tag and event name.

### IAM and management endpoints

Grant WebSocket and stream Lambdas the table read/write permissions needed for
connection, operation and membership access, including queries, batch writes
and transactional writes. Publishers need permission to write event items. The
stream Lambda also needs DynamoDB Streams read permissions for its event source.

Both the WebSocket Lambda and the stream Lambda need
`execute-api:ManageConnections` for POST and DELETE on the API's `@connections`
resources. Delivery may need to delete a connection as well as post to it.
Management endpoints derive from the WebSocket event's `domainName` and `stage`
and are stored with the connection. `Gateway.layer()` caches AWS clients by
endpoint and destroys them when its layer scope closes.

## Errors and formatting

Executors own errors inside GraphQL results. The transport forwards those
formatted results unchanged. Handler `formatError` handles transport failures
such as parsing, transport validation and context failures; it does not reformat
executor output.

With `executor(schema)` from `@model-ts/graphql`, configure public errors and
codes on the builder. Its `ErrorReporter` receives internal failures; the quick
start provides a logging layer for it. With `graphqlExecutor`, pass
`{ formatError }` to that adapter to customize execution error formatting.

The default transport formatter exposes explicit `TransportError` messages and
codes, `ConnectionRejected` as `UNAUTHORIZED`, and `MessageTooLarge` as
`MESSAGE_TOO_LARGE`. Trusted GraphQL request diagnostics use
`GRAPHQL_PARSE_FAILED`, `GRAPHQL_VALIDATION_FAILED` or `BAD_USER_INPUT`.
Unexpected errors are masked as `Internal server error` with
`INTERNAL_SERVER_ERROR`; setting arbitrary `GraphQLError.extensions` does not
make a failure public.

Use `TransportError({ message, code, status })` for an intentionally public
transport/context failure. Its status applies when it escapes to the HTTP
handler; errors already inside an executor result follow the result's HTTP
status rules. In authentication hooks, use `false` or `ConnectionRejected` to
reject credentials explicitly.

An `ErrorFormatter` receives a `GraphQLError`, including `originalError` where
available, and returns a `GraphQLFormattedError`. Report internal causes while
keeping returned details safe. Custom validation-rule failures are treated as
application failures. Formatter or serialization defects have masked fallbacks;
Effect interruption is preserved.

## Testing

Import test helpers from `@model-ts/graphql-lambda/Sandbox` and local adapters
from `@model-ts/graphql-lambda/Local`. Production entry points do not import or
re-export these modules, so production bundles do not load the sandbox engine,
local HTTP server, or memory Gateway.

`Sandbox.layerMemory()` runs without AWS. `Sandbox.layerLocal()` uses a running
DynamoDB Local instance and manages an isolated table for the layer's scope.
Both expose `Sandbox` for inspection, seeding and raw-row removal
with `remove(key)`; use that service for the transport table when a test also
has an application-table sandbox.

`Sandbox.layerGateway` records posts and closes without network calls.
`GatewayMessages.all` and `.closed` expose them; `.markGone(connectionId)`
simulates a 410 response on posts.

`Local.delivery` subscribes to the sandbox during layer construction and invokes
the production stream handler for subsequent writes. `Local.Delivery.settled`
waits until every write committed before the call has been attempted. This
example exercises registration through delivery using the quick start's schema:

```ts
import type { WebSocketEvent } from "@model-ts/graphql-lambda"
import * as Local from "@model-ts/graphql-lambda/Local"
import { Sandbox, GatewayMessages } from "@model-ts/graphql-lambda/Sandbox"

const socketEvent = (
  routeKey: string,
  message?: Schema.Json,
): WebSocketEvent => ({
  headers: { "sec-websocket-protocol": "graphql-transport-ws" },
  requestContext: {
    routeKey,
    connectionId: "test-connection",
    domainName: "api.example.test",
    stage: "ws",
  },
  ...(message === undefined ? {} : { body: JSON.stringify(message) }),
})

export const deliveryExample = Effect.gen(function* () {
  const invocation = { awsRequestId: "test-request" }
  yield* websocket(socketEvent("$connect"), invocation)
  yield* websocket(
    socketEvent("$default", { type: "connection_init" }),
    invocation,
  )
  yield* websocket(
    socketEvent("$default", {
      type: "subscribe",
      id: "announcement-1",
      payload: { query: "subscription { announcement }" },
    }),
    invocation,
  )

  yield* PubSub.publish(announcements, "Hello")
  yield* (yield* Local.Delivery).settled
  return yield* (yield* GatewayMessages).all
}).pipe(
  Effect.provide(
    Local.delivery(delivery).pipe(
      Layer.provideMerge(
        Layer.mergeAll(
          Sandbox.layerMemory(),
          Sandbox.layerGateway,
          errorReporting,
        ),
      ),
    ),
  ),
)
```

Run `deliveryExample` with `Effect.runPromise` or an Effect test runner. Expect
two recorded messages: `connection_ack` and `next` with `data.announcement`
equal to `"Hello"`. Replace `layerMemory()` with `layerLocal()` to exercise the
same flow against DynamoDB Local.

Delivery processes available records in commit order, in batches of at most
100 (`batchSize` changes the limit). Each batch gets a fresh Lambda context.
Failed batches and `batchItemFailures` are logged with their sequence numbers;
local delivery makes one attempt and continues. `settled` confirms attempts,
not successful delivery. Production retry configuration is unchanged.

To provide a deterministic barrier even while the consumer is busy, `settled`
writes a unique row under the reserved `GRAPHQL#LOCAL#DELIVERY#…` prefix and
waits for it to pass through the consumer. Marker INSERT and REMOVE records are
filtered before invoking the handler. The marker is deleted before `settled`
returns, keeping sandbox snapshots and diffs clean. Cleanup also runs when a
waiter is interrupted. A closed scope or stopped stream fails waiters with
`Local.DeliveryStoppedError`. Calls with no intervening writes are safe, as are
concurrent calls. Build the delivery layer before publishing; it does not replay old rows.

## Local development

`Local.layer` runs the same HTTP, WebSocket and stream handlers on an Effect HTTP
server. HTTP payloads follow API Gateway format 2.0, including cookies. WebSocket
connections use `$connect`, `$default` and `$disconnect`, the persisted-context
codec, and the production delivery and filtering logic.

Install a platform adapter in your application. With Effect `4.0.5`, use
`@effect/platform-node@4.0.5` and pin `@effect/platform-node-shared@4.0.5` as well;
the `4.0.6` adapters require Effect `^4.0.6`. This package has no platform runtime
dependency. The following server extends the quick start's schema, `options`,
and `errorReporting` application layer:

```ts
import { createServer } from "node:http"
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node"
import * as Local from "@model-ts/graphql-lambda/Local"
import { Sandbox } from "@model-ts/graphql-lambda/Sandbox"

const port = 4000
const local = Local.layer({
  http: createHttpHandler({
    ...options,
    graphiql: { subscriptionUrl: `ws://localhost:${port}/graphql` },
  }),
  webSocket: createWebSocketHandler(options),
  stream: PubSub.DynamoDB.streamHandler(options),
  keepAlive: "30 seconds",
}).pipe(
  Layer.provide(
    Layer.mergeAll(
      Sandbox.layerMemory(),
      errorReporting,
      NodeHttpServer.layer(createServer, {
        port,
        websocket: {
          handleProtocols: (protocols) =>
            protocols.has("graphql-transport-ws")
              ? "graphql-transport-ws"
              : false,
        },
      }),
    ),
  ),
)

NodeRuntime.runMain(Layer.launch(local))
```

Open `http://localhost:4000/graphql` for GraphiQL. The default `path` is
`/graphql` and the default `stage` is `local`. Both are configurable. Omit
`http`, `webSocket`, or `stream` when the application does not use that transport.
`path` is typed as an absolute path. Invalid batch sizes or keep-alive durations
are configuration defects, not layer failures. Keep-alive pings are disabled
unless `keepAlive` supplies an interval; they call
the production `keepAlive` function for each open connection.

`latency` delays HTTP requests before their handler runs, so the app's loading
states can be exercised locally. Pass a duration, or a function that returns
the delay for each request as an Effect. The function can read a `Ref` that
the app or an end-to-end test changes at runtime, and can exempt requests that
should stay fast; its services become requirements of the layer. WebSocket
messages and stream delivery are never delayed, so `settled` stays fast.

```ts
const latency = yield * Ref.make<Duration.Input>(Duration.zero)

Local.layer({
  http,
  webSocket,
  latency: (event) =>
    event.headers["x-skip-latency"]
      ? Effect.succeed(Duration.zero)
      : Ref.get(latency),
})

// Later, e.g. from a local-only control endpoint:
yield * Ref.set(latency, "5 seconds")
```

The application supplies `HttpServer.HttpServer`; Effect's generic upgrade API
does not expose subprotocol selection. Configure the platform's upgrade handler
to select `graphql-transport-ws`, as above, especially when clients offer multiple
protocols. Other Effect server adapters can be used with the same selection.

The layer provides only `Local.Delivery` (`settled` is a no-op when `stream` is
omitted). Its socket-backed `Gateway` is private to the handlers it runs, so it
does not replace the application's Gateway. It requires all other services used
by the handlers, and the PubSub sandbox when `stream` is supplied. Gateway
posts and closes ignore the persisted management endpoint. Unknown or closed
connections fail with `GatewayError` and `gone: true`. Server closes use 1000;
`DeleteConnection` does not accept an application close code, and AWS does not
document a guaranteed close code for that operation. Binary WebSocket messages
close with 1003. Disconnect cleanup runs on either peer closing and on scope
shutdown; one connection's failure is logged without stopping the server.

`Local.LambdaContext` supplies a fresh `awsRequestId`, local function/log metadata,
and a decreasing `getRemainingTimeInMillis` with a fifteen-minute budget. This is
metadata, not an enforced invocation timeout. Handlers accepting that shape need
no context configuration. For middleware-specific fields, both `Local.layer`
and `Local.delivery` require a factory:

```ts
Local.layer({
  http: middlewareHttpHandler,
  lambdaContext: ({ context }) => ({
    ...context,
    user: localUser,
  }),
})
```

The factory runs for every HTTP request, WebSocket route invocation and stream
batch. Its `event` is the Lambda-shaped event and `context` is the default local
context. It must return a context accepted by all supplied handlers.

## Executors

Use `executor(schema)` from `@model-ts/graphql` for builder schemas, as in the
quick start. It preserves resolver service requirements, declared public
errors and builder error policy. Use `graphqlExecutor<C>(schema)` for a plain
graphql-js schema:

```ts
import { graphqlExecutor } from "@model-ts/graphql-lambda"
import { GraphQLObjectType, GraphQLSchema, GraphQLString } from "graphql"

const plainSchema = new GraphQLSchema({
  query: new GraphQLObjectType<unknown, AppContext>({
    name: "Query",
    fields: {
      greeting: {
        type: GraphQLString,
        resolve: (_, __, context) => `Hello from ${context.requestId}`,
      },
    },
  }),
  subscription: new GraphQLObjectType<Schema.Json, AppContext>({
    name: "Subscription",
    fields: {
      announcement: {
        type: GraphQLString,
        subscribe: (_, __, context) => context.pubSub.subscribe(announcements),
        resolve: (payload) => Schema.decodeUnknownSync(Schema.String)(payload),
      },
    },
  }),
})

export const plainOptions = {
  ...options,
  executor: graphqlExecutor<AppContext>(plainSchema),
}
export const plainHttp = createHttpHandler(plainOptions)
export const plainWebSocket = createWebSocketHandler(plainOptions)
export const plainDelivery = PubSub.DynamoDB.streamHandler(plainOptions)
```

`graphqlExecutor` adapts async subscription iterables to Effect Streams,
formats results and normalizes them to JSON. It protects custom scalar input
callbacks without mutating the supplied schema.

A custom `Executor<C, E, R>` supplies:

| Member                                      | Contract                                                                 |
| ------------------------------------------- | ------------------------------------------------------------------------ |
| `schema`                                    | A graphql-js `GraphQLSchema` for operation selection and validation      |
| `execute({ document, request, context })`   | `Effect<Result, E, R>`                                                   |
| `subscribe({ document, request, context })` | `Stream<Result, E, R>`                                                   |
| `validation: "executor"` (optional)         | Executor performs standard schema validation and formats its diagnostics |

`Result` is JSON-safe formatted execution data: `data`, `errors` and
`extensions`. Error objects must already be safe to return to clients; raw
`GraphQLError` instances do not cross the executor interface.

The builder executor declares `validation: "executor"`. Parsing, operation
selection, limits, GET restrictions, introspection restrictions and
single-root subscription checks remain transport responsibilities. Additional
`validationRules` run only after standard validation succeeds. With executor-owned
validation and additional rules, the transport runs a standard validation pass
as a gate, then leaves diagnostics to the executor. Without executor-owned
validation, including with `graphqlExecutor`, the transport performs standard
validation before creating context.
