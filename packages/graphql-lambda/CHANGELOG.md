# @model-ts/graphql-lambda

## 7.6.0

## 7.5.0

## 7.4.0

## 7.3.0

## 7.2.0

## 7.1.0

### Minor Changes

- fb3d2a6: Add Local adapters that run the production GraphQL HTTP, WebSocket and DynamoDB stream handlers on an Effect HTTP server. Local delivery subscribes to sandbox table changes and exposes a deterministic settled barrier for tests, with a socket-backed Gateway, optional keep-alive pings and an optional fixed or per-request HTTP latency for exercising loading states in local development.

  Move the PubSub table sandbox and memory Gateway into the explicit @model-ts/graphql-lambda/Sandbox subpath. Local is available through @model-ts/graphql-lambda/Local. Production entry points no longer import these development and test helpers.

## 7.0.0

### Major Changes

- a3e4012: Add @model-ts/graphql-lambda to the coordinated model-ts 7 release. Serve a graphql-js 16 schema or a `GraphQL.executor` from AWS Lambda behind API Gateway, with HTTP and WebSocket handlers written in Effect. Typed request context, service requirements and public error formatting carry through from the executor.

  The HTTP handler accepts API Gateway payload format 2.0 only. It supports GET and POST, batching, `cookies` in and out, response headers, credentialed CORS, and CSRF protection. A request passes the CSRF check when it sends `Content-Type: application/json` or one of the configured `x-*` headers; no headers are configured by default. Pass `graphiql: true` or GraphiQL options to serve GraphiQL from the same endpoint.

  The WebSocket handler speaks `graphql-transport-ws` only. `onConnect` authenticates the `$connect` upgrade and `onInit` handles `connection_init`. Request context is persisted on the connection through a required `persistedContext` Schema codec and decoded again for every message and delivery. Operation limits, keep-alive, generation fencing for pending subscriptions, masking of private failures, and cleanup of disconnected or Gone connections are included. Close codes that API Gateway cannot send are recorded on the close span and in a warning log before the connection is deleted.

  The `PubSub` module provides `PubSub.publish`, subscription sources, filters, and a storage service. `PubSub.DynamoDB` implements it on a dedicated model-ts table: `layer`, `layerFrom`, `layerMemory`, `layerLocal`, its own sandbox, and a `streamHandler` that delivers publications from DynamoDB Streams with TTL enforcement, typed AWS failures, tracing and partial-batch retries.

  Requires graphql-js `^16.11.0`. Parsing, validation and delivery helpers are internal; the public modules expose only handler-facing APIs.

### Patch Changes

- Updated dependencies [bb6d84e]
- Updated dependencies [7e30f81]
  - @model-ts/core@7.0.0
  - @model-ts/dynamodb@7.0.0
