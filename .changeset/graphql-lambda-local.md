---
"@model-ts/graphql-lambda": minor
---

Add Local adapters that run the production GraphQL HTTP, WebSocket and DynamoDB stream handlers on an Effect HTTP server. Local delivery subscribes to sandbox table changes and exposes a deterministic settled barrier for tests, with a socket-backed Gateway, optional keep-alive pings and an optional fixed or per-request HTTP latency for exercising loading states in local development.

Move the PubSub table sandbox and memory Gateway into the explicit @model-ts/graphql-lambda/Sandbox subpath. Local is available through @model-ts/graphql-lambda/Local. Production entry points no longer import these development and test helpers.
