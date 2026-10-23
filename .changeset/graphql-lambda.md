---
"@model-ts/graphql-lambda": major
---

Add @model-ts/graphql-lambda to the coordinated model-ts 7 release. Replace aws-lambda-graphql and Apollo Server Lambda with Effect-native API Gateway HTTP and durable WebSocket handlers. Accept plain graphql-js schemas or GraphQL.executor directly, preserving typed request context, service requirements and formatted public errors with graphql-js 16.

Support HTTP batching, cookies, explicit credentialed CORS and CSRF protection, plus the legacy subscription protocol, upgrade/init authentication, operation limits and keep-alive. Keep untrusted init parameters separate from authenticated context, fence pending subscription setup with operation generations, mask private failures, and clean up disconnected or Gone connections.

Store connections, operations, topic memberships and optional published events as model-ts DynamoDB models behind a storage service. Deliver filtered publications directly from a publishing Lambda or through DynamoDB streams, with TTL enforcement, typed AWS failures, tracing and partial-batch retries. Include memory and DynamoDB Local tests, builder integration, and migration guidance for replacing the old four-table layout with one model-ts table.

Honor executor-owned schema validation so scalar literals and variables share the executor's public/internal error policy. Isolate delivery context and formatter defects per subscriber, log their causes, and continue fan-out while preserving transport storage and gateway retries.

Validate subscription roots through fragments on graphql-js 16, reject root skip/include directives without evaluating variables, and gate application validation rules on successful standard validation while preserving executor error policy. Mask and log thrown application validators on both transports.

Require graphql-js ^16.11.0 and keep transport parsing/validation helpers internal; the public request module exposes only handler-facing types.
