import { ManagedRuntime } from "effect"
import { executor, type Request } from "../../../src/GraphQL.ts"
import { DynamoDBMemory } from "../store/models.ts"
import type { AppContext } from "./context.ts"
import { GraphQL } from "./graphql.ts"
import { schema } from "./schema.ts"

export const execute = (request: Request, context: AppContext) =>
  GraphQL.execute(schema, request, context)

export const transport = executor(schema)

// App services are built once per Lambda container and shared by every request.
// In Lambda this is DynamoDBLive (clients, tracing); locally the in-memory table.
const runtime = ManagedRuntime.make(DynamoDBMemory)

export const handler = (request: Request, context: AppContext) =>
  runtime.runPromise(execute(request, context))
