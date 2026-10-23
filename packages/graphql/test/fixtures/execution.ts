import { Context, Effect, Stream } from "effect"
import { parse } from "graphql"
import {
  createBuilder,
  execute,
  subscribe,
  executor,
} from "../../src/GraphQL.ts"
import * as Testing from "../../src/Testing.ts"

export interface TenantContext {
  readonly tenant: string
}
export interface UserContext {
  readonly user: string
}
export class Service extends Context.Service<
  Service,
  { readonly value: string }
>()("fixture/Service") {}
export const tenant = createBuilder<{}, TenantContext>()
export const user = createBuilder<{}, UserContext>()
export const query = tenant.query({
  value: tenant.field("String", (_, __, ctx) => ctx.tenant),
})
export const subscription = tenant.subscription({
  event: tenant.events("String", {
    authorize: (_, __, ctx) => Effect.map(Service, () => ctx.tenant.length > 0),
    subscribe: (_, __, ctx) => Stream.succeed(ctx.tenant),
  }),
})
export const schema = tenant.schema({ query, subscription })
export const runQuery = execute(
  schema,
  { source: "{value}" },
  { tenant: "tenant" },
)
export const runEvents = subscribe(
  schema,
  { source: "subscription {event}" },
  { tenant: "tenant" },
)
export const adapter = executor(schema)
export const adapterQuery = adapter.execute({
  document: parse("{value}"),
  request: { query: "{value}" },
  context: { tenant: "tenant" },
})
export const adapterEvents = adapter.subscribe({
  document: parse("subscription {event}"),
  request: { query: "subscription {event}" },
  context: { tenant: "tenant" },
})
export const snapshotQuery = Testing.execute(
  schema,
  { source: "{value}" },
  { tenant: "tenant" },
)
export const snapshotEvents = Testing.subscribe(
  schema,
  { source: "subscription {event}" },
  { tenant: "tenant" },
)
export const provided = () =>
  Effect.runPromise(
    runQuery.pipe(Effect.provideService(Service, { value: "service" })),
  )
export const builderQuery = tenant.execute(
  schema,
  { source: "{value}" },
  { tenant: "tenant" },
)
export const builderEvents = tenant.subscribe(
  schema,
  { source: "subscription {event}" },
  { tenant: "tenant" },
)

export const formattedQuery = adapterQuery.pipe(
  Effect.map((result) => result.errors?.[0]?.message),
)
export const formattedEvent = adapterEvents.pipe(
  Stream.map((result) => result.errors?.[0]?.message),
)
