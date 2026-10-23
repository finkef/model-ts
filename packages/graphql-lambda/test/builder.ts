import { createBuilder, executor } from "@model-ts/graphql"
import { Context, Effect, Schema, Stream } from "effect"
import { Topic, type ContextRequest, type PubSub } from "../src/index.ts"
import { JsonObject } from "../src/Models.ts"

export interface BuilderContext {
  readonly user: string
  readonly pubSub: PubSub.Source
}
export class Greeting extends Context.Service<
  Greeting,
  { readonly prefix: string }
>()("test/Greeting") {}
export class Identity extends Context.Service<
  Identity,
  { readonly user: string }
>()("test/Identity") {}
class PublicFailure extends Schema.TaggedError<PublicFailure>()(
  "PublicFailure",
  { message: Schema.String },
) {}
export const privateFailure = new Error("private database credentials")
const GraphQL = createBuilder<{}, BuilderContext>().withErrorPolicy({
  public: [PublicFailure],
  publicCodes: { PublicFailure: "INVALID_STATE" },
})
const hello = GraphQL.queryField("hello", "String", (_, __, context) =>
  Effect.map(Greeting, ({ prefix }) => prefix + context.user),
)
const publicFailure = GraphQL.queryField(
  "publicFailure",
  GraphQL.nullable("String"),
  () =>
    Effect.fail(new PublicFailure({ message: "This action is unavailable" })),
)
const forbidden = GraphQL.queryField("forbidden", GraphQL.nullable("String"), {
  authorize: () => false,
  resolve: () => "private",
})
const internal = GraphQL.queryField(
  "internal",
  GraphQL.nullable("String"),
  () => Effect.die(privateFailure),
)
export const builderTopic = Topic.make("builder/messages")
const Payload = Schema.Struct({ user: Schema.String, message: Schema.String })
const message = GraphQL.subscriptionField("message", "String", {
  subscribe: (_, __, context) =>
    context.pubSub.stream(builderTopic).pipe(
      Stream.map((payload) => Schema.decodeUnknownSync(Payload)(payload)),
      Stream.filter((payload) => payload.user === context.user),
      Stream.mapEffect((payload) =>
        Effect.map(Greeting, ({ prefix }) => prefix + payload.message),
      ),
    ),
})
export const built = GraphQL.schema({
  hello,
  publicFailure,
  forbidden,
  internal,
  message,
})
export const builderOptions = {
  persistedContext: JsonObject,
  // No adapter: the builder owns error formatting and both sides consume Streams.
  executor: executor(built),
  context: (request: ContextRequest<{ readonly requestId: string }>) =>
    Effect.map(Identity, ({ user }): BuilderContext => ({
      user,
      pubSub: request.pubSub,
    })),
}
