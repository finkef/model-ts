import { Effect, Schema } from "effect"
import { PersistedContextError } from "../Errors.ts"

export const decodePersisted = <P, RD, RE>(
  schema: Schema.Codec<P, Schema.Json, RD, RE>,
  value: Schema.Json | undefined,
) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError(
      (cause) => new PersistedContextError({ operation: "decode", cause }),
    ),
    Effect.withSpan("GraphQL.context.decode"),
  )

export const encodePersisted = <P, RD, RE>(
  schema: Schema.Codec<P, Schema.Json, RD, RE>,
  value: P,
) =>
  Schema.encodeEffect(schema)(value).pipe(
    Effect.mapError(
      (cause) => new PersistedContextError({ operation: "encode", cause }),
    ),
    Effect.withSpan("GraphQL.context.encode"),
  )
