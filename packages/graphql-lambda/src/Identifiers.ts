import { Schema } from "effect"

export const ConnectionId = Schema.String.pipe(
  Schema.brand("GraphQLConnectionId"),
)
export type ConnectionId = typeof ConnectionId.Type
export const OperationId = Schema.String.pipe(
  Schema.brand("GraphQLOperationId"),
)
export type OperationId = typeof OperationId.Type
export const EventId = Schema.String.pipe(Schema.brand("GraphQLEventId"))
export type EventId = typeof EventId.Type
export const Topic = Schema.String.pipe(Schema.brand("GraphQLTopic"))
export type Topic = typeof Topic.Type

export const OperationGeneration = Schema.String.pipe(
  Schema.brand("GraphQLOperationGeneration"),
)
export type OperationGeneration = typeof OperationGeneration.Type
