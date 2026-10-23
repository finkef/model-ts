import { Model } from "@model-ts/core"
import { DynamoDB } from "@model-ts/dynamodb"
import { Schema } from "effect"
import {
  ConnectionId,
  OperationId,
  OperationGeneration,
  EventId,
  Topic,
} from "./Identifiers.ts"

export const JsonObject = Schema.Record(Schema.String, Schema.Json)
export type JsonObject = typeof JsonObject.Type
export const OperationRequest = Schema.Struct({
  query: Schema.String,
  variables: Schema.optionalKey(Schema.NullOr(JsonObject)),
  operationName: Schema.optionalKey(Schema.NullOr(Schema.String)),
  extensions: Schema.optionalKey(Schema.NullOr(JsonObject)),
})
export type OperationRequest = typeof OperationRequest.Type

// JSON tuples avoid collisions when connection/operation/topic names contain separators.
const connectionPK = (id: ConnectionId) =>
  `GRAPHQL#CONNECTION#${JSON.stringify(id)}`
const operationSK = (id: OperationId) => `OPERATION#${JSON.stringify(id)}`

export class Connection extends Model.Class<Connection>()(
  "GraphQLConnection",
  DynamoDB.provider,
  {
    id: ConnectionId,
    endpoint: Schema.String,
    context: JsonObject,
    initParams: Schema.optionalKey(JsonObject),
    initialized: Schema.Boolean,
    createdAt: Schema.Number,
    ttl: Schema.Number,
  },
) {
  get PK() {
    return connectionPK(this.id)
  }
  get SK() {
    return "CONNECTION"
  }
  static key(id: ConnectionId) {
    return { PK: connectionPK(id), SK: "CONNECTION" }
  }
}

export class SubscriptionOperation extends Model.Class<SubscriptionOperation>()(
  "GraphQLSubscriptionOperation",
  DynamoDB.provider,
  {
    connectionId: ConnectionId,
    operationId: OperationId,
    request: OperationRequest,
    generation: OperationGeneration,
    state: Schema.Literals(["pending", "active"]),
    topics: Schema.Array(Topic),
    ttl: Schema.Number,
  },
) {
  get PK() {
    return connectionPK(this.connectionId)
  }
  get SK() {
    return operationSK(this.operationId)
  }
  static key(connectionId: ConnectionId, operationId: OperationId) {
    return { PK: connectionPK(connectionId), SK: operationSK(operationId) }
  }
}

export class Subscription extends Model.Class<Subscription>()(
  "GraphQLSubscription",
  DynamoDB.provider,
  {
    topic: Topic,
    connectionId: ConnectionId,
    operationId: OperationId,
    ttl: Schema.Number,
  },
) {
  get PK() {
    return Subscription.key(this.topic, this.connectionId, this.operationId).PK
  }
  get SK() {
    return Subscription.key(this.topic, this.connectionId, this.operationId).SK
  }
  static partitionKey(topic: Topic) {
    return `GRAPHQL#TOPIC#${JSON.stringify(topic)}`
  }
  static key(
    topic: Topic,
    connectionId: ConnectionId,
    operationId: OperationId,
  ) {
    return {
      PK: Subscription.partitionKey(topic),
      SK: JSON.stringify([connectionId, operationId]),
    }
  }
}

export class PublishedEvent extends Model.Class<PublishedEvent>()(
  "GraphQLPublishedEvent",
  DynamoDB.provider,
  {
    id: EventId,
    topic: Topic,
    // JSON text deliberately preserves the old production serialization contract.
    payload: Schema.String,
    ttl: Schema.Number,
  },
) {
  get PK() {
    return `GRAPHQL#EVENT#${this.id}`
  }
  get SK() {
    return "EVENT"
  }
}
