import { GSI_NAMES, GSI } from "../gsi"

export type InMemoryIndexName = "primary" | GSI

export interface InMemoryMethodSpec {
  supportedParams: string[]
  unsupportedParams?: string[]
}

export interface InMemorySpec {
  version: string
  scope: string
  projection: "ALL"
  excludedIndexes: string[]
  indexes: InMemoryIndexName[]
  methods: {
    [method: string]: InMemoryMethodSpec
  }
  unsupportedMethods: string[]
}

export const IN_MEMORY_INDEXES: InMemoryIndexName[] = [
  "primary",
  ...GSI_NAMES,
]

export const IN_MEMORY_SPEC: InMemorySpec = {
  version: "2026-02-09",
  scope: "model-ts/dynamodb",
  projection: "ALL",
  excludedIndexes: ["GSI1"],
  indexes: IN_MEMORY_INDEXES,
  methods: {
    get: {
      supportedParams: ["TableName", "Key", "ConsistentRead"],
      unsupportedParams: [
        "AttributesToGet",
        "ProjectionExpression",
        "ExpressionAttributeNames",
      ],
    },
    put: {
      supportedParams: [
        "TableName",
        "Item",
        "ConditionExpression",
        "ExpressionAttributeNames",
        "ExpressionAttributeValues",
      ],
      unsupportedParams: [
        "Expected",
        "ReturnValues",
        "ReturnConsumedCapacity",
        "ReturnItemCollectionMetrics",
      ],
    },
    update: {
      supportedParams: [
        "TableName",
        "Key",
        "ConditionExpression",
        "UpdateExpression",
        "ExpressionAttributeNames",
        "ExpressionAttributeValues",
        "ReturnValues",
      ],
      unsupportedParams: [
        "Expected",
        "AttributeUpdates",
        "ReturnConsumedCapacity",
        "ReturnItemCollectionMetrics",
      ],
    },
    delete: {
      supportedParams: [
        "TableName",
        "Key",
        "ConditionExpression",
        "ExpressionAttributeNames",
        "ExpressionAttributeValues",
      ],
      unsupportedParams: [
        "Expected",
        "ReturnValues",
        "ReturnConsumedCapacity",
        "ReturnItemCollectionMetrics",
      ],
    },
    query: {
      supportedParams: [
        "TableName",
        "IndexName",
        "KeyConditionExpression",
        "FilterExpression",
        "ExpressionAttributeNames",
        "ExpressionAttributeValues",
        "Limit",
        "ExclusiveStartKey",
        "ScanIndexForward",
        "ConsistentRead",
      ],
      unsupportedParams: [
        "Select",
        "ProjectionExpression",
        "KeyConditions",
        "QueryFilter",
        "ConditionalOperator",
        "AttributesToGet",
      ],
    },
    scan: {
      supportedParams: [
        "TableName",
        "FilterExpression",
        "ExpressionAttributeNames",
        "ExpressionAttributeValues",
        "Limit",
        "ExclusiveStartKey",
      ],
      unsupportedParams: [
        "ProjectionExpression",
        "Segment",
        "TotalSegments",
        "Select",
        "ScanFilter",
      ],
    },
    batchGet: {
      supportedParams: ["RequestItems"],
      unsupportedParams: ["ReturnConsumedCapacity"],
    },
    batchWrite: {
      supportedParams: ["RequestItems"],
      unsupportedParams: ["ReturnConsumedCapacity", "ReturnItemCollectionMetrics"],
    },
    transactWrite: {
      supportedParams: ["TransactItems"],
      unsupportedParams: [
        "ClientRequestToken",
        "ReturnConsumedCapacity",
        "ReturnItemCollectionMetrics",
      ],
    },
  },
  unsupportedMethods: [
    "createSet",
    "transactGet",
    "putItem",
    "deleteItem",
    "updateItem",
    "queryItems",
    "scanItems",
  ],
}

export const IN_MEMORY_CONDITIONS = {
  excludedGSI: "GSI1 is intentionally excluded from in-memory mode.",
  gsiProjection: "All GSIs are treated as full projection for in-scope behavior.",
}
