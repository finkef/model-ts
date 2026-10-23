import { Context } from "effect"
import type { DynamoDB, DynamoDBService } from "../DynamoDB.ts"

/** The key of the `DynamoDB` service: its identity in a `Context`. */
export const KEY = "@model-ts/dynamodb/DynamoDB"

/**
 * The `DynamoDB` service, for the operations in `operations.ts`. A Context
 * finds a service by its key, so this tag and the `DynamoDB` class read the
 * same service. The operations use it so that they need no runtime import of
 * `DynamoDB.ts`, which imports them to expose them as statics.
 */
export const TableService = Context.Service<DynamoDB, DynamoDBService>(KEY)
