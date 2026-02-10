import { AWSError } from "aws-sdk/lib/error"
import { BulkOperation } from "./operations"

// TODO: populate errors with more info

export class KeyExistsError extends Error {
  name = "KeyExistsError"
}

export class ItemNotFoundError extends Error {
  name = "ItemNotFoundError"
}

export class ConditionalCheckFailedError extends Error {
  name = "ConditionalCheckFailedError"
}

export class RaceConditionError extends Error {
  name = "RaceConditionError"
}

export class BulkWriteTransactionError extends Error {
  name = "BulkWriteTransactionError"
  error: AWSError

  constructor(error: AWSError) {
    super("An error occurred in one transaction during the bulk-write process.")
    this.error = error
  }
}

export class BulkWriteRollbackError extends Error {
  name = "BulkWriteRollbackError"
  requiresRollback: BulkOperation<any, any>[]

  constructor(requiresRollback: BulkOperation<any, any>[]) {
    super(
      "An error occurred during the bulk-write rollback. Some operations have not been rolled back."
    )
    this.requiresRollback = requiresRollback
  }
}

export class PaginationError extends Error {
  name = "PaginationError"
}

export class NotSupportedError extends Error {
  name = "NotSupportedError"
  code = "NotSupportedError"
  method: string
  featurePath: string
  reason: string

  constructor({
    method,
    featurePath,
    reason,
  }: {
    method: string
    featurePath: string
    reason: string
  }) {
    super(reason)
    this.method = method
    this.featurePath = featurePath
    this.reason = reason
  }
}

export type DynamoDBError =
  | KeyExistsError
  | ItemNotFoundError
  | ConditionalCheckFailedError
  | BulkWriteTransactionError
  | BulkWriteRollbackError
  | PaginationError
  | NotSupportedError
