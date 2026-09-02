import { AWSError } from "./aws-sdk-v3-compat"
import { BulkOperation } from "./operations"

// TODO: populate errors with more info

export class KeyExistsError extends Error {
  readonly _tag = "KeyExistsError" as const
  name = "KeyExistsError"
}

export class ItemNotFoundError extends Error {
  readonly _tag = "ItemNotFoundError" as const
  name = "ItemNotFoundError"
}

export class ConditionalCheckFailedError extends Error {
  readonly _tag = "ConditionalCheckFailedError" as const
  name = "ConditionalCheckFailedError"
}

export class RaceConditionError extends Error {
  readonly _tag = "RaceConditionError" as const
  name = "RaceConditionError"
}

export class BulkWriteTransactionError extends Error {
  readonly _tag = "BulkWriteTransactionError" as const
  name = "BulkWriteTransactionError"
  error: AWSError

  constructor(error: AWSError) {
    super("An error occurred in one transaction during the bulk-write process.")
    this.error = error
  }
}

export class BulkWriteRollbackError extends Error {
  readonly _tag = "BulkWriteRollbackError" as const
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
  readonly _tag = "PaginationError" as const
  name = "PaginationError"
}

export class NotSupportedError extends Error {
  readonly _tag = "NotSupportedError" as const
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
