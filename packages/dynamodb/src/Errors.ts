/**
 * Every failure is a tagged error in the Effect error channel, so callers
 * handle them with `Effect.catchTag` and the compiler knows which ones can
 * occur:
 *
 * ```ts
 * User.load(key).pipe(
 *   Effect.catchTag("ItemNotFoundError", () => Effect.succeed(null)),
 * )
 * ```
 *
 * @since 7.0.0
 */
import { Predicate, Schema } from "effect"
import { formatKey } from "./internal/keys.ts"
import { Key } from "./Keys.ts"
import type { BulkOperation } from "./Operation.ts"

const describe = (cause: unknown) =>
  Predicate.hasProperty(cause, "message") && typeof cause.message === "string"
    ? cause.message
    : String(cause)

/**
 * The AWS SDK call failed for a reason this library does not interpret:
 * throttling, permissions, validation, network errors and so on. `cause` is
 * the SDK error and `code` its name.
 *
 * @since 7.0.0
 */
export class DynamoDBError extends Schema.TaggedError<DynamoDBError>()(
  "DynamoDBError",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {
  /** The AWS error name, e.g. `"ProvisionedThroughputExceededException"`. */
  get code(): string | undefined {
    return Predicate.hasProperty(this.cause, "name") &&
      typeof this.cause.name === "string"
      ? this.cause.name
      : undefined
  }

  override get message() {
    return `DynamoDB ${this.operation} failed: ${describe(this.cause)}`
  }
}

/**
 * No item is stored under the key.
 *
 * @since 7.0.0
 */
export class ItemNotFoundError extends Schema.TaggedError<ItemNotFoundError>()(
  "ItemNotFoundError",
  { key: Key },
) {
  override get message() {
    return `No item found at ${formatKey(this.key)}`
  }
}

/**
 * `put()` found an item under the key already.
 *
 * @since 7.0.0
 */
export class KeyExistsError extends Schema.TaggedError<KeyExistsError>()(
  "KeyExistsError",
  { key: Key },
) {
  override get message() {
    return `An item already exists at ${formatKey(this.key)}`
  }
}

/**
 * A condition expression passed by the caller did not hold.
 *
 * @since 7.0.0
 */
export class ConditionalCheckFailedError extends Schema.TaggedError<ConditionalCheckFailedError>()(
  "ConditionalCheckFailedError",
  { key: Key },
) {
  override get message() {
    return `The condition for ${formatKey(this.key)} did not hold`
  }
}

/**
 * The stored item changed (or was removed) since this instance was read, so
 * the optimistic version check on `_docVersion` rejected the write.
 *
 * @since 7.0.0
 */
export class RaceConditionError extends Schema.TaggedError<RaceConditionError>()(
  "RaceConditionError",
  { key: Key },
) {
  override get message() {
    return `The item at ${formatKey(this.key)} changed since this instance was read`
  }
}

/**
 * Why DynamoDB cancelled one entry of a transaction, as reported in
 * `CancellationReasons`. `Code` is `"None"` for entries that did not cause
 * the cancellation.
 *
 * @since 7.0.0
 */
export const CancellationReason = Schema.Struct({
  Code: Schema.optional(Schema.String),
  Message: Schema.optional(Schema.String),
})

/** @since 7.0.0 */
export type CancellationReason = typeof CancellationReason.Type

/**
 * DynamoDB cancelled a transaction, usually because a condition failed.
 * `cancellationReasons` has one entry per transaction item, in order.
 *
 * @since 7.0.0
 */
export class TransactionCanceledError extends Schema.TaggedError<TransactionCanceledError>()(
  "TransactionCanceledError",
  {
    cancellationReasons: Schema.Array(CancellationReason),
    cause: Schema.Defect(),
  },
) {
  override get message() {
    const codes = this.cancellationReasons.map(
      (reason) => reason.Code ?? "None",
    )
    return `Transaction cancelled [${codes.join(", ")}]`
  }
}

const BulkOperations = Schema.declare(
  (input): input is ReadonlyArray<BulkOperation> => Array.isArray(input),
)

/**
 * A bulk write failed and some of its already committed transactions could
 * not be rolled back. `requiresRollback` lists the operations whose effects
 * are still in the table; `cause` is the failure that started the rollback.
 *
 * @since 7.0.0
 */
export class BulkWriteRollbackError extends Schema.TaggedError<BulkWriteRollbackError>()(
  "BulkWriteRollbackError",
  {
    requiresRollback: BulkOperations,
    cause: Schema.Defect(),
    rollbackCause: Schema.Defect(),
  },
) {
  override get message() {
    return `A bulk write failed (${describe(this.cause)}) and ${this.requiresRollback.length} operations could not be rolled back (${describe(this.rollbackCause)})`
  }
}

/**
 * Invalid pagination arguments or an unreadable cursor.
 *
 * @since 7.0.0
 */
export class PaginationError extends Schema.TaggedError<PaginationError>()(
  "PaginationError",
  { message: Schema.String },
) {}

/**
 * A stored item does not match the model's schema. `key` is the item's
 * primary key, when it has one.
 *
 * @since 7.0.0
 */
export class ItemDecodeError extends Schema.TaggedError<ItemDecodeError>()(
  "ItemDecodeError",
  {
    model: Schema.String,
    key: Schema.optional(Key),
    cause: Schema.instanceOf(Schema.SchemaError),
  },
) {
  override get message() {
    const at = this.key ? ` at ${formatKey(this.key)}` : ""
    return `Stored item${at} does not match ${this.model}: ${this.cause.message}`
  }
}

/**
 * A model instance holds a value its schema cannot encode. Construction does
 * not validate, so this is where a bad value is caught before it is written.
 *
 * @since 7.0.0
 */
export class ItemEncodeError extends Schema.TaggedError<ItemEncodeError>()(
  "ItemEncodeError",
  {
    model: Schema.String,
    cause: Schema.instanceOf(Schema.SchemaError),
  },
) {
  override get message() {
    return `Cannot encode ${this.model}: ${this.cause.message}`
  }
}
