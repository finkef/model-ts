/**
 * Publishing fails with one of three tagged errors, together `PublishErrors`.
 *
 * @since 7.0.0
 */
import { Predicate, Schema } from "effect"

const describe = (cause: unknown) =>
  Predicate.hasProperty(cause, "message") && typeof cause.message === "string"
    ? cause.message
    : String(cause)

/**
 * The PutEvents call failed: network, permissions, throttling and so on.
 * `cause` is the AWS SDK error.
 *
 * @since 7.0.0
 */
export class EventBridgeError extends Schema.TaggedError<EventBridgeError>()(
  "EventBridgeError",
  { cause: Schema.Defect() },
) {
  override get message() {
    return `EventBridge PutEvents failed: ${describe(this.cause)}`
  }
}

/**
 * One entry of a PutEvents response.
 *
 * @since 7.0.0
 */
export const ResultEntry = Schema.Struct({
  EventId: Schema.optional(Schema.String),
  ErrorCode: Schema.optional(Schema.String),
  ErrorMessage: Schema.optional(Schema.String),
})

/** @since 7.0.0 */
export type ResultEntry = typeof ResultEntry.Type

/**
 * PutEvents answered, but rejected some entries. `entries` has one result
 * per published event, in order, so the failed ones can be retried.
 *
 * @since 7.0.0
 */
export class PublishError extends Schema.TaggedError<PublishError>()(
  "PublishError",
  {
    failedEntryCount: Schema.Number,
    entries: Schema.Array(ResultEntry),
  },
) {
  override get message() {
    return `EventBridge rejected ${this.failedEntryCount} of ${this.entries.length} events`
  }
}

/**
 * An event could not be encoded with its own schema, so nothing was sent.
 *
 * @since 7.0.0
 */
export class EventEncodeError extends Schema.TaggedError<EventEncodeError>()(
  "EventEncodeError",
  {
    detailType: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message() {
    return `Cannot encode ${this.detailType}: ${describe(this.cause)}`
  }
}

/** @since 7.0.0 */
export type PublishErrors = EventBridgeError | PublishError | EventEncodeError
