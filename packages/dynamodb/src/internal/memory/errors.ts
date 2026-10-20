/**
 * Thrown by the in-memory engine for a DynamoDB feature it does not
 * implement. The request handler turns it into an error response, so callers
 * see a `DynamoDBError` whose cause is named `NotSupportedError`.
 */
export class NotSupportedError extends Error {
  override readonly name = "NotSupportedError"
  readonly code = "NotSupportedError"
  readonly method: string
  readonly featurePath: string
  readonly reason: string

  constructor(options: {
    readonly method: string
    readonly featurePath: string
    readonly reason: string
  }) {
    super(options.reason)
    this.method = options.method
    this.featurePath = options.featurePath
    this.reason = options.reason
  }
}
