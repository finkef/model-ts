/**
 * DynamoDB Streams records in the JSON form AWS Lambda receives them:
 * attribute values in wire form, with binary values as base64 strings.
 * The sandboxes emit these records, so the stream handlers an application
 * deploys can run against a test table unchanged.
 *
 * @since 7.1.0
 */

/**
 * @since 7.1.0
 */
export type AttributeValue =
  | { readonly S: string }
  | { readonly N: string }
  | { readonly B: string }
  | { readonly SS: ReadonlyArray<string> }
  | { readonly NS: ReadonlyArray<string> }
  | { readonly BS: ReadonlyArray<string> }
  | { readonly M: Image }
  | { readonly L: ReadonlyArray<AttributeValue> }
  | { readonly NULL: true }
  | { readonly BOOL: boolean }

/**
 * An item, or the key of one, as attribute values.
 *
 * @since 7.1.0
 */
export interface Image {
  readonly [attribute: string]: AttributeValue
}

/**
 * One change to one item. Images follow the `NEW_AND_OLD_IMAGES` view:
 * `NewImage` is missing on `REMOVE` and `OldImage` on `INSERT`.
 *
 * @since 7.1.0
 */
export interface StreamRecord {
  readonly eventID: string
  readonly eventName: "INSERT" | "MODIFY" | "REMOVE"
  readonly eventVersion: string
  readonly eventSource: "aws:dynamodb"
  readonly awsRegion: string
  readonly eventSourceARN: string
  readonly dynamodb: {
    /** Seconds since the epoch. */
    readonly ApproximateCreationDateTime: number
    readonly Keys: Image
    readonly NewImage?: Image
    readonly OldImage?: Image
    /** Increases with every record of a shard. */
    readonly SequenceNumber: string
    readonly SizeBytes: number
    readonly StreamViewType: "NEW_AND_OLD_IMAGES"
  }
}

/**
 * The event a Lambda function subscribed to a table's stream receives.
 *
 * @since 7.1.0
 */
export interface StreamEvent {
  readonly Records: ReadonlyArray<StreamRecord>
}
