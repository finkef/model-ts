import { Schema } from "effect"

export {
  ItemNotFoundError,
  ItemDecodeError,
  DynamoDBError,
  ConditionalCheckFailedError,
  PaginationError,
  RaceConditionError,
  KeyExistsError,
  ItemEncodeError,
  TransactionCanceledError,
} from "@model-ts/dynamodb"

// App errors that clients see as union members
export class NotFoundError extends Schema.TaggedError<NotFoundError>()(
  "NotFoundError",
  { message: Schema.String },
) {}
export class UserInputError extends Schema.TaggedError<UserInputError>()(
  "UserInputError",
  { message: Schema.String, field: Schema.String },
) {}
