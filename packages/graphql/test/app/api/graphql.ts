import { createBuilder } from "../../../src/GraphQL.ts"
import {
  ConditionalCheckFailedError,
  DynamoDBError,
  ItemDecodeError,
  ItemNotFoundError,
  PaginationError,
  RaceConditionError,
  KeyExistsError,
  ItemEncodeError,
  TransactionCanceledError,
} from "../store/errors.ts"
import type { AppContext } from "./context.ts"
export type { Register } from "../../../src/GraphQL.ts"
export interface Types {}

export const GraphQL = createBuilder<Types, AppContext>().withErrorPolicy({
  internal: [
    DynamoDBError,
    ItemDecodeError,
    ConditionalCheckFailedError,
    RaceConditionError,
    KeyExistsError,
    ItemEncodeError,
    TransactionCanceledError,
  ],
  public: [ItemNotFoundError, PaginationError],
})

export const t = GraphQL.fields
