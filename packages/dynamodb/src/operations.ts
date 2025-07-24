import { DocumentClient } from "aws-sdk/clients/dynamodb"
import { TypeOf } from "@model-ts/core"
import {
  DynamoDBModelInstance,
  DynamoDBModelConstructor,
  Decodable,
} from "./dynamodb-model"
import { Key } from "./client"
import { GSIPK, GSISK } from "./gsi"

export type Operation<
  T extends DynamoDBModelInstance,
  M extends DynamoDBModelConstructor<T>
> =
  | GetOperation<M>
  | PutOperation<T, M>
  | DeleteOperation<M>
  | UpdateRawOperation<M>
  | ConditionCheckOperation

export interface GetOperation<M extends Decodable>
  extends Pick<DocumentClient.GetItemInput, "ConsistentRead"> {
  _operation: "get"
  _model: M
  key: Key
}

export interface PutOperation<
  T extends DynamoDBModelInstance,
  M extends DynamoDBModelConstructor<T>
> extends Pick<
    DocumentClient.PutItemInput,
    | "ConditionExpression"
    | "ExpressionAttributeValues"
    | "ExpressionAttributeNames"
  > {
  IgnoreExistence?: boolean
  _operation: "put"
  _model: M
  _deleted?: boolean
  item: T
}

export interface DeleteOperation<M extends DynamoDBModelConstructor<any>> {
  _operation: "delete"
  _model: M
  key: Key
}

export interface UpdateRawOperation<M extends DynamoDBModelConstructor<any>>
  extends Pick<
    DocumentClient.UpdateItemInput,
    | "UpdateExpression"
    | "ConditionExpression"
    | "ExpressionAttributeNames"
    | "ExpressionAttributeValues"
  > {
  _operation: "updateRaw"
  _model: M
  key: Key
  attributes: Partial<TypeOf<M>> &
    { [key in GSIPK]?: string | null } &
    { [key in GSISK]?: string | null }
}

// -------------------------------------------------------------------------------------
// Bulk Operations
// -------------------------------------------------------------------------------------

export interface ConditionCheckOperation
  extends Pick<
    DocumentClient.ConditionCheck,
    | "ConditionExpression"
    | "ExpressionAttributeNames"
    | "ExpressionAttributeValues"
  > {
  _operation: "condition"
  key: Key
}

export interface TransactionOperation<
  T extends DynamoDBModelInstance,
  M extends DynamoDBModelConstructor<T>
> {
  /**
   * Performed inside a call to `docClient.transactWrite`. Might be executed in a different
   * transaction than other operations of this call.
   */
  action:
    | PutOperation<T, M>
    | UpdateRawOperation<M>
    | DeleteOperation<M>
    | ConditionCheckOperation
  /**
   * Performed in case a subsequent transaction fails. Can be omitted.
   */
  rollback?: PutOperation<T, M> | UpdateRawOperation<M> | DeleteOperation<M>
}

export type BulkOperation<
  T extends DynamoDBModelInstance,
  M extends DynamoDBModelConstructor<T>
> =
  | PutOperation<T, M>
  | UpdateRawOperation<M>
  | DeleteOperation<M>
  | ConditionCheckOperation
  | TransactionOperation<T, M>

export const isTransactionOperation = <
  T extends DynamoDBModelInstance,
  M extends DynamoDBModelConstructor<T>
>(
  operation: BulkOperation<T, M>
): operation is TransactionOperation<T, M> => "action" in operation
