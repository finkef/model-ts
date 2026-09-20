import {
  DynamoDBModelConstructor,
  DynamoDBModelInstance,
} from "./dynamodb-model"
import { DeleteOperation, DeleteOptions, PutOperation } from "./operations"

// Internal helpers: the public operation carries only DynamoDB condition fields.
export function itemDelete<M extends DynamoDBModelConstructor<any>>(
  model: M,
  item: DynamoDBModelInstance,
  options?: DeleteOptions,
  params: Omit<DeleteOperation<M>, "_model" | "_operation" | "key"> = {}
): DeleteOperation<M> {
  const operation: DeleteOperation<M> = {
    _model: model,
    _operation: "delete",
    key: { PK: item.PK, SK: item.SK },
    ...params,
  }
  const version = item._docVersion
  if (options?.ignoreVersion || typeof version !== "number") return operation

  const names = { ...params.ExpressionAttributeNames }
  const values = { ...params.ExpressionAttributeValues }
  let name = "#docVersion"
  let value = ":docVersion"
  while (name in names) name += "_"
  while (value in values) value += "_"
  names[name] = "_docVersion"
  values[value] = version

  const versionCondition =
    version === 0
      ? `(attribute_not_exists(${name}) OR ${name} = ${value})`
      : `${name} = ${value}`
  const guard = `attribute_exists(PK) AND ${versionCondition}`
  return {
    ...operation,
    ConditionExpression: params.ConditionExpression
      ? `(${params.ConditionExpression}) AND (${guard})`
      : guard,
    ExpressionAttributeNames: names,
    ExpressionAttributeValues: values,
  }
}

export function softDeleteOperations<
  T extends DynamoDBModelInstance,
  M extends DynamoDBModelConstructor<T>
>(
  model: M,
  item: T,
  options?: DeleteOptions
): [
  { action: DeleteOperation<M>; rollback: PutOperation<T, M> },
  { action: PutOperation<T, M>; rollback: DeleteOperation<M> }
] {
  return [
    {
      action: itemDelete(model, item, options),
      rollback: { _model: model, _operation: "put", item },
    },
    {
      action: { _model: model, _operation: "put", _deleted: true, item },
      rollback: {
        _model: model,
        _operation: "delete",
        key: { PK: `$$DELETED$$${item.PK}`, SK: `$$DELETED$$${item.SK}` },
      },
    },
  ]
}
