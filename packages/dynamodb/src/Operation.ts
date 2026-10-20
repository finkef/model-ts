/**
 * Write operations as data, for `Dynamo.bulk`. Building an operation does
 * not touch the table; `bulk` runs them in transactions of up to 100 and
 * rolls back committed transactions when a later one fails.
 *
 * ```ts
 * yield* Dynamo.bulk([
 *   organization.operation("put"),
 *   membership.operation("put"),
 *   oldSlug.operation("delete"),
 * ])
 * ```
 *
 * @since 7.0.0
 */
import { docVersion, setDocVersion, modelOf, type TableModel } from "./Item.ts"
import { type GSIKeys, type Key, keyOf } from "./Keys.ts"
import { deletedKey } from "./Item.ts"

/**
 * Condition parameters shared by write operations.
 *
 * @since 7.0.0
 */
export interface ConditionParams {
  readonly ConditionExpression?: string | undefined
  readonly ExpressionAttributeNames?: Record<string, string> | undefined
  readonly ExpressionAttributeValues?: Record<string, unknown> | undefined
}

/**
 * @since 7.0.0
 */
export interface PutOptions extends ConditionParams {
  /** Overwrite an existing item instead of failing with `KeyExistsError`. */
  readonly IgnoreExistence?: boolean | undefined
}

/**
 * @since 7.0.0
 */
export interface DeleteOptions {
  /**
   * Skip the version and existence check of an instance delete. Soft deletes
   * still refuse to overwrite an existing archived item.
   */
  readonly ignoreVersion?: boolean | undefined
}

/**
 * @since 7.0.0
 */
export interface UpdateRawParams extends ConditionParams {
  readonly UpdateExpression?: string | undefined
}

/**
 * Attributes for `updateRaw`: model fields, plus GSI attributes. A GSI
 * attribute set to `null` is removed, which drops the item from that index.
 *
 * @since 7.0.0
 */
export type UpdateRawAttributes<Values> = Partial<Values> & {
  readonly [K in keyof GSIKeys]?: string | null
}

/** @since 7.0.0 */
export interface PutOperation extends PutOptions {
  readonly _tag: "Put"
  readonly item: Key
  /** Write the item to its soft-deleted location. */
  readonly softDeleted?: boolean | undefined
}

/** @since 7.0.0 */
export interface UpdateRawOperation extends UpdateRawParams {
  readonly _tag: "UpdateRaw"
  readonly model: TableModel
  readonly key: Key
  readonly attributes: Record<string, unknown>
}

/** @since 7.0.0 */
export interface DeleteOperation extends ConditionParams {
  readonly _tag: "Delete"
  readonly key: Key
}

/** @since 7.0.0 */
export interface ConditionCheckOperation extends ConditionParams {
  readonly _tag: "ConditionCheck"
  readonly key: Key
  readonly ConditionExpression: string
}

/**
 * One entry of a DynamoDB transaction.
 *
 * @since 7.0.0
 */
export type WriteOperation =
  PutOperation | UpdateRawOperation | DeleteOperation | ConditionCheckOperation

/**
 * A write with the write that undoes it, used when a later transaction of
 * the same `bulk` call fails.
 *
 * @since 7.0.0
 */
export interface TransactionOperation {
  readonly action: WriteOperation
  readonly rollback?:
    PutOperation | UpdateRawOperation | DeleteOperation | undefined
}

/** @since 7.0.0 */
export type BulkOperation = WriteOperation | TransactionOperation

/**
 * What `bulk` accepts. A nested array is a group: its operations always run
 * in the same transaction.
 *
 * @since 7.0.0
 */
export type BulkInput = ReadonlyArray<
  BulkOperation | ReadonlyArray<BulkOperation>
>

/**
 * What `update` returns: a single put when the key stays the same, or a
 * replace (put the new key, delete the old one) when it changes.
 *
 * @since 7.0.0
 */
export type UpdateOperation =
  PutOperation | readonly [TransactionOperation, TransactionOperation]

/** @since 7.0.0 */
export const isTransactionOperation = (
  operation: BulkOperation,
): operation is TransactionOperation => "action" in operation

/**
 * Creates `item`, failing the transaction when its key exists. Rolled back
 * by deleting it.
 *
 * @since 7.0.0
 */
export const put = (item: Key, options?: PutOptions): TransactionOperation => ({
  action: { _tag: "Put", item, ...options },
  rollback: { _tag: "Delete", key: keyOf(item) },
})

/**
 * Applies `patch` to `item`. Returns the updated instance and the operation
 * that stores it, guarded by the version `item` was read with.
 *
 * @since 7.0.0
 */
export const update = <A extends Key>(
  item: A,
  patch: Partial<Omit<A, "_tag">>,
): readonly [A, UpdateOperation] => {
  const model = modelOf(item)
  const previous = docVersion(item)
  const next = new model({ ...valuesOf(item), ...withoutUndefined(patch) })
  setDocVersion(next, (previous ?? 0) + 1)

  if (next.PK === item.PK && next.SK === item.SK) {
    const guard: ConditionParams =
      previous === undefined
        ? { ConditionExpression: undefined }
        : {
            ConditionExpression:
              "attribute_not_exists(#docVersion) OR #docVersion = :docVersion",
            ExpressionAttributeNames: { "#docVersion": "_docVersion" },
            ExpressionAttributeValues: { ":docVersion": previous },
          }
    return [next, { _tag: "Put", item: next, ...guard }]
  }

  return [
    next,
    [
      {
        action: {
          _tag: "Put",
          item: next,
          ConditionExpression: "attribute_not_exists(PK)",
        },
        rollback: { _tag: "Delete", key: keyOf(next) },
      },
      {
        action: { _tag: "Delete", key: keyOf(item) },
        rollback: { _tag: "Put", item },
      },
    ],
  ]
}

/**
 * Sets attributes on the item at `key` with an UpdateExpression built from
 * `attributes`. Fails the transaction when the item does not exist.
 *
 * @since 7.0.0
 */
export const updateRaw = <Values>(
  model: TableModel,
  key: Key,
  attributes: UpdateRawAttributes<Values>,
  params?: UpdateRawParams,
): UpdateRawOperation => ({
  _tag: "UpdateRaw",
  model,
  key: keyOf(key),
  attributes: withoutUndefined(attributes),
  ...params,
})

const remove = (key: Key, params?: ConditionParams): DeleteOperation => ({
  _tag: "Delete",
  key: keyOf(key),
  ...params,
})

export {
  /**
   * Deletes the item at `key`, unconditionally unless `params` add a
   * condition.
   *
   * @since 7.0.0
   */
  remove as delete,
}

/**
 * Deletes `item` if it still has the version it was read with. With
 * `ignoreVersion`, or for an instance that was never stored, the delete is
 * unconditional.
 *
 * @since 7.0.0
 */
export const deleteItem = (
  item: Key,
  options?: DeleteOptions,
): DeleteOperation => {
  const version = docVersion(item)
  if (options?.ignoreVersion || version === undefined) return remove(item)
  return remove(item, {
    ConditionExpression:
      version === 0
        ? "attribute_exists(PK) AND (attribute_not_exists(#docVersion) OR #docVersion = :docVersion)"
        : "attribute_exists(PK) AND #docVersion = :docVersion",
    ExpressionAttributeNames: { "#docVersion": "_docVersion" },
    ExpressionAttributeValues: { ":docVersion": version },
  })
}

/**
 * Moves `item` to its `$$DELETED$$` keys: deletes it (version-guarded like
 * `deleteItem`) and writes the archived copy. Pass the pair as one group to
 * `bulk` so both halves share a transaction.
 *
 * @since 7.0.0
 */
export const softDelete = (
  item: Key,
  options?: DeleteOptions,
): readonly [TransactionOperation, TransactionOperation] => [
  {
    action: deleteItem(item, options),
    rollback: { _tag: "Put", item },
  },
  {
    action: { _tag: "Put", item, softDeleted: true },
    rollback: { _tag: "Delete", key: deletedKey(item) },
  },
]

/**
 * Fails the transaction unless `ConditionExpression` holds for the item at
 * `key`. Writes nothing.
 *
 * @since 7.0.0
 */
export const condition = (
  key: Key,
  params: Omit<ConditionCheckOperation, "_tag" | "key">,
): ConditionCheckOperation => ({
  _tag: "ConditionCheck",
  key: keyOf(key),
  ...params,
})

const valuesOf = (item: Key): Record<string, unknown> =>
  (item as unknown as { values(): Record<string, unknown> }).values()

/** @internal */
export const withoutUndefined = <T extends object>(value: T): T =>
  Object.fromEntries(
    Object.entries(value).filter(([, entry]) => entry !== undefined),
  ) as T
