/**
 * The DynamoDB model provider, available as `DynamoDB.provider`: table
 * operations as model members.
 *
 * ```ts
 * class Post extends Model.Class<Post>()("Post", DynamoDB.provider, {
 *   id: PostId,
 *   title: Schema.String,
 * }) {
 *   get PK() { return `POST#${this.id}` }
 *   get SK() { return "POST" }
 * }
 *
 * const post = yield* Post.load({ PK: "POST#1", SK: "POST" })
 * const renamed = yield* post.update({ title: "Renamed" })
 * ```
 *
 * @since 7.0.0
 */
import { type Model, Provider } from "@model-ts/core"
import type { Effect, Stream } from "effect"
import type { DynamoDB } from "./DynamoDB.ts"
import { modelOf } from "./internal/item.ts"
import { keysOf } from "./internal/keys.ts"
import * as Table from "./internal/operations.ts"
import type { Decodable, Recovered, TableItem, TableModel } from "./Item.ts"
import type { ItemKeys, Key } from "./Keys.ts"
import * as Operation from "./Operation.ts"
import type { PaginationInput, PaginationResult } from "./Pagination.ts"

/**
 * Members the provider adds to instances. They need `PK` and `SK` on the
 * instance, so a model without them cannot call them.
 *
 * @since 7.0.0
 */
export interface DynamoDBInstance<Self, F> {
  /** See `DynamoDB.put`. */
  put(
    this: Self & Key,
    options?: Operation.PutOptions,
  ): Effect.Effect<Self, DynamoDB.PutError, DynamoDB>
  /** See `DynamoDB.update`. Returns the updated instance. */
  update(
    this: Self & Key,
    patch: Partial<Model.Values<F>>,
  ): Effect.Effect<Self, DynamoDB.UpdateError, DynamoDB>
  /** The updated instance and the operation that stores it, for `DynamoDB.bulk`. */
  applyUpdate(
    this: Self & Key,
    patch: Partial<Model.Values<F>>,
  ): readonly [Self, Operation.UpdateOperation]
  /** See `DynamoDB.deleteItem`: version-guarded unless `ignoreVersion`. */
  delete(
    this: Self & Key,
    options?: Operation.DeleteOptions,
  ): Effect.Effect<void, DynamoDB.DeleteItemError, DynamoDB>
  /** See `DynamoDB.softDelete`. */
  softDelete(
    this: Self & Key,
    options?: Operation.DeleteOptions,
  ): Effect.Effect<Self, DynamoDB.SoftDeleteError, DynamoDB>
  /** `PK`, `SK` and every GSI attribute. */
  keys(this: Self & Key): ItemKeys
  /** The pagination cursor of this item. */
  cursor(this: Self & Key): Effect.Effect<string, never, DynamoDB>
  /** Operations on this item, for `DynamoDB.bulk`. */
  operation(
    this: Self & Key,
    kind: "put",
    options?: Operation.PutOptions,
  ): Operation.TransactionOperation
  operation(
    this: Self & Key,
    kind: "update",
    patch: Partial<Model.Values<F>>,
  ): Operation.UpdateOperation
  operation(
    this: Self & Key,
    kind: "updateRaw",
    attributes: Operation.UpdateRawAttributes<Model.Values<F>>,
    params?: Operation.UpdateRawParams,
  ): Operation.UpdateRawOperation
  operation(
    this: Self & Key,
    kind: "delete",
    options?: Operation.DeleteOptions,
  ): Operation.DeleteOperation
  operation(
    this: Self & Key,
    kind: "softDelete",
    options?: Operation.DeleteOptions,
  ): readonly [Operation.TransactionOperation, Operation.TransactionOperation]
  operation(
    this: Self & Key,
    kind: "condition",
    params: Omit<Operation.ConditionCheckOperation, "_tag" | "key">,
  ): Operation.ConditionCheckOperation
}

/**
 * Reads available on model classes and on unions.
 *
 * @since 7.0.0
 */
export interface DynamoDBReads<Self> {
  /** See `DynamoDB.get`. */
  get(
    key: Key,
    options?: DynamoDB.ReadOptions,
  ): Effect.Effect<Self, DynamoDB.GetError, DynamoDB>
  /** See `DynamoDB.load`: batched with concurrent loads. */
  load(
    key: Key,
    options: DynamoDB.LoadOptions & { readonly recover: true },
  ): Effect.Effect<Self & Recovered, DynamoDB.GetError, DynamoDB>
  load(
    key: Key,
    options?: DynamoDB.LoadOptions,
  ): Effect.Effect<Self, DynamoDB.GetError, DynamoDB>
  /** See `DynamoDB.loadMany`. */
  loadMany(
    keys: Iterable<Key>,
    options?: DynamoDB.ReadOptions,
  ): Effect.Effect<Array<Self>, DynamoDB.GetError, DynamoDB>
  /**
   * See `DynamoDB.query`. Items of other models are left out; an item tagged
   * with this model that does not decode fails with `ItemDecodeError`.
   */
  query(
    params: DynamoDB.QueryParams,
  ): Effect.Effect<DynamoDB.QueryItems<Self>, DynamoDB.QueryError, DynamoDB>
  /** See `DynamoDB.stream`. */
  stream(
    params: DynamoDB.StreamParams,
  ): Stream.Stream<Self, DynamoDB.QueryError, DynamoDB>
  /** See `DynamoDB.paginate`. */
  paginate(
    input: PaginationInput,
    params: DynamoDB.PaginationParams,
  ): Effect.Effect<PaginationResult<Self>, DynamoDB.PaginateError, DynamoDB>
}

/**
 * Members the provider adds to model classes.
 *
 * @since 7.0.0
 */
export interface DynamoDBStatics<Self, F> extends DynamoDBReads<Self> {
  /** See `DynamoDB.updateRaw`. */
  updateRaw(
    key: Key,
    attributes: Operation.UpdateRawAttributes<Model.Values<F>>,
    params?: Operation.UpdateRawParams,
  ): Effect.Effect<Self, DynamoDB.UpdateRawError, DynamoDB>
  /** See `DynamoDB.delete`: unconditional unless `params` add a condition. */
  delete(
    key: Key,
    params?: Operation.ConditionParams,
  ): Effect.Effect<void, DynamoDB.DeleteError, DynamoDB>
  /** Operations on items of this model, for `DynamoDB.bulk`. */
  operation(
    kind: "put",
    item: Self & TableItem,
    options?: Operation.PutOptions,
  ): Operation.TransactionOperation
  operation(
    kind: "update",
    item: Self & TableItem,
    patch: Partial<Model.Values<F>>,
  ): Operation.UpdateOperation
  operation(
    kind: "updateRaw",
    key: Key,
    attributes: Operation.UpdateRawAttributes<Model.Values<F>>,
    params?: Operation.UpdateRawParams,
  ): Operation.UpdateRawOperation
  operation(
    kind: "delete",
    key: Key,
    params?: Operation.ConditionParams,
  ): Operation.DeleteOperation
  operation(
    kind: "softDelete",
    item: Self & TableItem,
    options?: Operation.DeleteOptions,
  ): readonly [Operation.TransactionOperation, Operation.TransactionOperation]
  operation(
    kind: "condition",
    key: Key,
    params: Omit<Operation.ConditionCheckOperation, "_tag" | "key">,
  ): Operation.ConditionCheckOperation
}

/**
 * @since 7.0.0
 */
export interface DynamoDBMembers extends Provider.Members {
  readonly instance: DynamoDBInstance<this["Self"], this["Fields"]>
  readonly static: DynamoDBStatics<this["Self"], this["Fields"]>
  readonly union: DynamoDBReads<this["Self"]>
}

// The implementations below run with `this` bound to the model class, the
// union or the instance. Each states which in its `this` parameter, and the
// declared members above give callers the precise model types.

function load(
  this: Decodable,
  key: Key,
  options: DynamoDB.LoadOptions & { readonly recover: true },
): Effect.Effect<Key & Recovered, DynamoDB.GetError, DynamoDB>
function load(
  this: Decodable,
  key: Key,
  options?: DynamoDB.LoadOptions,
): Effect.Effect<Key, DynamoDB.GetError, DynamoDB>
function load(this: Decodable, key: Key, options?: DynamoDB.LoadOptions) {
  return Table.load(this, key, options)
}

const reads = {
  get(this: Decodable, key: Key, options?: DynamoDB.ReadOptions) {
    return Table.get(this, key, options)
  },
  load,
  loadMany(
    this: Decodable,
    keys: Iterable<Key>,
    options?: DynamoDB.ReadOptions,
  ) {
    return Table.loadMany(this, keys, options)
  },
  query(this: Decodable, params: DynamoDB.QueryParams) {
    return Table.queryItems(this, params)
  },
  stream(this: Decodable, params: DynamoDB.StreamParams) {
    return Table.stream(this, params)
  },
  paginate(
    this: Decodable,
    input: PaginationInput,
    params: DynamoDB.PaginationParams,
  ) {
    return Table.paginate(this, input, params)
  },
} satisfies DynamoDBReads<unknown>

type FieldValues = Readonly<Record<string, unknown>>
type ConditionInput = Omit<Operation.ConditionCheckOperation, "_tag" | "key">

function instanceOperation(
  this: TableItem,
  kind: "put",
  options?: Operation.PutOptions,
): Operation.TransactionOperation
function instanceOperation(
  this: TableItem,
  kind: "update",
  patch: FieldValues,
): Operation.UpdateOperation
function instanceOperation(
  this: TableItem,
  kind: "updateRaw",
  attributes: Operation.UpdateRawAttributes<FieldValues>,
  params?: Operation.UpdateRawParams,
): Operation.UpdateRawOperation
function instanceOperation(
  this: TableItem,
  kind: "delete",
  options?: Operation.DeleteOptions,
): Operation.DeleteOperation
function instanceOperation(
  this: TableItem,
  kind: "softDelete",
  options?: Operation.DeleteOptions,
): readonly [Operation.TransactionOperation, Operation.TransactionOperation]
function instanceOperation(
  this: TableItem,
  kind: "condition",
  params: ConditionInput,
): Operation.ConditionCheckOperation
function instanceOperation(
  this: TableItem,
  ...args:
    | readonly ["put", Operation.PutOptions?]
    | readonly ["update", FieldValues]
    | readonly [
        "updateRaw",
        Operation.UpdateRawAttributes<FieldValues>,
        Operation.UpdateRawParams?,
      ]
    | readonly ["delete", Operation.DeleteOptions?]
    | readonly ["softDelete", Operation.DeleteOptions?]
    | readonly ["condition", ConditionInput]
) {
  switch (args[0]) {
    case "put":
      return Operation.put(this, args[1])
    case "update":
      return Operation.update(this, args[1])[1]
    case "updateRaw":
      return Operation.updateRaw(modelOf(this), this, args[1], args[2])
    case "delete":
      return Operation.deleteItem(this, args[1])
    case "softDelete":
      return Operation.softDelete(this, args[1])
    case "condition":
      return Operation.condition(this, args[1])
  }
}

function classOperation(
  this: TableModel<TableItem>,
  kind: "put",
  item: TableItem,
  options?: Operation.PutOptions,
): Operation.TransactionOperation
function classOperation(
  this: TableModel<TableItem>,
  kind: "update",
  item: TableItem,
  patch: FieldValues,
): Operation.UpdateOperation
function classOperation(
  this: TableModel<TableItem>,
  kind: "updateRaw",
  key: Key,
  attributes: Operation.UpdateRawAttributes<FieldValues>,
  params?: Operation.UpdateRawParams,
): Operation.UpdateRawOperation
function classOperation(
  this: TableModel<TableItem>,
  kind: "delete",
  key: Key,
  params?: Operation.ConditionParams,
): Operation.DeleteOperation
function classOperation(
  this: TableModel<TableItem>,
  kind: "softDelete",
  item: TableItem,
  options?: Operation.DeleteOptions,
): readonly [Operation.TransactionOperation, Operation.TransactionOperation]
function classOperation(
  this: TableModel<TableItem>,
  kind: "condition",
  key: Key,
  params: ConditionInput,
): Operation.ConditionCheckOperation
function classOperation(
  this: TableModel<TableItem>,
  ...args:
    | readonly ["put", TableItem, Operation.PutOptions?]
    | readonly ["update", TableItem, FieldValues]
    | readonly [
        "updateRaw",
        Key,
        Operation.UpdateRawAttributes<FieldValues>,
        Operation.UpdateRawParams?,
      ]
    | readonly ["delete", Key, Operation.ConditionParams?]
    | readonly ["softDelete", TableItem, Operation.DeleteOptions?]
    | readonly ["condition", Key, ConditionInput]
) {
  switch (args[0]) {
    case "put":
      return Operation.put(args[1], args[2])
    case "update":
      return Operation.update(args[1], args[2])[1]
    case "updateRaw":
      return Operation.updateRaw(this, args[1], args[2], args[3])
    case "delete":
      return Operation.delete(args[1], args[2])
    case "softDelete":
      return Operation.softDelete(args[1], args[2])
    case "condition":
      return Operation.condition(args[1], args[2])
  }
}

/**
 * @since 7.0.0
 */
/**
 * The type of `DynamoDB.provider`.
 *
 * @since 7.0.0
 */
export type DynamoDBProvider = Provider.Provider<DynamoDBMembers>

/** The implementation behind `DynamoDB.provider`. */
export const provider: DynamoDBProvider = Provider.make<DynamoDBMembers>()({
  instanceProps: {
    put(this: TableItem, options) {
      return Table.put(this, options)
    },
    update(this: TableItem, patch: FieldValues) {
      return Table.update(this, patch)
    },
    applyUpdate(this: TableItem, patch: FieldValues) {
      return Operation.update(this, patch)
    },
    delete(this: TableItem, options) {
      return Table.deleteItem(this, options)
    },
    softDelete(this: TableItem, options) {
      return Table.softDelete(this, options)
    },
    keys(this: Key) {
      return keysOf(this)
    },
    cursor(this: Key) {
      return Table.cursor(this)
    },
    operation: instanceOperation,
  },
  classProps: {
    ...reads,
    updateRaw(
      this: TableModel<TableItem>,
      key: Key,
      attributes: Operation.UpdateRawAttributes<FieldValues>,
      params?: Operation.UpdateRawParams,
    ) {
      return Table.updateRaw(this, key, attributes, params)
    },
    delete(key, params) {
      return Table.remove(key, params)
    },
    operation: classOperation,
  },
  unionProps: reads,
})
