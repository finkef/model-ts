/**
 * The `dynamodb` provider: table operations as model members.
 *
 * ```ts
 * class Post extends model<Post>()("Post", dynamodb, { id: PostId, title: Schema.String }) {
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
import type { Values } from "@model-ts/core"
import { makeProvider, type ProviderMembers } from "@model-ts/core"
import type { Effect, Option, Stream } from "effect"
import * as Dynamo from "./Dynamo.ts"
import type { DynamoDB } from "./DynamoDB.ts"
import type { Decodable, Recovered, TableModel } from "./Item.ts"
import { type ItemKeys, type Key, keysOf } from "./Keys.ts"
import * as Operation from "./Operation.ts"
import type { PaginationInput, PaginationResult } from "./Pagination.ts"

/**
 * Members the provider adds to instances. They need `PK` and `SK` on the
 * instance, so a model without them cannot call them.
 *
 * @since 7.0.0
 */
export interface DynamoDBInstance<Self, F> {
  /** See `Dynamo.put`. */
  put(
    this: Self & Key,
    options?: Operation.PutOptions,
  ): Effect.Effect<Self, Dynamo.PutError, DynamoDB>
  /** See `Dynamo.update`. Returns the updated instance. */
  update(
    this: Self & Key,
    patch: Partial<Values<F>>,
  ): Effect.Effect<Self, Dynamo.UpdateError, DynamoDB>
  /** The updated instance and the operation that stores it, for `Dynamo.bulk`. */
  applyUpdate(
    this: Self & Key,
    patch: Partial<Values<F>>,
  ): readonly [Self, Operation.UpdateOperation]
  /** See `Dynamo.deleteItem`: version-guarded unless `ignoreVersion`. */
  delete(
    this: Self & Key,
    options?: Operation.DeleteOptions,
  ): Effect.Effect<void, Dynamo.DeleteItemError, DynamoDB>
  /** See `Dynamo.softDelete`. */
  softDelete(
    this: Self & Key,
    options?: Operation.DeleteOptions,
  ): Effect.Effect<Self, Dynamo.SoftDeleteError, DynamoDB>
  /** `PK`, `SK` and every GSI attribute. */
  keys(this: Self & Key): ItemKeys
  /** The pagination cursor of this item. */
  cursor(this: Self & Key): Effect.Effect<string, never, DynamoDB>
  /** Operations on this item, for `Dynamo.bulk`. */
  operation(
    this: Self & Key,
    kind: "put",
    options?: Operation.PutOptions,
  ): Operation.TransactionOperation
  operation(
    this: Self & Key,
    kind: "update",
    patch: Partial<Values<F>>,
  ): Operation.UpdateOperation
  operation(
    this: Self & Key,
    kind: "updateRaw",
    attributes: Operation.UpdateRawAttributes<Values<F>>,
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
  /** See `Dynamo.get`. */
  get(
    key: Key,
    options?: Dynamo.ReadOptions,
  ): Effect.Effect<Self, Dynamo.GetError, DynamoDB>
  /** See `Dynamo.load`: batched with concurrent loads. */
  load(
    key: Key,
    options: Dynamo.LoadOptions & { readonly recover: true },
  ): Effect.Effect<Self & Recovered, Dynamo.GetError, DynamoDB>
  load(
    key: Key,
    options?: Dynamo.LoadOptions,
  ): Effect.Effect<Self, Dynamo.GetError, DynamoDB>
  /** See `Dynamo.loadOption`. */
  loadOption(
    key: Key,
    options: Dynamo.LoadOptions & { readonly recover: true },
  ): Effect.Effect<
    Option.Option<Self & Recovered>,
    Exclude<Dynamo.GetError, { readonly _tag: "ItemNotFoundError" }>,
    DynamoDB
  >
  loadOption(
    key: Key,
    options?: Dynamo.LoadOptions,
  ): Effect.Effect<
    Option.Option<Self>,
    Exclude<Dynamo.GetError, { readonly _tag: "ItemNotFoundError" }>,
    DynamoDB
  >
  /** See `Dynamo.loadMany`. */
  loadMany(
    keys: Iterable<Key>,
    options?: Dynamo.ReadOptions,
  ): Effect.Effect<Array<Self>, Dynamo.GetError, DynamoDB>
  /** See `Dynamo.query`. Items this model cannot decode are left out. */
  query(
    params: Dynamo.QueryParams,
  ): Effect.Effect<Dynamo.QueryItems<Self>, Dynamo.QueryError, DynamoDB>
  /** See `Dynamo.stream`. */
  stream(
    params: Dynamo.StreamParams,
  ): Stream.Stream<Self, Dynamo.QueryError, DynamoDB>
  /** See `Dynamo.paginate`. */
  paginate(
    input: PaginationInput,
    params: Dynamo.PaginationParams,
  ): Effect.Effect<PaginationResult<Self>, Dynamo.PaginateError, DynamoDB>
}

/**
 * Members the provider adds to model classes.
 *
 * @since 7.0.0
 */
export interface DynamoDBStatics<Self, F> extends DynamoDBReads<Self> {
  /** See `Dynamo.put`. */
  put(
    item: Self & Key,
    options?: Operation.PutOptions,
  ): Effect.Effect<Self, Dynamo.PutError, DynamoDB>
  /** See `Dynamo.updateRaw`. */
  updateRaw(
    key: Key,
    attributes: Operation.UpdateRawAttributes<Values<F>>,
    params?: Operation.UpdateRawParams,
  ): Effect.Effect<Self, Dynamo.UpdateRawError, DynamoDB>
  /** See `Dynamo.delete`: unconditional unless `params` add a condition. */
  delete(
    key: Key,
    params?: Operation.ConditionParams,
  ): Effect.Effect<void, Dynamo.DeleteError, DynamoDB>
  /** See `Dynamo.softDelete`. */
  softDelete(
    item: Self & Key,
    options?: Operation.DeleteOptions,
  ): Effect.Effect<Self, Dynamo.SoftDeleteError, DynamoDB>
  /** Operations on items of this model, for `Dynamo.bulk`. */
  operation(
    kind: "put",
    item: Self & Key,
    options?: Operation.PutOptions,
  ): Operation.TransactionOperation
  operation(
    kind: "update",
    item: Self & Key,
    patch: Partial<Values<F>>,
  ): Operation.UpdateOperation
  operation(
    kind: "updateRaw",
    key: Key,
    attributes: Operation.UpdateRawAttributes<Values<F>>,
    params?: Operation.UpdateRawParams,
  ): Operation.UpdateRawOperation
  operation(
    kind: "delete",
    key: Key,
    params?: Operation.ConditionParams,
  ): Operation.DeleteOperation
  operation(
    kind: "softDelete",
    item: Self & Key,
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
export interface DynamoDBMembers extends ProviderMembers {
  readonly instance: DynamoDBInstance<this["Self"], this["Fields"]>
  readonly static: DynamoDBStatics<this["Self"], this["Fields"]>
  readonly union: DynamoDBReads<this["Self"]>
}

type AnyItem = Key & { values(): Record<string, unknown> }

// Provider members run with `this` bound to the model class (or union); these
// helpers name what `this` is there.
const decodable = (self: unknown) => self as Decodable
const tableModel = (self: unknown) => self as TableModel

const reads = {
  get(key: Key, options?: Dynamo.ReadOptions) {
    return Dynamo.get(decodable(this), key, options)
  },
  load(key: Key, options?: Dynamo.LoadOptions) {
    return Dynamo.load(decodable(this), key, options)
  },
  loadOption(key: Key, options?: Dynamo.LoadOptions) {
    return Dynamo.loadOption(decodable(this), key, options)
  },
  loadMany(keys: Iterable<Key>, options?: Dynamo.ReadOptions) {
    return Dynamo.loadMany(decodable(this), keys, options)
  },
  query(params: Dynamo.QueryParams) {
    return Dynamo.queryItems(decodable(this), params)
  },
  stream(params: Dynamo.StreamParams) {
    return Dynamo.stream(decodable(this), params)
  },
  paginate(input: PaginationInput, params: Dynamo.PaginationParams) {
    return Dynamo.paginate(decodable(this), input, params)
  },
} satisfies DynamoDBReads<any>

/**
 * @since 7.0.0
 */
export const dynamodb = makeProvider<DynamoDBMembers>()({
  instanceProps: {
    put(options) {
      return Dynamo.put(this, options)
    },
    update(patch) {
      return Dynamo.update(this as AnyItem, patch)
    },
    applyUpdate(patch) {
      return Operation.update(this as AnyItem, patch)
    },
    delete(options) {
      return Dynamo.deleteItem(this, options)
    },
    softDelete(options) {
      return Dynamo.softDelete(this, options)
    },
    keys() {
      return keysOf(this)
    },
    cursor() {
      return Dynamo.cursor(this)
    },
    operation(kind: string, ...args: ReadonlyArray<any>): any {
      switch (kind) {
        case "put":
          return Operation.put(this, args[0])
        case "update":
          return Operation.update(this as AnyItem, args[0])[1]
        case "updateRaw":
          return Operation.updateRaw(
            tableModel(this.constructor),
            this,
            args[0],
            args[1],
          )
        case "delete":
          return Operation.deleteItem(this, args[0])
        case "softDelete":
          return Operation.softDelete(this, args[0])
        case "condition":
          return Operation.condition(this, args[0])
      }
      throw new TypeError(`Unknown operation "${kind}"`)
    },
  },
  classProps: {
    ...reads,
    put(item, options) {
      return Dynamo.put(item, options)
    },
    updateRaw(key, attributes, params) {
      return Dynamo.updateRaw(tableModel(this), key, attributes, params)
    },
    delete(key, params) {
      return Dynamo.delete(key, params)
    },
    softDelete(item, options) {
      return Dynamo.softDelete(item, options)
    },
    operation(kind: string, ...args: ReadonlyArray<any>): any {
      switch (kind) {
        case "put":
          return Operation.put(args[0], args[1])
        case "update":
          return Operation.update(args[0], args[1])[1]
        case "updateRaw":
          return Operation.updateRaw(
            tableModel(this),
            args[0],
            args[1],
            args[2],
          )
        case "delete":
          return Operation.delete(args[0], args[1])
        case "softDelete":
          return Operation.softDelete(args[0], args[1])
        case "condition":
          return Operation.condition(args[0], args[1])
      }
      throw new TypeError(`Unknown operation "${kind}"`)
    },
  },
  unionProps: reads,
})
