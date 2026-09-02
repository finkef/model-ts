import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import { ModelOf, RuntimeTypeValidationError, TypeOf } from "@model-ts/core"
import {
  BatchGetResult,
  BulkWriteState,
  Client,
  ClientProps,
  Key,
  LoadResult,
  PaginationParams,
  QueryIteratorParams,
  QueryParams,
  QueryResponse,
} from "./client"
import {
  Decodable,
  DecodableInstance,
  DynamoDBModelConstructor,
  DynamoDBModelInstance,
  DynamoDBUnion,
} from "./dynamodb-model"
import * as errors from "./errors"
import {
  BulkOperation,
  DeleteOperation,
  GetOperation,
  PutOperation,
  UpdateRawOperation,
} from "./operations"
import { PaginationInput, PaginationResult } from "./pagination"
import { getProvider } from "./provider"

export * from "./errors"

export class DynamoDBClientError extends Data.TaggedError(
  "DynamoDBClientError"
)<{
  readonly operation: string
  readonly cause: unknown
}> {}

export type KnownDynamoDBError =
  | errors.KeyExistsError
  | errors.ItemNotFoundError
  | errors.ConditionalCheckFailedError
  | errors.RaceConditionError
  | errors.BulkWriteTransactionError
  | errors.BulkWriteRollbackError
  | errors.PaginationError
  | errors.NotSupportedError
  | RuntimeTypeValidationError

export type PutEffectError = errors.KeyExistsError | DynamoDBClientError
export type GetEffectError =
  | errors.ItemNotFoundError
  | RuntimeTypeValidationError
  | DynamoDBClientError
export type LoadError<Null extends boolean> =
  | (Null extends true
      ? RuntimeTypeValidationError
      : errors.ItemNotFoundError | RuntimeTypeValidationError)
  | DynamoDBClientError
export type LoadManyEffectError = DynamoDBClientError
export type UpdateRawEffectError =
  | errors.ConditionalCheckFailedError
  | errors.ItemNotFoundError
  | RuntimeTypeValidationError
  | DynamoDBClientError
export type DeleteEffectError = DynamoDBClientError
export type BulkError =
  | errors.BulkWriteTransactionError
  | errors.BulkWriteRollbackError
export type SoftDeleteEffectError = BulkError | DynamoDBClientError
export type QueryEffectError = RuntimeTypeValidationError | DynamoDBClientError
export type IteratorEffectError =
  | RuntimeTypeValidationError
  | DynamoDBClientError
export type PaginateEffectError =
  | errors.PaginationError
  | RuntimeTypeValidationError
  | DynamoDBClientError
export type BatchGetEffectError =
  | errors.ItemNotFoundError
  | RuntimeTypeValidationError
  | DynamoDBClientError
export type BulkEffectError = BulkError | DynamoDBClientError

const KNOWN = [
  errors.KeyExistsError,
  errors.ItemNotFoundError,
  errors.ConditionalCheckFailedError,
  errors.RaceConditionError,
  errors.BulkWriteTransactionError,
  errors.BulkWriteRollbackError,
  errors.PaginationError,
  errors.NotSupportedError,
  RuntimeTypeValidationError,
]

const mapError =
  <E extends KnownDynamoDBError>(operation: string) =>
  (cause: unknown): E | DynamoDBClientError =>
    KNOWN.some((ErrorClass) => cause instanceof ErrorClass)
      ? (cause as E)
      : new DynamoDBClientError({ operation, cause })

export interface EffectClient {
  readonly client: Client
  readonly put: <
    T extends DynamoDBModelInstance,
    M extends DynamoDBModelConstructor<T>
  >(
    operation: PutOperation<T, M>
  ) => Effect.Effect<T, PutEffectError>
  readonly get: <M extends Decodable>(
    operation: GetOperation<M>
  ) => Effect.Effect<DecodableInstance<M>, GetEffectError>
  readonly load: <
    M extends Decodable,
    Null extends boolean = false,
    Recover extends boolean = false
  >(
    operation: GetOperation<M>,
    params?: { null?: Null; recover?: Recover }
  ) => Effect.Effect<LoadResult<M, Null, Recover>, LoadError<Null>>
  readonly loadMany: <M extends Decodable>(
    operations: GetOperation<M>[]
  ) => Effect.Effect<Array<DecodableInstance<M> | Error>, LoadManyEffectError>
  readonly updateRaw: <M extends DynamoDBModelConstructor<any>>(
    operation: UpdateRawOperation<M>
  ) => Effect.Effect<InstanceType<M>, UpdateRawEffectError>
  readonly delete: <M extends DynamoDBModelConstructor<any>>(
    operation: DeleteOperation<M>
  ) => Effect.Effect<null, DeleteEffectError>
  readonly softDelete: <T extends DynamoDBModelInstance>(
    item: T
  ) => Effect.Effect<T, SoftDeleteEffectError>
  readonly query: <M extends Decodable, R extends { [name: string]: M }>(
    params: QueryParams,
    models: R
  ) => Effect.Effect<QueryResponse<R>, QueryEffectError>
  readonly iterator: <M extends Decodable>(
    params: QueryIteratorParams,
    model: M
  ) => Stream.Stream<DecodableInstance<M>[], IteratorEffectError>
  readonly paginate: <M extends Decodable>(
    model: M,
    args: PaginationInput,
    params: PaginationParams
  ) => Effect.Effect<
    PaginationResult<DecodableInstance<M>>,
    PaginateEffectError
  >
  readonly batchGet: <
    R extends Record<string, GetOperation<any>>,
    IndividualErrors extends boolean = false
  >(
    requests: R,
    params?: {
      stronglyConsistent?: boolean
      individualErrors?: IndividualErrors
    }
  ) => Effect.Effect<BatchGetResult<R, IndividualErrors>, BatchGetEffectError>
  readonly bulk: (
    operations: (
      | BulkOperation<DynamoDBModelInstance, DynamoDBModelConstructor<any>>
      | BulkOperation<DynamoDBModelInstance, DynamoDBModelConstructor<any>>[]
    )[]
  ) => Effect.Effect<BulkWriteState, BulkEffectError>
}

export const makeEffectClient = (client: Client): EffectClient => ({
  client,
  put: Effect.fn("@model-ts/dynamodb/put")(
    <T extends DynamoDBModelInstance, M extends DynamoDBModelConstructor<T>>(
      operation: PutOperation<T, M>
    ) =>
      Effect.tryPromise({
        try: () => client.put(operation),
        catch: mapError<errors.KeyExistsError>("put"),
      })
  ),
  get: Effect.fn("@model-ts/dynamodb/get")(
    <M extends Decodable>(operation: GetOperation<M>) =>
      Effect.tryPromise({
        try: () => client.get(operation),
        catch: mapError<errors.ItemNotFoundError | RuntimeTypeValidationError>(
          "get"
        ),
      })
  ),
  load: Effect.fn("@model-ts/dynamodb/load")(
    <
      M extends Decodable,
      Null extends boolean = false,
      Recover extends boolean = false
    >(
      operation: GetOperation<M>,
      params?: { null?: Null; recover?: Recover }
    ) =>
      Effect.tryPromise({
        try: () => client.load<M, Null, Recover>(operation, params),
        catch:
          mapError<
            Null extends true
              ? RuntimeTypeValidationError
              : errors.ItemNotFoundError | RuntimeTypeValidationError
          >("load"),
      })
  ),
  loadMany: Effect.fn("@model-ts/dynamodb/loadMany")(
    <M extends Decodable>(operations: GetOperation<M>[]) =>
      Effect.tryPromise({
        try: () => client.loadMany(operations),
        catch: mapError<never>("loadMany"),
      })
  ),
  updateRaw: Effect.fn("@model-ts/dynamodb/updateRaw")(
    <M extends DynamoDBModelConstructor<any>>(
      operation: UpdateRawOperation<M>
    ) =>
      Effect.tryPromise({
        try: () => client.updateRaw(operation),
        catch: mapError<
          | errors.ConditionalCheckFailedError
          | errors.ItemNotFoundError
          | RuntimeTypeValidationError
        >("updateRaw"),
      })
  ),
  delete: Effect.fn("@model-ts/dynamodb/delete")(
    <M extends DynamoDBModelConstructor<any>>(operation: DeleteOperation<M>) =>
      Effect.tryPromise({
        try: () => client.delete(operation),
        catch: mapError<never>("delete"),
      })
  ),
  softDelete: Effect.fn("@model-ts/dynamodb/softDelete")(
    <T extends DynamoDBModelInstance>(item: T) =>
      Effect.tryPromise({
        try: () => client.softDelete(item),
        catch: mapError<BulkError>("softDelete"),
      })
  ),
  query: Effect.fn("@model-ts/dynamodb/query")(
    <M extends Decodable, R extends { [name: string]: M }>(
      params: QueryParams,
      models: R
    ) =>
      Effect.tryPromise({
        try: () => client.query(params, models),
        catch: mapError<RuntimeTypeValidationError>("query"),
      })
  ),
  iterator: <M extends Decodable>(
    params: QueryIteratorParams,
    model: M
  ): Stream.Stream<DecodableInstance<M>[], IteratorEffectError> =>
    Stream.suspend(() =>
      Stream.fromAsyncIterable(
        client.iterator(params, model),
        mapError<RuntimeTypeValidationError>("iterator")
      )
    ),
  paginate: Effect.fn("@model-ts/dynamodb/paginate")(
    <M extends Decodable>(
      model: M,
      args: PaginationInput,
      params: PaginationParams
    ) =>
      Effect.tryPromise({
        try: () => client.paginate(model, args, params),
        catch: mapError<errors.PaginationError | RuntimeTypeValidationError>(
          "paginate"
        ),
      })
  ),
  batchGet: Effect.fn("@model-ts/dynamodb/batchGet")(
    <
      R extends Record<string, GetOperation<any>>,
      IndividualErrors extends boolean = false
    >(
      requests: R,
      params?: {
        stronglyConsistent?: boolean
        individualErrors?: IndividualErrors
      }
    ) =>
      Effect.tryPromise({
        try: () => client.batchGet<R, IndividualErrors>(requests, params),
        catch: mapError<errors.ItemNotFoundError | RuntimeTypeValidationError>(
          "batchGet"
        ),
      })
  ),
  bulk: Effect.fn("@model-ts/dynamodb/bulk")(
    (
      operations: (
        | BulkOperation<DynamoDBModelInstance, DynamoDBModelConstructor<any>>
        | BulkOperation<DynamoDBModelInstance, DynamoDBModelConstructor<any>>[]
      )[]
    ) =>
      Effect.tryPromise({
        try: () => client.bulk(operations),
        catch: mapError<BulkError>("bulk"),
      })
  ),
})

export const getEffectProvider = (client: Client) => {
  const base = getProvider(client)
  const fx = makeEffectClient(client)

  return {
    classProps: {
      ...base.classProps,
      get<M extends DynamoDBModelConstructor<any>>(
        this: M,
        key: Key,
        params?: Omit<GetOperation<M>, "_model" | "_operation" | "key">
      ) {
        return fx.get({ _model: this, _operation: "get", key, ...params })
      },
      load<
        M extends DynamoDBModelConstructor<any>,
        Null extends boolean = false,
        Recover extends boolean = false
      >(
        this: M,
        key: Key,
        params?: Omit<GetOperation<M>, "_model" | "_operation" | "key"> & {
          null?: Null
          recover?: Recover
        }
      ) {
        return fx.load<M, Null, Recover>(
          { _model: this, _operation: "get", key, ...params },
          { null: params?.null, recover: params?.recover }
        )
      },
      loadMany<M extends DynamoDBModelConstructor<any>>(
        this: M,
        keys: Key[],
        params?: Omit<GetOperation<M>, "_model" | "_operation" | "key">
      ) {
        return fx.loadMany(
          keys.map((key) => ({
            _model: this,
            _operation: "get",
            key,
            ...params,
          }))
        )
      },
      paginate<M extends DynamoDBModelConstructor<any>>(
        this: M,
        args: PaginationInput,
        params: PaginationParams
      ) {
        return fx.paginate(this, args, params)
      },
      query<M extends DynamoDBModelConstructor<any>>(
        this: M,
        params: QueryParams
      ) {
        return fx
          .query(params, { items: this })
          .pipe(Effect.map(({ items, meta }) => Object.assign(items, { meta })))
      },
      iterator<M extends DynamoDBModelConstructor<any>>(
        this: M,
        params: QueryIteratorParams
      ) {
        return fx.iterator(params, this)
      },
      put<M extends DynamoDBModelConstructor<any>>(
        this: M,
        item: TypeOf<M>,
        params?: Omit<
          PutOperation<TypeOf<M>, M>,
          "_model" | "_operation" | "item"
        >
      ) {
        return fx.put({ _model: this, _operation: "put", item, ...params })
      },
      updateRaw<M extends DynamoDBModelConstructor<any>>(
        this: M,
        key: Key,
        attributes: UpdateRawOperation<M>["attributes"],
        params?: Omit<
          UpdateRawOperation<M>,
          "_operation" | "_model" | "attributes" | "key"
        >
      ) {
        return fx.updateRaw({
          _model: this,
          _operation: "updateRaw",
          key,
          attributes,
          ...params,
        })
      },
      delete<M extends DynamoDBModelConstructor<any>>(this: M, key: Key) {
        return fx.delete({ _model: this, _operation: "delete", key })
      },
      softDelete<M extends DynamoDBModelConstructor<any>>(
        this: M,
        item: TypeOf<M>
      ) {
        return fx.softDelete(item)
      },
    },
    instanceProps: {
      ...base.instanceProps,
      put<T extends DynamoDBModelInstance>(
        this: T,
        params?: Omit<
          PutOperation<T, ModelOf<T>>,
          "_model" | "_operation" | "item"
        >
      ) {
        return fx.put({
          _model: this._model,
          _operation: "put",
          item: this,
          ...params,
        })
      },
      update<T extends DynamoDBModelInstance>(
        this: T,
        attributes: Partial<ModelOf<T>["_codec"]["_A"]>
      ) {
        return Effect.suspend<
          T,
          errors.RaceConditionError | BulkError | DynamoDBClientError,
          never
        >(() => {
          const [updated, operations] = this.applyUpdate(attributes)

          if (Array.isArray(operations))
            return fx.bulk(operations).pipe(Effect.as(updated))

          return Effect.tryPromise({
            try: () => client.put(operations),
            catch: (cause): errors.RaceConditionError | DynamoDBClientError =>
              (cause as any)?.code === "ConditionalCheckFailedException"
                ? new errors.RaceConditionError(
                    "The instance you are attempting to update is out of sync with the stored value."
                  )
                : mapError<never>("update")(cause),
          }).pipe(Effect.as(updated))
        })
      },
      delete<T extends DynamoDBModelInstance>(this: T) {
        const { PK, SK } = this.keys()
        return fx.delete({
          _model: this._model,
          _operation: "delete",
          key: { PK, SK },
        })
      },
      softDelete<T extends DynamoDBModelInstance>(this: T) {
        return fx.softDelete(this)
      },
    },
    unionProps: {
      ...base.unionProps,
      get<M extends DynamoDBUnion>(
        this: M,
        key: Key,
        params?: Omit<GetOperation<M>, "_model" | "_operation" | "key">
      ) {
        return fx.get({ _model: this, _operation: "get", key, ...params })
      },
      load<
        M extends DynamoDBUnion,
        Null extends boolean = false,
        Recover extends boolean = false
      >(
        this: M,
        key: Key,
        params?: Omit<GetOperation<M>, "_model" | "_operation" | "key"> & {
          null?: Null
          recover?: Recover
        }
      ) {
        return fx.load<M, Null, Recover>(
          { _model: this, _operation: "get", key, ...params },
          { null: params?.null, recover: params?.recover }
        )
      },
      loadMany<M extends DynamoDBUnion>(
        this: M,
        keys: Key[],
        params?: Omit<GetOperation<M>, "_model" | "_operation" | "key">
      ) {
        return fx.loadMany(
          keys.map((key) => ({
            _model: this,
            _operation: "get",
            key,
            ...params,
          }))
        )
      },
      paginate<M extends DynamoDBUnion>(
        this: M,
        args: PaginationInput,
        params: PaginationParams
      ) {
        return fx.paginate(this, args, params)
      },
      query<M extends DynamoDBUnion>(this: M, params: QueryParams) {
        return fx
          .query(params, { items: this })
          .pipe(Effect.map(({ items, meta }) => Object.assign(items, { meta })))
      },
      iterator<M extends DynamoDBUnion>(this: M, params: QueryIteratorParams) {
        return fx.iterator(params, this)
      },
    },
  }
}

export type DynamoDBEffectProvider = ReturnType<typeof getEffectProvider>

export class DynamoDB extends Context.Service<DynamoDB, EffectClient>()(
  "@model-ts/dynamodb/DynamoDB"
) {
  static readonly layer = (props: ClientProps) =>
    Layer.sync(DynamoDB, () => makeEffectClient(new Client(props)))
  static readonly layerFromClient = (client: Client) =>
    Layer.succeed(DynamoDB, makeEffectClient(client))
}
