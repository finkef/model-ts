import { Client, Key, PaginationParams } from "./client"
import {
  DynamoDBModelInstance,
  DynamoDBModelConstructor,
  DynamoDBUnion,
  Decodable,
} from "./dynamodb-model"
import {
  UpdateRawOperation,
  GetOperation,
  PutOperation,
  DeleteOperation,
  ConditionCheckOperation,
  Operation,
  BulkOperation,
} from "./operations"
import { OutputOf, TypeOf, ModelOf } from "@model-ts/core"
import { RaceConditionError } from "./errors"
import { absurd } from "fp-ts/lib/function"
import { encodeDDBCursor, PaginationInput } from "./pagination"
import { GSI_NAMES, GSIPK, GSISK } from "./gsi"

export interface DynamoDBInternals<M extends Decodable> {
  __dynamoDBDecode(
    value: unknown
  ): M extends DynamoDBModelConstructor<any>
    ? InstanceType<M>
    : M extends DynamoDBUnion
    ? InstanceType<M["_models"][number]>
    : never
  __dynamoDBEncode(
    item: DynamoDBModelInstance
  ): M extends DynamoDBModelConstructor<any> ? OutputOf<M> : never
}

export const getProvider = (client: Client) => {
  // operation func overloads with access to client

  function operation<M extends DynamoDBModelConstructor<any>>(
    this: M,
    operation: "get",
    key: Key,
    params?: Omit<GetOperation<M>, "_model" | "_operation" | "key">
  ): GetOperation<M>
  function operation<M extends DynamoDBModelConstructor<any>>(
    this: M,
    operation: "put",
    item: TypeOf<M>,
    params?: Omit<PutOperation<TypeOf<M>, M>, "_model" | "_operation" | "item">
  ): { action: PutOperation<TypeOf<M>, M>; rollback: DeleteOperation<M> }
  function operation<T extends DynamoDBModelInstance>(
    this: ModelOf<T>,
    operation: "update",
    item: T,
    attributes: Partial<TypeOf<ModelOf<T>>>
  ):
    | PutOperation<T, ModelOf<T>>
    | [
        {
          action: PutOperation<T, ModelOf<T>>
          rollback: DeleteOperation<ModelOf<T>>
        },
        {
          action: DeleteOperation<ModelOf<T>>
          rollback: PutOperation<T, ModelOf<T>>
        }
      ]
  function operation<M extends DynamoDBModelConstructor<any>>(
    this: M,
    operation: "updateRaw",
    key: Key,
    attributes: UpdateRawOperation<M>["attributes"],
    params?: Omit<
      UpdateRawOperation<M>,
      "_operation" | "_model" | "attributes" | "key"
    >
  ): UpdateRawOperation<M>
  function operation<M extends DynamoDBModelConstructor<any>>(
    this: M,
    operation: "delete",
    key: Key
  ): DeleteOperation<M>
  function operation<M extends DynamoDBModelConstructor<any>>(
    this: M,
    operation: "softDelete",
    item: TypeOf<M>
  ): [
    {
      action: PutOperation<TypeOf<M>, M>
      rollback: DeleteOperation<M>
    },
    {
      action: DeleteOperation<M>
      rollback: PutOperation<TypeOf<M>, M>
    }
  ]
  function operation(
    operation: "condition",
    key: Key,
    params: Omit<ConditionCheckOperation, "_operation" | "key">
  ): ConditionCheckOperation
  function operation<
    T extends DynamoDBModelInstance,
    M extends DynamoDBModelConstructor<T>
  >(
    this: M,
    operation:
      | "get"
      | "put"
      | "update"
      | "updateRaw"
      | "delete"
      | "softDelete"
      | "condition",
    ...args: any[]
  ): Operation<T, M> | BulkOperation<T, M> | BulkOperation<T, M>[] {
    switch (operation) {
      case "get": {
        const [key, params] = args
        return {
          _operation: "get",
          _model: this,
          key,
          ...params,
        }
      }
      case "put": {
        const [item, params] = args
        return {
          action: {
            _model: this,
            _operation: "put",
            item,
            ...params,
          },
          rollback: {
            _model: this,
            _operation: "delete",
            key: { PK: item.PK, SK: item.SK },
          },
        }
      }
      case "update": {
        const [item, attributes] = args

        const updatedItem = new item._model({
          ...item.values(),
          ...stripUndefinedValues(attributes),
          _docVersion: (item._docVersion ?? 0) + 1,
        }) as T

        if (item.PK === updatedItem.PK && item.SK === updatedItem.SK) {
          // update in place
          return {
            _model: item._model,
            _operation: "put",
            item: updatedItem,
            ...(typeof item._docVersion === "number"
              ? {
                  ConditionExpression: `attribute_not_exists(#docVersion) OR #docVersion = :docVersion`,
                  ExpressionAttributeNames: { "#docVersion": "_docVersion" },
                  ExpressionAttributeValues: {
                    ":docVersion": item._docVersion,
                  },
                }
              : { ConditionExpression: undefined }),
          }
        } else {
          // Replace item since Key changed
          return [
            {
              action: {
                _model: item._model,
                _operation: "put",
                item: updatedItem,
                ConditionExpression: "attribute_not_exists(PK)",
              },
              rollback: {
                _model: item._model,
                _operation: "delete",
                key: { PK: updatedItem.PK, SK: updatedItem.SK },
              },
            },
            {
              action: {
                _model: item._model,
                _operation: "delete",
                key: { PK: item.PK, SK: item.SK },
              },
              rollback: {
                _model: item._model,
                _operation: "put",
                item,
              },
            },
          ]
        }
      }
      case "updateRaw": {
        const [key, attributes, params] = args
        return {
          _model: this,
          _operation: "updateRaw",
          key,
          attributes: stripUndefinedValues(attributes),
          ...params,
        }
      }
      case "delete": {
        const [key] = args
        return {
          _model: this,
          _operation: "delete",
          key,
        }
      }
      case "softDelete": {
        const [item] = args
        return [
          {
            action: {
              _model: this,
              _operation: "delete",
              key: { PK: item.PK, SK: item.SK },
            },
            rollback: { _model: this, _operation: "put", item },
          },
          {
            action: {
              _model: this,
              _operation: "put",
              _deleted: true,
              item,
            },
            rollback: {
              _model: this,
              _operation: "delete",
              key: {
                PK: `$$DELETED$$${item.PK}`,
                SK: `$$DELETED$$${item.SK}`,
              },
            },
          },
        ]
      }
      case "condition": {
        const [key, params] = args
        return {
          _operation: "condition",
          key,
          ...params,
        }
      }

      default:
        return absurd(operation)
    }
  }

  function instanceOperation<T extends DynamoDBModelInstance>(
    this: T,
    operation: "put",
    params?: Omit<PutOperation<T, ModelOf<T>>, "_model" | "_operation" | "item">
  ): {
    action: PutOperation<T, ModelOf<T>>
    rollback: DeleteOperation<ModelOf<T>>
  }
  function instanceOperation<T extends DynamoDBModelInstance>(
    this: T,
    operation: "update",
    attributes: Partial<TypeOf<ModelOf<T>>>
  ):
    | PutOperation<T, ModelOf<T>>
    | [
        {
          action: PutOperation<T, ModelOf<T>>
          rollback: DeleteOperation<ModelOf<T>>
        },
        {
          action: DeleteOperation<ModelOf<T>>
          rollback: PutOperation<T, ModelOf<T>>
        }
      ]
  function instanceOperation<T extends DynamoDBModelInstance>(
    this: T,
    operation: "updateRaw",
    attributes: UpdateRawOperation<ModelOf<T>>["attributes"],
    params?: Omit<
      UpdateRawOperation<ModelOf<T>>,
      "_operation" | "_model" | "attributes" | "key"
    >
  ): UpdateRawOperation<ModelOf<T>>
  function instanceOperation<T extends DynamoDBModelInstance>(
    this: T,
    operation: "delete"
  ): DeleteOperation<ModelOf<T>>
  function instanceOperation<T extends DynamoDBModelInstance>(
    this: T,
    operation: "softDelete"
  ): [
    {
      action: PutOperation<T, ModelOf<T>>
      rollback: DeleteOperation<ModelOf<T>>
    },
    {
      action: DeleteOperation<ModelOf<T>>
      rollback: PutOperation<T, ModelOf<T>>
    }
  ]
  function instanceOperation(
    operation: "condition",
    params: Omit<ConditionCheckOperation, "_operation" | "key">
  ): ConditionCheckOperation
  function instanceOperation<T extends DynamoDBModelInstance>(
    this: T,
    op: "put" | "update" | "updateRaw" | "delete" | "softDelete" | "condition",
    ...args: any[]
  ): BulkOperation<T, ModelOf<T>> | BulkOperation<T, ModelOf<T>>[] {
    switch (op) {
      case "put": {
        const [params] = args
        return (operation as any).call(
          this._model as any,
          "put",
          this,
          params
        ) as any
      }
      case "update": {
        const [attributes] = args
        return (operation as any).call(
          this._model as any,
          "update",
          this,
          stripUndefinedValues(attributes)
        ) as any
      }
      case "updateRaw": {
        const [attributes, params] = args
        return (operation as any).call(
          this._model as any,
          "updateRaw",
          { PK: this.PK, SK: this.SK },
          stripUndefinedValues(attributes),
          params
        ) as any
      }
      case "delete": {
        return (operation as any).call(this._model as any, "delete", {
          PK: this.PK,
          SK: this.SK,
        }) as any
      }
      case "softDelete": {
        return (operation as any).call(
          this._model as any,
          "softDelete",
          this
        ) as any
      }
      case "condition": {
        const [params] = args
        return (operation as any).call(
          this._model as any,
          "condition",
          { PK: this.PK, SK: this.SK },
          params
        ) as any
      }

      default:
        return absurd(op)
    }
  }

  return {
    classProps: {
      dynamodb: client,
      operation,
      __dynamoDBDecode<M extends DynamoDBModelConstructor<any>>(
        this: M,
        value: unknown
      ) {
        const decoded = this.from(value)

        try {
          return Object.assign(decoded, {
            _docVersion: (value as any)._docVersion ?? 0,
          })
        } catch (error) {
          return decoded
        }
      },
      __dynamoDBEncode<
        T extends DynamoDBModelInstance,
        M extends DynamoDBModelConstructor<T>
      >(this: M, item: T) {
        const encoded = item.encode()

        return Object.assign(encoded, item.keys(), {
          _docVersion:
            typeof item._docVersion === "number" ? item._docVersion : 0,
        })
      },

      get<M extends DynamoDBModelConstructor<any>>(
        this: M,
        key: Key,
        params?: Omit<GetOperation<M>, "_model" | "_operation" | "key">
      ) {
        return client.get<M>({
          _model: this,
          _operation: "get",
          key,
          ...params,
        })
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
        return client.load<M, Null, Recover>(
          {
            _model: this,
            _operation: "get",
            key,
            ...params,
          },
          { null: params?.null, recover: params?.recover }
        )
      },

      loadMany<M extends DynamoDBModelConstructor<any>>(
        this: M,
        keys: Key[],
        params?: Omit<GetOperation<M>, "_model" | "_operation" | "key">
      ) {
        return client.loadMany<M>(
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
        return client.paginate(this, args, params)
      },

      put<M extends DynamoDBModelConstructor<any>>(
        this: M,
        item: TypeOf<M>,
        params?: Omit<
          PutOperation<TypeOf<M>, M>,
          "_model" | "_operation" | "item"
        >
      ) {
        return client.put({
          _model: this,
          _operation: "put",
          item,
          ...params,
        })
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
        return client.updateRaw<M>({
          _model: this,
          _operation: "updateRaw",
          key,
          attributes: stripUndefinedValues(attributes),
          ...params,
        })
      },

      delete<M extends DynamoDBModelConstructor<any>>(this: M, key: Key) {
        return client.delete<M>({
          _model: this,
          _operation: "delete",
          key,
        })
      },

      softDelete<M extends DynamoDBModelConstructor<any>>(
        this: M,
        item: TypeOf<M>
      ) {
        return client.softDelete<TypeOf<M>>(item)
      },
    },
    instanceProps: {
      dynamodb: client,
      keys<T extends DynamoDBModelInstance>(
        this: T
      ): { PK: string; SK: string } & { [key in GSIPK]?: string } &
        { [key in GSISK]?: string } {
        return {
          PK: this.PK,
          SK: this.SK,
          ...GSI_NAMES.map((GSI) => ({
            [`${GSI}PK`]: this[`${GSI}PK`],
            [`${GSI}SK`]: this[`${GSI}SK`],
          })).reduce((acc, cur) => Object.assign(acc, cur), {}),
        }
      },
      cursor<T extends DynamoDBModelInstance>(this: T) {
        return encodeDDBCursor(this.keys(), client.cursorEncryptionKey)
      },
      put<T extends DynamoDBModelInstance>(
        this: T,
        params?: Omit<
          PutOperation<T, ModelOf<T>>,
          "_model" | "_operation" | "item"
        >
      ) {
        return client.put({
          _model: this._model,
          _operation: "put",
          item: this,
          ...params,
        })
      },
      async update<T extends DynamoDBModelInstance>(
        this: T,
        attributes: Partial<ModelOf<T>["_codec"]["_A"]>
      ) {
        const op = (this.operation as typeof instanceOperation)(
          "update",
          stripUndefinedValues(attributes)
        )

        if (!Array.isArray(op)) {
          // Update in place
          try {
            return await client.put(op)
          } catch (error) {
            if (error.code === "ConditionalCheckFailedException")
              throw new RaceConditionError(
                "The instance you are attempting to update is out of sync with the stored value."
              )

            throw error
          }
        } else {
          // Keys changed
          try {
            const updatedItem = op[0].action.item

            await client.bulk(op)

            return updatedItem
          } catch (error) {
            throw error
          }
        }
      },
      applyUpdate<T extends DynamoDBModelInstance>(
        this: T,
        attributes: Partial<ModelOf<T>["_codec"]["_A"]>
      ): [T, BulkOperation<T, ModelOf<T>> | BulkOperation<T, ModelOf<T>>[]] {
        const op = (this.operation as typeof instanceOperation)(
          "update",
          stripUndefinedValues(attributes)
        )

        const updatedItem = Array.isArray(op) ? op[0].action.item : op.item

        return [updatedItem, op]
      },
      delete<T extends DynamoDBModelInstance>(this: T): Promise<null> {
        const { PK, SK } = this.keys()

        return client.delete({
          _model: this._model,
          _operation: "delete",
          key: { PK, SK },
        })
      },
      softDelete<T extends DynamoDBModelInstance>(this: T) {
        return client.softDelete<T>(this)
      },
      operation: instanceOperation,
    },

    unionProps: {
      dynamodb: client,
      __dynamoDBDecode<M extends DynamoDBUnion>(this: M, value: unknown) {
        const decoded = this.from(value)

        try {
          return Object.assign(decoded, {
            _docVersion: (value as any)._docVersion ?? 0,
          })
        } catch (error) {
          return decoded
        }
      },
      async get<M extends DynamoDBUnion>(
        this: M,
        key: Key,
        params?: Omit<GetOperation<M>, "_model" | "_operation" | "key">
      ) {
        return client.get<M>({
          _model: this,
          _operation: "get",
          key: { PK: key.PK, SK: key.SK },
          ...params,
        })
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
        return client.load<M, Null, Recover>(
          {
            _model: this,
            _operation: "get",
            key,
            ...params,
          },
          { null: params?.null, recover: params?.recover }
        )
      },

      loadMany<M extends DynamoDBUnion>(
        this: M,
        keys: Key[],
        params?: Omit<GetOperation<M>, "_model" | "_operation" | "key">
      ) {
        return client.loadMany<M>(
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
        return client.paginate(this, args, params)
      },
    },
  }
}

export type DynamoDBProvider = ReturnType<typeof getProvider>

function stripUndefinedValues<T extends Record<string, any>>(obj: T): T {
  return Object.fromEntries(
    Object.entries(obj).filter(([_key, value]) => typeof value !== "undefined")
  ) as T
}
