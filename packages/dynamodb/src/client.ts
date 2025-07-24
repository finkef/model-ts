import { DocumentClient, ClientApiVersions } from "aws-sdk/clients/dynamodb"
import { ServiceConfigurationOptions } from "aws-sdk/lib/service"
import { AWSError } from "aws-sdk/lib/error"
import { pipe, absurd } from "fp-ts/lib/function"
import * as A from "fp-ts/lib/Array"
import * as E from "fp-ts/lib/Either"
import * as TE from "fp-ts/lib/TaskEither"
import { retrying } from "retry-ts/lib/Task"
import { monoidRetryPolicy, constantDelay, limitRetries } from "retry-ts"
import DataLoader from "dataloader"
import {
  BulkOperation,
  PutOperation,
  GetOperation,
  UpdateRawOperation,
  DeleteOperation,
  Operation,
  ConditionCheckOperation,
  isTransactionOperation,
} from "./operations"
import {
  DynamoDBError,
  KeyExistsError,
  ItemNotFoundError,
  ConditionalCheckFailedError,
  BulkWriteTransactionError,
  BulkWriteRollbackError,
} from "./errors"
import {
  Decodable,
  DecodableInstance,
  DynamoDBModelConstructor,
  DynamoDBModelInstance,
  DynamoDBUnion,
} from "./dynamodb-model"
import { DynamoDBInternals } from "./provider"
import {
  decodeDDBCursor,
  decodePagination,
  encodeDDBCursor,
  PaginationDirection,
  PaginationInput,
  PaginationOptions,
  PaginationResult,
} from "./pagination"
import { GSI, GSI_NAMES, GSIPK, GSISK } from "./gsi"

export type QueryParams = Omit<
  DocumentClient.QueryInput,
  "TableName" | "KeyConditionExpression"
> &
  Required<Pick<DocumentClient.QueryInput, "KeyConditionExpression">> & {
    FetchAllPages?: boolean
  }

export type QueryResponse<T extends { [name: string]: any }> = {
  [K in keyof T]: DecodableInstance<T[K]>[]
} & {
  _unknown: unknown[]
  meta: { lastEvaluatedKey?: DocumentClient.Key }
}

export type PaginationParams = Omit<
  QueryParams,
  "FetchAllPages" | "Limit" | "ExclusiveStartKey" | "ScanIndexForward"
>

export interface BulkWriteState {
  inRollback?: boolean
  rollbackSuccessful?: boolean
  // TODO: handle different errors
  transactionError?: DynamoDBError
  rollbackError?: DynamoDBError
  successful: BulkOperation<any, any>[]
  rollbackSuccess: BulkOperation<any, any>[]
  rollbackFailure: BulkOperation<any, any>[]
}

export interface ClientProps
  extends DocumentClient.DocumentClientOptions,
    ServiceConfigurationOptions,
    ClientApiVersions {
  tableName: string

  /**
   * The encryption key used to encrypt cursors using AES-256-CTR.
   * Must be a 32 character string, 256 bits.
   */
  cursorEncryptionKey?: Buffer

  /**
   * Defaults for pagination.
   */
  paginationOptions?: PaginationOptions
}

export interface Key {
  PK: string
  SK: string
}

export class Client {
  tableName: string
  documentClient: DocumentClient
  dataLoader: DataLoader<GetOperation<Decodable>, DynamoDBModelInstance, string>
  cursorEncryptionKey?: Buffer
  paginationOptions?: PaginationOptions

  constructor(props: ClientProps) {
    this.tableName = props?.tableName
    this.cursorEncryptionKey = props?.cursorEncryptionKey
    this.paginationOptions = props?.paginationOptions
    this.documentClient = new DocumentClient(props)
    this.dataLoader = new DataLoader<
      GetOperation<Decodable>,
      DynamoDBModelInstance,
      string
    >(
      async (operations) => {
        const map = Object.fromEntries(operations.map((op, i) => [i, op]))
        const stronglyConsistent = operations.some(
          ({ ConsistentRead }) => ConsistentRead
        )

        const results = await this.batchGet(map, {
          stronglyConsistent,
          individualErrors: true,
        })

        return operations.map((_, i) => results[i])
      },
      {
        maxBatchSize: 100,
        cacheKeyFn: ({ key: { PK, SK } }) => `${PK}::${SK}`,
        // Don't cache for now
        cache: false,
      }
    )
  }

  /**
   * Sets a new table name, intended to use in test setups.
   *
   * @param tableName
   */
  setTableName(tableName: string) {
    this.tableName = tableName
  }

  /**
   * Sets a new DocumentClient, intended to use in test setups.
   * @param docClient
   */
  setDocumentClient(docClient: DocumentClient) {
    this.documentClient = docClient
  }

  async put<
    T extends DynamoDBModelInstance,
    M extends DynamoDBModelConstructor<T>
  >({
    _model,
    _deleted,
    item,
    IgnoreExistence,
    ...params
  }: PutOperation<T, M>): Promise<T> {
    try {
      const encoded = (_model as M & DynamoDBInternals<M>).__dynamoDBEncode(
        item
      )

      await this.documentClient
        .put({
          TableName: this.tableName,
          // Maybe add deletion prefix if this is part of a soft-delete
          Item: _deleted ? this.applySoftDeletionFields(encoded) : encoded,
          ConditionExpression: IgnoreExistence
            ? undefined
            : "attribute_not_exists(PK)",
          ...params,
        })
        .promise()

      // @ts-ignore
      item._docVersion = encoded._docVersion

      return item
    } catch (error) {
      if (
        error.code === "ConditionalCheckFailedException" &&
        !params?.ConditionExpression
      ) {
        throw new KeyExistsError()
      }

      throw error
    }
  }

  async get<M extends Decodable>({
    _model,
    key,
    ...params
  }: GetOperation<M>): Promise<DecodableInstance<M>> {
    const { Item } = await this.documentClient
      .get({
        TableName: this.tableName,
        Key: { PK: key.PK, SK: key.SK },
        ...params,
      })
      .promise()

    if (!Item) throw new ItemNotFoundError()

    return (_model as M & DynamoDBInternals<M>).__dynamoDBDecode(Item) as any
  }

  async load<
    M extends Decodable,
    Null extends boolean = false,
    Recover extends boolean = false
  >(
    operation: GetOperation<M>,
    params?: { null?: Null; recover?: Recover }
  ): Promise<
    Recover extends true
      ? Null extends true
        ? (DecodableInstance<M> & { isDeleted?: true }) | null
        : DecodableInstance<M> & { isDeleted?: true }
      : Null extends true
      ? DecodableInstance<M> | null
      : DecodableInstance<M>
  > {
    const item = await this.dataLoader.load(operation).catch((e) => {
      // Maybe return null instead of throwing
      if (e instanceof ItemNotFoundError) {
        // Try to recover a deleted item
        if (params?.recover)
          return this.dataLoader
            .load({
              ...operation,
              key: {
                PK: `$$DELETED$$${operation.key.PK}`,
                SK: `$$DELETED$$${operation.key.SK}`,
              },
            })
            .then((item) => Object.assign(item, { isDeleted: true }))
            .catch((e2) => {
              if (e2 instanceof ItemNotFoundError && params.null) return null

              // Throw the original error
              throw e
            })

        if (params?.null) return null
      }

      throw e
    })

    return item as any
  }

  async loadMany<M extends Decodable>(
    operations: GetOperation<M>[]
  ): Promise<Array<DecodableInstance<M> | Error>> {
    const itemsOrErrors = await this.dataLoader.loadMany(operations)
    return itemsOrErrors as any
  }

  async updateRaw<M extends DynamoDBModelConstructor<any>>(
    operation: UpdateRawOperation<M>
  ): Promise<InstanceType<M>> {
    const { _model, key, attributes, ...params } = operation

    const {
      UpdateExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    } = this.buildUpdateExpression(operation)

    try {
      const { Attributes } = await this.documentClient
        .update({
          TableName: this.tableName,
          Key: key,
          ReturnValues: "ALL_NEW",
          ConditionExpression:
            "attribute_exists(PK) and attribute_not_exists(dynamotorLegacy)",
          UpdateExpression,
          ...params,
          ExpressionAttributeNames:
            // Omit if empty
            Object.keys(ExpressionAttributeNames).length > 0
              ? ExpressionAttributeNames
              : undefined,
          ExpressionAttributeValues:
            // Omit if empty
            Object.keys(ExpressionAttributeValues).length > 0
              ? ExpressionAttributeValues
              : undefined,
        })
        .promise()

      return (_model as M & DynamoDBInternals<M>).__dynamoDBDecode(Attributes)
    } catch (error) {
      if (error.code === "ConditionalCheckFailedException") {
        if (params.ConditionExpression) throw new ConditionalCheckFailedError()
        else throw new ItemNotFoundError()
      }

      throw error
    }
  }

  /**
   * Builds an UpdateExpression, ExpressionAttributeNames object and ExpressionAttributeValues
   * object.
   *
   * ExpressionAttributeNames and ExpressionAttributeValues are merged if provided in params.
   */
  private buildUpdateExpression<M extends DynamoDBModelConstructor<any>>({
    _model,
    key,
    attributes,
    ...params
  }: UpdateRawOperation<M>) {
    /**
     * We cannot set GSI values to null after they have been set, so we need to actually `REMOVE`
     * them. In order to do that, we partition all attribute updates into `SET` and `REMOVE` arrays.
     */
    const { left: removeAttributes, right: setAttributes } = pipe(
      Object.entries(attributes).filter(
        ([, value]) => typeof value !== "undefined"
      ),
      A.map(([key, value]) => [
        key,
        _model.encodeProp(key as any, value as any),
      ]),
      A.partitionMap(([key, value]) =>
        // We need to remove a value, if it's used in a GSI and the value is null
        // TODO: handle differently in new version
        (key as string).startsWith("GSI") && value === null
          ? E.left([key, value])
          : E.right([key, value])
      )
    )

    const sanitizedKeySet = new Set()
    const sanitizeKey = (key: string) => {
      const alphaNum = key.replace(/[^a-zA-Z\d]/g, "")

      // In case of a collision, append x + the current size of the key set
      if (sanitizedKeySet.has(alphaNum)) {
        const withPostfix = `${alphaNum}x${sanitizedKeySet.size}`
        sanitizedKeySet.add(withPostfix)
        return withPostfix
      } else {
        sanitizedKeySet.add(alphaNum)
        return alphaNum
      }
    }

    const set = setAttributes.reduce<{
      UpdateExpression: string
      ExpressionAttributeNames: { [key: string]: string }
      ExpressionAttributeValues: { [key: string]: any }
    }>(
      (acc, [key, value], index) => {
        const sanitizedKey = sanitizeKey(key)
        const attributeName = `#${sanitizedKey}`
        const attributeValue = `:${sanitizedKey}`

        return {
          UpdateExpression: `${acc.UpdateExpression}${
            index == 0 ? "" : ","
          } ${attributeName} = ${attributeValue}`,
          ExpressionAttributeNames: {
            ...acc.ExpressionAttributeNames,
            [attributeName]: key,
          },
          ExpressionAttributeValues: {
            ...acc.ExpressionAttributeValues,
            [attributeValue]: value,
          },
        }
      },
      {
        UpdateExpression: "SET ",
        ExpressionAttributeNames: params?.ExpressionAttributeNames ?? {},
        ExpressionAttributeValues: params?.ExpressionAttributeValues ?? {},
      }
    )

    const remove = removeAttributes.reduce<{
      UpdateExpression: string
      ExpressionAttributeNames: { [key: string]: string }
    }>(
      (acc, [key], index) => {
        const sanitizedKey = `${key.replace(/[^a-zA-Z\d]/g, "")}`
        const attributeName = `#${sanitizedKey}`

        return {
          UpdateExpression: `${acc.UpdateExpression}${
            index == 0 ? "" : ","
          } ${attributeName}`,
          ExpressionAttributeNames: {
            ...acc.ExpressionAttributeNames,
            [attributeName]: key,
          },
        }
      },
      {
        UpdateExpression: "REMOVE ",
        ExpressionAttributeNames: params?.ExpressionAttributeNames ?? {},
      }
    )

    // Merge set and remove
    return {
      UpdateExpression:
        (setAttributes.length ? set.UpdateExpression : "") +
        " " +
        (removeAttributes.length ? remove.UpdateExpression : ""),
      ExpressionAttributeNames: {
        ...set.ExpressionAttributeNames,
        ...remove.ExpressionAttributeNames,
      },
      ExpressionAttributeValues: set.ExpressionAttributeValues,
    }
  }

  async delete<M extends DynamoDBModelConstructor<any>>({
    key,
  }: DeleteOperation<M>): Promise<null> {
    await this.documentClient
      .delete({
        TableName: this.tableName,
        Key: key,
      })
      .promise()

    return null
  }

  /**
   * Updates all item keys to be prefixed with "$$DELETED$$" and adds a "_deletedAt" field.
   */
  public async softDelete<T extends DynamoDBModelInstance>(
    item: T
  ): Promise<T> {
    await this.bulk([
      {
        _operation: "delete",
        _model: item._model,
        key: { PK: item.PK, SK: item.SK },
      },
      { _operation: "put", _model: item._model, _deleted: true, item },
    ])

    return item
  }

  async query<M extends Decodable, R extends { [name: string]: M }>(
    { FetchAllPages, ...params }: QueryParams,
    models: R
  ): Promise<QueryResponse<R>> {
    const query = async (
      ExclusiveStartKey?: DocumentClient.Key
    ): Promise<{ Items: unknown[]; LastEvaluatedKey?: DocumentClient.Key }> => {
      const { Items, LastEvaluatedKey } = await this.documentClient
        .query({
          TableName: this.tableName,
          FilterExpression: "attribute_not_exists(dynamotorLegacy)",
          ExclusiveStartKey,
          ...params,
        })
        .promise()

      if (LastEvaluatedKey && FetchAllPages) {
        // Recursively fetch next page
        const nextPage = await query(LastEvaluatedKey)
        return {
          Items: [...(Items ?? []), ...(nextPage.Items ?? [])],
          LastEvaluatedKey: nextPage.LastEvaluatedKey,
        }
      } else return { Items: Items ?? [], LastEvaluatedKey }
    }

    // Fetch pages
    const { Items, LastEvaluatedKey: lastEvaluatedKey } = await query()

    const grouped: { [K in keyof R]: DecodableInstance<R[K]> } = <any>{}

    // Init object
    Object.keys(models).forEach((key: keyof R) => {
      grouped[key] = [] as any
    })

    // Collect unmatched entries
    const unknown: unknown[] = []

    // Adds the item to the grouped object if one model returns right
    const matcher = (item: unknown) => {
      // TODO: first try models matching `_tag`

      for (const key in models) {
        try {
          const decoded = (
            models[key] as any as DynamoDBInternals<M>
          ).__dynamoDBDecode(item)
          grouped[key].push(decoded as any)

          // Early exit
          return
        } catch (error) {
          // Didn't match codec
        }
      }

      // As fallback add to unknown array
      unknown.push(item)
    }

    // Execute matcher
    Items.forEach(matcher)

    return { ...grouped, _unknown: unknown, meta: { lastEvaluatedKey } }
  }

  async paginate<M extends Decodable>(
    model: M,
    args: PaginationInput,
    params: PaginationParams
  ): Promise<PaginationResult<DecodableInstance<M>>> {
    const { cursor, limit, direction } = decodePagination(
      args,
      this.paginationOptions
    )

    const { results } = await this.query(
      {
        ...params,
        // Fetch one additional item to test for a next page
        Limit: limit + 1,
        ExclusiveStartKey: cursor
          ? decodeDDBCursor(
              cursor,
              // GSI1 is the inverse index and uses PK and SK (switched around)
              params.IndexName && params.IndexName !== "GSI1"
                ? (params.IndexName as GSI)
                : undefined,
              this.cursorEncryptionKey
            )
          : undefined,
        ScanIndexForward: direction === PaginationDirection.FORWARD,
      },
      { results: model }
    )

    // Strip extra item
    const slice = results.slice(0, limit)

    // Reverse if necessary
    if (direction === PaginationDirection.BACKWARD) slice.reverse()

    // Build edges
    const edges = slice.map((item: any) => ({
      node: item,
      cursor: encodeDDBCursor(item, this.cursorEncryptionKey),
    }))

    return {
      pageInfo: {
        hasPreviousPage:
          direction === PaginationDirection.BACKWARD &&
          results.length === limit + 1,
        hasNextPage:
          direction === PaginationDirection.FORWARD &&
          results.length === limit + 1,
        startCursor: edges[0]?.cursor ?? null,
        endCursor: edges[edges.length - 1]?.cursor ?? null,
      },
      edges,
    }
  }

  /**
   * Get a batch of items. Calls batchGet recursively if UnprocessedKeys are returned by DynamoDB.
   * If you need to load > 100 items, use `load` instead.
   *
   * @param requests - Limited to 100 items
   */
  async batchGet<
    R extends Record<string, GetOperation<any>>,
    IndividualErrors extends boolean = false
  >(
    requests: R,
    params?: {
      stronglyConsistent?: boolean
      individualErrors?: IndividualErrors
    }
  ): Promise<
    IndividualErrors extends true
      ? { [K in keyof R]: DecodableInstance<R[K]["_model"]> | Error }
      : { [K in keyof R]: DecodableInstance<R[K]["_model"]> }
  > {
    // Early exit if requests are empty
    if (!Object.keys(requests).length) return {} as any

    const requestsMap = new Map<
      string,
      GetOperation<any> & { fieldNames: string[] }
    >()

    const getIdentifier = (key: { PK: string; SK: string }) =>
      `${key.PK}::${key.SK}`

    Object.entries(requests).forEach(([fieldName, operation]) => {
      const identifier = getIdentifier(operation.key)
      const existingEntry = requestsMap.get(identifier)

      if (existingEntry) {
        existingEntry.fieldNames.push(fieldName)
      } else {
        requestsMap.set(identifier, {
          ...operation,
          fieldNames: [fieldName],
        })
      }
    })

    const fetchBatch = async (
      keys: Array<{ PK: string; SK: string }>
    ): Promise<Array<{ PK: string; SK: string; [key: string]: unknown }>> => {
      const { Responses, UnprocessedKeys } = await this.documentClient
        .batchGet({
          RequestItems: {
            [this.tableName]: {
              Keys: keys,
              ConsistentRead: params?.stronglyConsistent,
            },
          },
        })
        .promise()

      const responses =
        (Responses?.[this.tableName] as Array<{
          PK: string
          SK: string
          [key: string]: unknown
        }>) ?? []
      const unprocessedKeys =
        (UnprocessedKeys?.[this.tableName]?.Keys as Array<{
          PK: string
          SK: string
        }>) ?? []

      if (!responses.length && unprocessedKeys.length)
        throw new Error("Received only unprocessed keys")

      if (!unprocessedKeys.length) return responses
      else {
        // Call recursively and concat
        const nextBatch = await fetchBatch(unprocessedKeys)
        return [...responses, ...nextBatch]
      }
    }

    const responses = await fetchBatch(
      [...requestsMap.values()].map(({ key: { PK, SK } }) => ({ PK, SK }))
    )

    const responsesMap = Object.fromEntries(
      responses.map((response) => [getIdentifier(response), response])
    )

    // Throw for full batch
    if (responses.length < requestsMap.size && !params?.individualErrors)
      throw new ItemNotFoundError()

    return Object.fromEntries(
      [...requestsMap.entries()].flatMap(
        ([identifier, { _model, fieldNames }]) => {
          const response = responsesMap[identifier]

          const decoded = !response
            ? new ItemNotFoundError()
            : (_model as DynamoDBInternals<any>).__dynamoDBDecode(response)

          return fieldNames.map((fieldName) => [fieldName, decoded])
        }
      )
    ) as any
  }

  /**
   * Runs a series of operations using DynamoDB's `transactWrite` API. If > 25 operations are specified,
   * multiple calls are made.
   *
   * In case of a failure in a consecutive call, actions will be attempted to be rolled back.
   *
   * @param operations
   */
  async bulk(
    operations: (
      | BulkOperation<DynamoDBModelInstance, DynamoDBModelConstructor<any>>
      | BulkOperation<DynamoDBModelInstance, DynamoDBModelConstructor<any>>[]
    )[]
  ): Promise<BulkWriteState> {
    const result = await this.executeBulkTransaction(
      operations.map((op) => (Array.isArray(op) ? op : [op])).flat()
    )

    if (E.isLeft(result)) {
      const { rollbackSuccessful, transactionError, rollbackFailure } =
        result.left

      if (rollbackSuccessful) throw transactionError
      else throw new BulkWriteRollbackError(rollbackFailure)
    }

    return result.right
  }

  private async executeBulkTransaction(
    operations: BulkOperation<
      DynamoDBModelInstance,
      DynamoDBModelConstructor<any>
    >[],
    state: BulkWriteState = {
      successful: [],
      rollbackSuccess: [],
      rollbackFailure: [],
    }
  ): Promise<E.Either<BulkWriteState, BulkWriteState>> {
    // Base case reached
    if (!operations.length)
      return state.inRollback
        ? E.left({ ...state, rollbackSuccessful: true })
        : E.right(state)

    const [currentBatch, remaining] = A.splitAt(25)(operations)

    try {
      const transactItems = currentBatch
        .map((operation) => {
          if (isTransactionOperation(operation))
            return state.inRollback ? operation.rollback! : operation.action
          else return state.inRollback ? null! : operation
        })
        .filter(Boolean)
        .map(this.operationToTransactItem)

      if (transactItems.length) await this.transactWrite(transactItems)

      if (state.inRollback)
        return await this.executeBulkTransaction(remaining, {
          ...state,
          rollbackSuccess: [...state.rollbackSuccess, ...currentBatch],
        })

      return await this.executeBulkTransaction(remaining, {
        ...state,
        successful: [...state.successful, ...currentBatch],
      })
    } catch (error) {
      // Already in rollback, but failed again. Terminate.
      if (state.inRollback)
        return E.left({
          ...state,
          rollbackError: error,
          rollbackFailure: operations,
        })

      // Failed for the first time, start rollback.
      return await this.executeBulkTransaction(
        // Rollback all successful operations
        state.successful,
        {
          inRollback: true,
          transactionError: error,
          successful: state.successful,
          rollbackFailure: [],
          rollbackSuccess: [],
        }
      )
    }
  }

  private async transactWrite(
    ops: DocumentClient.TransactWriteItem[]
  ): Promise<DocumentClient.TransactWriteItemsOutput> {
    const retryPolicy = monoidRetryPolicy.concat(
      constantDelay(50),
      limitRetries(3)
    )

    const execute = retrying(
      retryPolicy,
      () =>
        TE.tryCatch(
          () =>
            this.documentClient
              .transactWrite({
                TransactItems: ops,
              })
              .promise(),
          (e) =>
            (e as AWSError).code === "TransactionCanceledException"
              ? new BulkWriteTransactionError(e as AWSError)
              : (e as Error)
        ),
      // Retry only if there was a network error, etc.
      (result) =>
        E.isLeft(result) && !(result.left instanceof BulkWriteTransactionError)
    )

    const either = await execute()

    if (E.isLeft(either)) throw either.left
    else return either.right
  }

  private operationToTransactItem = <
    T extends DynamoDBModelInstance,
    M extends DynamoDBModelConstructor<T>
  >(
    operation: Operation<T, M> | ConditionCheckOperation
  ): DocumentClient.TransactWriteItem => {
    switch (operation._operation) {
      case "put": {
        const { _model, _deleted, item, IgnoreExistence, ...params } = operation
        debugger
        const encoded = (_model as M & DynamoDBInternals<M>).__dynamoDBEncode(
          item
        )
        return {
          Put: {
            TableName: this.tableName,
            // maybe apply deletion prefix
            Item: _deleted ? this.applySoftDeletionFields(encoded) : encoded,
            ConditionExpression: IgnoreExistence
              ? undefined
              : "attribute_not_exists(PK)",
            ...params,
          },
        }
      }
      case "updateRaw": {
        const { _model, _operation, key, attributes, ...params } = operation

        const {
          UpdateExpression,
          ExpressionAttributeNames,
          ExpressionAttributeValues,
        } = this.buildUpdateExpression(operation)
        return {
          Update: {
            Key: key,
            TableName: this.tableName,
            UpdateExpression,
            ConditionExpression:
              "attribute_exists(PK) and attribute_not_exists(dynamotorLegacy)",
            ...params,
            ExpressionAttributeNames:
              Object.keys(ExpressionAttributeNames).length > 0
                ? ExpressionAttributeNames
                : undefined,
            ExpressionAttributeValues:
              Object.keys(ExpressionAttributeValues).length > 0
                ? ExpressionAttributeValues
                : undefined,
          },
        }
      }
      case "delete":
        return {
          Delete: {
            Key: operation.key,
            TableName: this.tableName,
          },
        }
      case "condition":
        const { key, _operation, ...rest } = operation
        return {
          ConditionCheck: {
            TableName: this.tableName,
            Key: key,
            ...rest,
          },
        }
      case "get":
        return absurd(operation as never)
    }
  }

  private applySoftDeletionFields<
    T extends {
      PK: string
      SK: string
    } & { [key in GSIPK]?: string } &
      { [key in GSISK]?: string }
  >(item: T): T {
    const prefix = "$$DELETED$$"

    const maybeWithPrefix = (str?: string) =>
      typeof str === "string" ? `${prefix}${str}` : undefined

    return {
      _deletedAt: new Date().toISOString(),
      ...item,
      PK: maybeWithPrefix(item.PK),
      SK: maybeWithPrefix(item.SK),
      ...GSI_NAMES.map((GSI) => ({
        [`${GSI}PK`]: maybeWithPrefix(item[`${GSI}PK` as const]),
        [`${GSI}SK`]: maybeWithPrefix(item[`${GSI}SK` as const]),
      })).reduce((acc, cur) => Object.assign(acc, cur), {}),
    }
  }
}
