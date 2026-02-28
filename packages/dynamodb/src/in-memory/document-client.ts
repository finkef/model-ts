import { NotSupportedError } from "../errors"
import {
  DocumentPathPart,
  evaluateConditionExpression,
  parseKeyConditionExpression,
  ParsedUpdateExpression,
  parseUpdateExpression,
} from "./expression"
import {
  InMemoryTableState,
  PRIMARY_INDEX_NAME,
  isSupportedIndexName,
  matchesKeyConditionDescriptor,
  parseIndexName,
  isGSI,
} from "./store"
import { IN_MEMORY_SPEC, InMemoryIndexName } from "./spec"
import { cloneItem, encodeItemKey } from "./utils"

interface PromiseRequest<T> {
  promise: () => Promise<T>
}

type AnyParams = { [key: string]: any }

export interface InMemoryDocumentClient {
  get(params: AnyParams): PromiseRequest<{ Item?: any }>
  put(params: AnyParams): PromiseRequest<{}>
  update(params: AnyParams): PromiseRequest<{ Attributes?: any }>
  delete(params: AnyParams): PromiseRequest<{}>
  query(params: AnyParams): PromiseRequest<{
    Items?: any[]
    Count: number
    ScannedCount: number
    LastEvaluatedKey?: any
  }>
  scan(params: AnyParams): PromiseRequest<{
    Items?: any[]
    Count: number
    ScannedCount: number
    LastEvaluatedKey?: any
  }>
  batchGet(params: AnyParams): PromiseRequest<{ Responses?: { [tableName: string]: any[] } }>
  batchWrite(params: AnyParams): PromiseRequest<{ UnprocessedItems: { [tableName: string]: any[] } }>
  transactWrite(params: AnyParams): PromiseRequest<{}>
  __inMemorySnapshot(tableName: string): { [key: string]: any }
  __inMemoryResetTable(tableName: string): void
}

class InMemoryDocumentClientImpl implements InMemoryDocumentClient {
  private readonly tables = new Map<string, InMemoryTableState>()

  get(params: AnyParams): PromiseRequest<{ Item?: any }> {
    return this.request("get", params, async () => {
      this.assertSupportedParams("get", params)
      const tableName = this.getRequiredTableName(params)
      const key = this.getRequiredKey(params, "Key")

      const table = this.getTable(tableName)
      const item = table.cloneItemByKey(key)

      return item ? { Item: item } : {}
    })
  }

  put(params: AnyParams): PromiseRequest<{}> {
    return this.request("put", params, async () => {
      this.assertSupportedParams("put", params)
      const tableName = this.getRequiredTableName(params)
      const table = this.getTable(tableName)

      const item = params.Item
      if (!item || typeof item !== "object") {
        throw new NotSupportedError({
          method: "put",
          featurePath: "put.Item",
          reason: "Item must be an object.",
        })
      }

      this.assertPrimaryKey(item, "put")
      this.assertExpressionAttributeInputs(params, [params.ConditionExpression])

      const existing = table.cloneItemByKey({ PK: item.PK, SK: item.SK })
      this.assertCondition("put", params, existing)

      table.put(item)

      return {}
    })
  }

  update(params: AnyParams): PromiseRequest<{ Attributes?: any }> {
    return this.request("update", params, async () => {
      this.assertSupportedParams("update", params)
      const tableName = this.getRequiredTableName(params)
      const key = this.getRequiredKey(params, "Key")

      if (typeof params.UpdateExpression !== "string") {
        throw new NotSupportedError({
          method: "update",
          featurePath: "update.UpdateExpression",
          reason: "UpdateExpression is required.",
        })
      }

      const returnValues = params.ReturnValues ?? "NONE"
      if (!["NONE", "ALL_NEW"].includes(returnValues)) {
        throw this.validationError(
          `Unsupported ReturnValues value: ${returnValues}`
        )
      }
      this.assertExpressionAttributeInputs(params, [
        params.ConditionExpression,
        params.UpdateExpression,
      ])

      const table = this.getTable(tableName)
      const existing = table.cloneItemByKey(key)

      this.assertCondition("update", params, existing)

      const base = existing ?? { PK: key.PK, SK: key.SK }
      const parsed = this.parseUpdateExpressionOrValidationError(
        params.UpdateExpression,
        {
          method: "update",
          expressionAttributeNames: params.ExpressionAttributeNames,
          expressionAttributeValues: params.ExpressionAttributeValues,
          item: base,
        }
      )

      const next = cloneItem(base)
      this.applyParsedUpdateExpression(next, parsed)

      this.assertPrimaryKey(next, "update")
      table.put(next)

      if (returnValues === "ALL_NEW") {
        return { Attributes: cloneItem(next) }
      }

      return {}
    })
  }

  delete(params: AnyParams): PromiseRequest<{}> {
    return this.request("delete", params, async () => {
      this.assertSupportedParams("delete", params)
      const tableName = this.getRequiredTableName(params)
      const key = this.getRequiredKey(params, "Key")

      const table = this.getTable(tableName)
      const existing = table.cloneItemByKey(key)
      this.assertExpressionAttributeInputs(params, [params.ConditionExpression])

      this.assertCondition("delete", params, existing)

      table.deleteByKey(key)
      return {}
    })
  }

  query(params: AnyParams): PromiseRequest<{
    Items?: any[]
    Count: number
    ScannedCount: number
    LastEvaluatedKey?: any
  }> {
    return this.request("query", params, async () => {
      this.assertSupportedParams("query", params)
      const tableName = this.getRequiredTableName(params)
      const table = this.getTable(tableName)

      if (typeof params.KeyConditionExpression !== "string") {
        throw new NotSupportedError({
          method: "query",
          featurePath: "query.KeyConditionExpression",
          reason: "KeyConditionExpression is required.",
        })
      }

      const indexName = this.resolveIndexName(params.IndexName)

      if (params.ConsistentRead && isGSI(indexName)) {
        throw this.validationError(
          "Consistent read cannot be true when querying a GSI"
        )
      }
      this.assertExpressionAttributeInputs(params, [
        params.KeyConditionExpression,
        params.FilterExpression,
      ])

      const condition = this.parseKeyConditionOrValidationError(
        params.KeyConditionExpression,
        {
          method: "query",
          expressionAttributeNames: params.ExpressionAttributeNames,
          expressionAttributeValues: params.ExpressionAttributeValues,
        }
      )

      if (!matchesKeyConditionDescriptor(indexName, condition)) {
        throw new NotSupportedError({
          method: "query",
          featurePath: "query.KeyConditionExpression",
          reason: "KeyConditionExpression attributes do not match the selected index.",
        })
      }

      if (typeof condition.hashValue !== "string") {
        throw new NotSupportedError({
          method: "query",
          featurePath: "query.KeyConditionExpression.partitionValue",
          reason: "Partition key values must be strings.",
        })
      }

      const scanIndexForward = params.ScanIndexForward !== false
      const limit = this.normalizeLimit(params.Limit, "query")
      const exclusiveStartKey = params.ExclusiveStartKey
        ? this.resolveExclusiveStartKey(params.ExclusiveStartKey, indexName, table)
        : undefined

      const iterator = table.iterateQueryCandidates({
        indexName,
        hashKey: condition.hashValue,
        rangeCondition: condition.range?.condition,
        scanIndexForward,
        exclusiveStartKey,
      })[Symbol.iterator]()

      const items: any[] = []
      let scannedCount = 0
      let lastEvaluatedKey: any | undefined

      let next = iterator.next()
      while (!next.done) {
        const candidate = next.value
        scannedCount += 1

        const include = params.FilterExpression
          ? this.evaluateConditionOrValidationError(
              params.FilterExpression,
              candidate.item,
              {
                method: "query",
                expressionAttributeNames: params.ExpressionAttributeNames,
                expressionAttributeValues: params.ExpressionAttributeValues,
              },
              "FilterExpression"
            )
          : true

        if (include) {
          items.push(candidate.item)
        }

        if (limit !== undefined && scannedCount >= limit) {
          lastEvaluatedKey = this.buildLastEvaluatedKey(indexName, candidate.item)
          break
        }

        next = iterator.next()
      }

      return {
        Items: items,
        Count: items.length,
        ScannedCount: scannedCount,
        LastEvaluatedKey: lastEvaluatedKey,
      }
    })
  }

  scan(params: AnyParams): PromiseRequest<{
    Items?: any[]
    Count: number
    ScannedCount: number
    LastEvaluatedKey?: any
  }> {
    return this.request("scan", params, async () => {
      this.assertSupportedParams("scan", params)

      const tableName = this.getRequiredTableName(params)
      const table = this.getTable(tableName)
      const limit = this.normalizeLimit(params.Limit, "scan")
      this.assertExpressionAttributeInputs(params, [params.FilterExpression])

      const exclusiveStartKey = params.ExclusiveStartKey
        ? this.getRequiredKey({ Key: params.ExclusiveStartKey }, "ExclusiveStartKey")
        : undefined

      const all = table.scanItems(exclusiveStartKey)

      const items: any[] = []
      let scannedCount = 0
      let lastEvaluatedKey: any | undefined

      for (let index = 0; index < all.length; index += 1) {
        const candidate = all[index]
        scannedCount += 1

        const include = params.FilterExpression
          ? this.evaluateConditionOrValidationError(
              params.FilterExpression,
              candidate,
              {
                method: "scan",
                expressionAttributeNames: params.ExpressionAttributeNames,
                expressionAttributeValues: params.ExpressionAttributeValues,
              },
              "FilterExpression"
            )
          : true

        if (include) {
          items.push(candidate)
        }

        if (limit !== undefined && scannedCount >= limit) {
          lastEvaluatedKey = { PK: candidate.PK, SK: candidate.SK }
          break
        }
      }

      return {
        Items: items,
        Count: items.length,
        ScannedCount: scannedCount,
        LastEvaluatedKey: lastEvaluatedKey,
      }
    })
  }

  batchGet(params: AnyParams): PromiseRequest<{ Responses?: { [tableName: string]: any[] } }> {
    return this.request("batchGet", params, async () => {
      this.assertSupportedParams("batchGet", params)

      if (!params.RequestItems || typeof params.RequestItems !== "object") {
        throw new NotSupportedError({
          method: "batchGet",
          featurePath: "batchGet.RequestItems",
          reason: "RequestItems is required.",
        })
      }

      const responses: { [tableName: string]: any[] } = {}

      for (const [tableName, request] of Object.entries<any>(params.RequestItems)) {
        if (!request || typeof request !== "object") {
          throw new NotSupportedError({
            method: "batchGet",
            featurePath: "batchGet.RequestItems",
            reason: "Each RequestItems entry must be an object.",
          })
        }

        const keys = Array.isArray(request.Keys) ? request.Keys : []

        if (keys.length > 100) {
          throw this.validationError(
            "Too many items requested for the BatchGetItem call"
          )
        }

        const table = this.getTable(tableName)
        const tableResponses: any[] = []
        const unique = new Set<string>()

        for (const key of keys) {
          const parsedKey = this.getRequiredKey({ Key: key }, "Key")
          const dedupKey = `${parsedKey.PK}::${parsedKey.SK}`
          if (unique.has(dedupKey)) {
            throw this.validationError(
              "Provided list of item keys contains duplicates"
            )
          }
          unique.add(dedupKey)

          const item = table.cloneItemByKey(parsedKey)
          if (item) tableResponses.push(item)
        }

        responses[tableName] = tableResponses
      }

      return { Responses: responses }
    })
  }

  batchWrite(params: AnyParams): PromiseRequest<{ UnprocessedItems: { [tableName: string]: any[] } }> {
    return this.request("batchWrite", params, async () => {
      this.assertSupportedParams("batchWrite", params)

      if (!params.RequestItems || typeof params.RequestItems !== "object") {
        throw new NotSupportedError({
          method: "batchWrite",
          featurePath: "batchWrite.RequestItems",
          reason: "RequestItems is required.",
        })
      }

      for (const [tableName, requests] of Object.entries<any[]>(params.RequestItems)) {
        if (!Array.isArray(requests)) {
          throw new NotSupportedError({
            method: "batchWrite",
            featurePath: "batchWrite.RequestItems",
            reason: "Each RequestItems entry must be an array.",
          })
        }

        if (requests.length > 25) {
          throw this.validationError(
            "Too many items requested for the BatchWriteItem call"
          )
        }

        const table = this.getTable(tableName)

        for (const entry of requests) {
          if (entry.PutRequest) {
            const item = entry.PutRequest.Item
            if (!item || typeof item !== "object") {
              throw new NotSupportedError({
                method: "batchWrite",
                featurePath: "batchWrite.RequestItems.PutRequest.Item",
                reason: "PutRequest.Item must be an object.",
              })
            }

            this.assertPrimaryKey(item, "batchWrite")
            table.put(item)
            continue
          }

          if (entry.DeleteRequest) {
            const key = this.getRequiredKey(
              { Key: entry.DeleteRequest.Key },
              "DeleteRequest.Key"
            )
            table.deleteByKey(key)
            continue
          }

          throw this.validationError(
            "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes"
          )
        }
      }

      return { UnprocessedItems: {} }
    })
  }

  transactWrite(params: AnyParams): PromiseRequest<{}> {
    return this.request("transactWrite", params, async () => {
      this.assertSupportedParams("transactWrite", params)

      const transactItems = params.TransactItems
      if (!Array.isArray(transactItems) || transactItems.length === 0) {
        throw new NotSupportedError({
          method: "transactWrite",
          featurePath: "transactWrite.TransactItems",
          reason: "TransactItems must be a non-empty array.",
        })
      }

      if (transactItems.length > 100) {
        throw this.validationError(
          "Member must have length less than or equal to 100"
        )
      }

      const touched = new Set<string>()
      const journal = new Map<string, { tableName: string; key: { PK: string; SK: string }; before: any | null }>()

      const remember = (tableName: string, key: { PK: string; SK: string }, before: any | undefined) => {
        const marker = `${tableName}::${key.PK}::${key.SK}`
        if (!journal.has(marker)) {
          journal.set(marker, {
            tableName,
            key,
            before: before ? cloneItem(before) : null,
          })
        }
      }

      const ensureNoDuplicate = (tableName: string, key: { PK: string; SK: string }) => {
        const marker = `${tableName}::${key.PK}::${key.SK}`
        if (touched.has(marker)) {
          throw this.validationError(
            "Transaction request cannot include multiple operations on one item"
          )
        }

        touched.add(marker)
      }

      const rollback = () => {
        for (const entry of [...journal.values()].reverse()) {
          const table = this.getTable(entry.tableName)

          if (entry.before === null) {
            table.deleteByKey(entry.key)
          } else {
            table.put(entry.before)
          }
        }
      }

      try {
        for (const transactionEntry of transactItems) {
          if (transactionEntry.Put) {
            const put = transactionEntry.Put
            const tableName = this.getRequiredTableName(put)
            const table = this.getTable(tableName)
            const item = put.Item

            if (!item || typeof item !== "object") {
              throw new NotSupportedError({
                method: "transactWrite",
                featurePath: "transactWrite.TransactItems.Put.Item",
                reason: "Put.Item must be an object.",
              })
            }

            this.assertPrimaryKey(item, "transactWrite")
            const key = { PK: item.PK, SK: item.SK }
            ensureNoDuplicate(tableName, key)

            const existing = table.cloneItemByKey(key)
            this.assertExpressionAttributeInputs(put, [put.ConditionExpression])
            this.assertCondition("transactWrite", put, existing)

            remember(tableName, key, existing)
            table.put(item)

            continue
          }

          if (transactionEntry.Update) {
            const update = transactionEntry.Update
            const tableName = this.getRequiredTableName(update)
            const key = this.getRequiredKey(update, "Key")
            ensureNoDuplicate(tableName, key)

            const table = this.getTable(tableName)
            const existing = table.cloneItemByKey(key)
            this.assertExpressionAttributeInputs(update, [
              update.ConditionExpression,
              update.UpdateExpression,
            ])
            this.assertCondition("transactWrite", update, existing)

            if (typeof update.UpdateExpression !== "string") {
              throw new NotSupportedError({
                method: "transactWrite",
                featurePath: "transactWrite.TransactItems.Update.UpdateExpression",
                reason: "UpdateExpression is required.",
              })
            }

            const base = existing ?? { PK: key.PK, SK: key.SK }
            const parsed = this.parseUpdateExpressionOrValidationError(
              update.UpdateExpression,
              {
                method: "transactWrite",
                expressionAttributeNames: update.ExpressionAttributeNames,
                expressionAttributeValues: update.ExpressionAttributeValues,
                item: base,
              }
            )

            const next = cloneItem(base)
            this.applyParsedUpdateExpression(next, parsed)

            this.assertPrimaryKey(next, "transactWrite")

            remember(tableName, key, existing)
            table.put(next)

            continue
          }

          if (transactionEntry.Delete) {
            const del = transactionEntry.Delete
            const tableName = this.getRequiredTableName(del)
            const key = this.getRequiredKey(del, "Key")
            ensureNoDuplicate(tableName, key)

            const table = this.getTable(tableName)
            const existing = table.cloneItemByKey(key)
            this.assertExpressionAttributeInputs(del, [del.ConditionExpression])
            this.assertCondition("transactWrite", del, existing)
            remember(tableName, key, existing)

            table.deleteByKey(key)
            continue
          }

          if (transactionEntry.ConditionCheck) {
            const check = transactionEntry.ConditionCheck
            const tableName = this.getRequiredTableName(check)
            const key = this.getRequiredKey(check, "Key")
            ensureNoDuplicate(tableName, key)

            const table = this.getTable(tableName)
            const existing = table.cloneItemByKey(key)
            this.assertExpressionAttributeInputs(check, [check.ConditionExpression])
            this.assertCondition("transactWrite", check, existing)
            continue
          }

          throw new NotSupportedError({
            method: "transactWrite",
            featurePath: "transactWrite.TransactItems",
            reason: "Each transaction entry must include Put, Update, Delete, or ConditionCheck.",
          })
        }
      } catch (error: any) {
        rollback()

        if (error instanceof NotSupportedError) throw error

        if (
          error?.code === "ValidationException" &&
          /Cannot update attribute (PK|SK)\. This attribute is part of the key/.test(
            String(error?.message ?? "")
          )
        ) {
          throw this.transactionCanceledError(
            "Transaction cancelled, please refer cancellation reasons for specific reasons [ValidationError]"
          )
        }

        if (error?.code === "ValidationException") {
          throw error
        }

        if (error?.code === "ConditionalCheckFailedException") {
          throw this.transactionCanceledError(
            "Transaction cancelled, please refer cancellation reasons for specific reasons [None, ConditionalCheckFailed]"
          )
        }

        if (error?.code === "TransactionCanceledException") {
          throw error
        }

        throw this.transactionCanceledError(error?.message ?? "Transaction failed.")
      }

      return {}
    })
  }

  __inMemorySnapshot(tableName: string): { [key: string]: any } {
    return this.getTable(tableName).snapshot()
  }

  __inMemoryResetTable(tableName: string): void {
    this.getTable(tableName).clear()
  }

  private request<T>(method: string, params: AnyParams, fn: () => Promise<T>): PromiseRequest<T> {
    return {
      promise: async () => {
        try {
          return await fn()
        } catch (error) {
          throw error
        }
      },
    }
  }

  private getTable(tableName: string): InMemoryTableState {
    const existing = this.tables.get(tableName)
    if (existing) return existing

    const next = new InMemoryTableState()
    this.tables.set(tableName, next)
    return next
  }

  private assertSupportedParams(method: string, params: AnyParams) {
    const spec = IN_MEMORY_SPEC.methods[method]

    if (!spec) {
      throw new NotSupportedError({
        method,
        featurePath: method,
        reason: `Unsupported method: ${method}`,
      })
    }

    for (const [key, value] of Object.entries(params)) {
      if (typeof value === "undefined") continue

      if (spec.unsupportedParams?.includes(key)) {
        throw new NotSupportedError({
          method,
          featurePath: `${method}.${key}`,
          reason: `${key} is not supported in in-memory mode.`,
        })
      }

      if (!spec.supportedParams.includes(key)) {
        throw new NotSupportedError({
          method,
          featurePath: `${method}.${key}`,
          reason: `Unsupported parameter: ${key}`,
        })
      }
    }
  }

  private getRequiredTableName(params: AnyParams): string {
    if (typeof params.TableName !== "string" || !params.TableName) {
      throw new NotSupportedError({
        method: "documentClient",
        featurePath: "TableName",
        reason: "TableName must be a non-empty string.",
      })
    }

    return params.TableName
  }

  private getRequiredKey(
    params: AnyParams,
    featurePath: string
  ): { PK: string; SK: string } {
    const key = params.Key

    if (!key || typeof key !== "object") {
      throw new NotSupportedError({
        method: "documentClient",
        featurePath,
        reason: "Key must be an object.",
      })
    }

    if (typeof key.PK !== "string" || typeof key.SK !== "string") {
      throw this.validationError(
        "One or more parameter values were invalid: Type mismatch for key"
      )
    }

    return { PK: key.PK, SK: key.SK }
  }

  private assertPrimaryKey(item: any, method: string) {
    if (typeof item.PK !== "string" || typeof item.SK !== "string") {
      throw this.validationError(
        "One or more parameter values were invalid: Type mismatch for key"
      )
    }
  }

  private applyParsedUpdateExpression(item: any, parsed: ParsedUpdateExpression) {
    for (const assignment of parsed.set) {
      const keyAttribute = this.getTopLevelKeyAttribute(assignment.path)
      if (
        keyAttribute &&
        (assignment.path.length !== 1 || assignment.value !== item[keyAttribute])
      ) {
        throw this.validationError(
          `One or more parameter values were invalid: Cannot update attribute ${keyAttribute}. This attribute is part of the key`
        )
      }

      this.setValueAtDocumentPath(item, assignment.path, assignment.value)
    }

    for (const removal of parsed.remove) {
      const keyAttribute = this.getTopLevelKeyAttribute(removal.path)
      if (keyAttribute) {
        throw this.validationError(
          `One or more parameter values were invalid: Cannot update attribute ${keyAttribute}. This attribute is part of the key`
        )
      }

      this.removeValueAtDocumentPath(item, removal.path)
    }
  }

  private getTopLevelKeyAttribute(path: DocumentPathPart[]): "PK" | "SK" | null {
    const head = path[0]
    if (!head || head.type !== "attribute") return null
    if (head.value === "PK" || head.value === "SK") return head.value
    return null
  }

  private setValueAtDocumentPath(
    item: any,
    path: DocumentPathPart[],
    value: any
  ) {
    const parent = this.resolveDocumentPathParent(item, path)
    const leaf = path[path.length - 1]

    if (leaf.type === "attribute") {
      if (!parent || typeof parent !== "object" || Array.isArray(parent)) {
        throw this.invalidUpdatePathValidationError()
      }

      parent[leaf.value] = value
      return
    }

    if (!Array.isArray(parent) || leaf.value > parent.length) {
      throw this.invalidUpdatePathValidationError()
    }

    parent[leaf.value] = value
  }

  private removeValueAtDocumentPath(item: any, path: DocumentPathPart[]) {
    const parent = this.tryResolveDocumentPathParent(item, path)
    if (typeof parent === "undefined") return

    const leaf = path[path.length - 1]

    if (leaf.type === "attribute") {
      if (!parent || typeof parent !== "object" || Array.isArray(parent)) return
      delete parent[leaf.value]
      return
    }

    if (!Array.isArray(parent)) return
    if (leaf.value >= parent.length) return
    parent.splice(leaf.value, 1)
  }

  private resolveDocumentPathParent(item: any, path: DocumentPathPart[]): any {
    let current = item

    for (let index = 0; index < path.length - 1; index += 1) {
      const part = path[index]

      if (part.type === "attribute") {
        if (!current || typeof current !== "object" || Array.isArray(current)) {
          throw this.invalidUpdatePathValidationError()
        }

        if (!(part.value in current)) {
          throw this.invalidUpdatePathValidationError()
        }

        current = current[part.value]
        continue
      }

      if (!Array.isArray(current) || part.value >= current.length) {
        throw this.invalidUpdatePathValidationError()
      }

      current = current[part.value]
    }

    return current
  }

  private tryResolveDocumentPathParent(
    item: any,
    path: DocumentPathPart[]
  ): any | undefined {
    let current = item

    for (let index = 0; index < path.length - 1; index += 1) {
      const part = path[index]

      if (part.type === "attribute") {
        if (!current || typeof current !== "object" || Array.isArray(current)) {
          return undefined
        }

        if (!(part.value in current)) {
          return undefined
        }

        current = current[part.value]
        continue
      }

      if (!Array.isArray(current) || part.value >= current.length) {
        return undefined
      }

      current = current[part.value]
    }

    return current
  }

  private invalidUpdatePathValidationError(): Error & { code: string } {
    return this.validationError(
      "The document path provided in the update expression is invalid for update"
    )
  }

  private assertCondition(method: string, params: AnyParams, item?: any) {
    if (!params.ConditionExpression) return

    const ok = this.evaluateConditionOrValidationError(
      params.ConditionExpression,
      item,
      {
        method,
        expressionAttributeNames: params.ExpressionAttributeNames,
        expressionAttributeValues: params.ExpressionAttributeValues,
      },
      "ConditionExpression"
    )

    if (!ok) {
      throw this.awsError(
        "ConditionalCheckFailedException",
        "The conditional request failed."
      )
    }
  }

  private resolveIndexName(indexName?: string): InMemoryIndexName {
    if (!indexName) return PRIMARY_INDEX_NAME
    if (indexName === "GSI1") {
      throw new NotSupportedError({
        method: "query",
        featurePath: "query.IndexName",
        reason: "GSI1 is intentionally excluded from in-memory mode.",
      })
    }

    if (!isSupportedIndexName(indexName)) {
      throw new NotSupportedError({
        method: "query",
        featurePath: "query.IndexName",
        reason: `Unsupported index: ${indexName}`,
      })
    }

    return parseIndexName(indexName)
  }

  private resolveExclusiveStartKey(
    startKey: any,
    indexName: InMemoryIndexName,
    table: InMemoryTableState
  ) {
    const key = this.getRequiredKey({ Key: startKey }, "query.ExclusiveStartKey")

    const descriptor = table.getDescriptor(indexName)
    const rangeKey = startKey?.[descriptor.rangeAttribute]
    const hashKey = startKey?.[descriptor.hashAttribute]

    if (typeof rangeKey !== "string" || typeof hashKey !== "string") {
      throw this.validationError(
        "Exclusive Start Key must have same size as table's key schema"
      )
    }

    return {
      itemKey: encodeItemKey(key.PK, key.SK),
      rangeKey,
    }
  }

  private buildLastEvaluatedKey(indexName: InMemoryIndexName, item: any): any {
    const key: any = {
      PK: item.PK,
      SK: item.SK,
    }

    if (indexName !== PRIMARY_INDEX_NAME) {
      key[`${indexName}PK`] = item[`${indexName}PK`]
      key[`${indexName}SK`] = item[`${indexName}SK`]
    }

    return key
  }

  private normalizeLimit(value: any, method: "query" | "scan"): number | undefined {
    if (typeof value === "undefined") return undefined

    if (
      typeof value !== "number" ||
      !Number.isFinite(value) ||
      value < 1
    ) {
      throw this.validationError(
        "Limit must be greater than or equal to 1"
      )
    }

    return Math.floor(value)
  }

  private awsError(code: string, message: string): Error & { code: string } {
    const error = new Error(message) as Error & { code: string }
    error.code = code
    error.name = code
    return error
  }

  private transactionCanceledError(message: string): Error & { code: string } {
    return this.awsError("TransactionCanceledException", message)
  }

  private validationError(message: string): Error & { code: string } {
    return this.awsError("ValidationException", message)
  }

  private parseKeyConditionOrValidationError(
    expression: string,
    context: {
      method: string
      expressionAttributeNames?: { [key: string]: string }
      expressionAttributeValues?: { [key: string]: any }
    }
  ) {
    try {
      return parseKeyConditionExpression(expression, context)
    } catch (error) {
      if (error instanceof NotSupportedError) {
        throw this.validationError(
          this.toDynamoExpressionValidationMessage(
            "KeyConditionExpression",
            error
          )
        )
      }

      throw error
    }
  }

  private parseUpdateExpressionOrValidationError(
    expression: string,
    context: {
      method: string
      expressionAttributeNames?: { [key: string]: string }
      expressionAttributeValues?: { [key: string]: any }
      item?: any
    }
  ) {
    try {
      return parseUpdateExpression(expression, context)
    } catch (error) {
      if (error instanceof NotSupportedError) {
        throw this.validationError(
          this.toDynamoExpressionValidationMessage("UpdateExpression", error)
        )
      }

      throw error
    }
  }

  private evaluateConditionOrValidationError(
    expression: string,
    item: any,
    context: {
      method: string
      expressionAttributeNames?: { [key: string]: string }
      expressionAttributeValues?: { [key: string]: any }
    },
    expressionType: "ConditionExpression" | "FilterExpression"
  ): boolean {
    try {
      return evaluateConditionExpression(expression, item, context)
    } catch (error) {
      if (error instanceof NotSupportedError) {
        throw this.validationError(
          this.toDynamoExpressionValidationMessage(expressionType, error)
        )
      }

      throw error
    }
  }

  private toDynamoExpressionValidationMessage(
    expressionType:
      | "ConditionExpression"
      | "FilterExpression"
      | "KeyConditionExpression"
      | "UpdateExpression",
    error: NotSupportedError
  ): string {
    const reason = String(error.reason ?? "").replace(/\.$/, "")

    const namePlaceholder = error.featurePath.match(
      /ExpressionAttributeNames\.(#[A-Za-z_][A-Za-z0-9_]*)$/
    )?.[1]
    if (namePlaceholder) {
      return `Invalid ${expressionType}: An expression attribute name used in the document path is not defined; attribute name: ${namePlaceholder}`
    }

    const valuePlaceholder = error.featurePath.match(
      /ExpressionAttributeValues\.(:[A-Za-z_][A-Za-z0-9_]*)$/
    )?.[1]
    if (
      reason === "ExpressionAttributeValues are required for value placeholders" ||
      reason === "Missing expression attribute value placeholder"
    ) {
      const token = valuePlaceholder ?? ":missing"
      return `Invalid ${expressionType}: An expression attribute value used in expression is not defined; attribute value: ${token}`
    }

    if (
      reason === "Malformed SET assignment" ||
      reason === "Malformed REMOVE assignment"
    ) {
      const token = reason.includes("REMOVE") ? "REMOVE" : "SET"
      return `Invalid UpdateExpression: Syntax error; token: "<EOF>", near: "${token}"`
    }

    const invalidFunctionFromClause = reason.match(
      /^Unsupported clause:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(/
    )?.[1]
    if (invalidFunctionFromClause) {
      return `Invalid ${expressionType}: Invalid function name; function: ${invalidFunctionFromClause}`
    }

    const unsupportedValue = reason.match(/^Unsupported value token:\s*(.+)$/)?.[1]
    if (unsupportedValue) {
      const fn =
        unsupportedValue.match(/\band\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(/i)?.[1] ??
        unsupportedValue.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*\(/)?.[1]
      if (fn) {
        return `Invalid ${expressionType}: Invalid function name; function: ${fn}`
      }
    }

    return reason
  }

  private assertExpressionAttributeInputs(
    params: AnyParams,
    expressions: Array<string | undefined>
  ) {
    const activeExpressions = expressions.filter(
      (value): value is string => typeof value === "string" && value.trim().length > 0
    )
    if (activeExpressions.length === 0) return

    if (typeof params.ExpressionAttributeValues !== "undefined") {
      const values = params.ExpressionAttributeValues
      if (!values || typeof values !== "object" || Array.isArray(values)) {
        throw this.validationError("ExpressionAttributeValues must not be empty")
      }

      const valueKeys = Object.keys(values)
      if (valueKeys.length === 0) {
        throw this.validationError("ExpressionAttributeValues must not be empty")
      }

      const usedValueTokens = new Set<string>()
      for (const expression of activeExpressions) {
        for (const token of expression.match(/:[A-Za-z_][A-Za-z0-9_]*/g) ?? []) {
          usedValueTokens.add(token)
        }
      }

      const unusedValues = valueKeys
        .filter((token) => !usedValueTokens.has(token))
        .sort()
      if (unusedValues.length > 0) {
        throw this.validationError(
          `Value provided in ExpressionAttributeValues unused in expressions: keys: {${unusedValues.join(", ")}}`
        )
      }
    }

    if (typeof params.ExpressionAttributeNames !== "undefined") {
      const names = params.ExpressionAttributeNames
      if (!names || typeof names !== "object" || Array.isArray(names)) {
        throw this.validationError("ExpressionAttributeNames must not be empty")
      }

      const nameKeys = Object.keys(names)
      if (nameKeys.length === 0) {
        throw this.validationError("ExpressionAttributeNames must not be empty")
      }

      const usedNameTokens = new Set<string>()
      for (const expression of activeExpressions) {
        for (const token of expression.match(/#[A-Za-z_][A-Za-z0-9_]*/g) ?? []) {
          usedNameTokens.add(token)
        }
      }

      const unusedNames = nameKeys
        .filter((token) => !usedNameTokens.has(token))
        .sort()
      if (unusedNames.length > 0) {
        throw this.validationError(
          `Value provided in ExpressionAttributeNames unused in expressions: keys: {${unusedNames.join(", ")}}`
        )
      }
    }
  }
}

const createUnsupportedRequest = (
  method: string
): ((params: AnyParams) => PromiseRequest<never>) => {
  return (_params: AnyParams) => ({
    promise: async () => {
      throw new NotSupportedError({
        method,
        featurePath: method,
        reason: `${method} is not supported in in-memory mode.`,
      })
    },
  })
}

export const createInMemoryDocumentClient = (): InMemoryDocumentClient => {
  const instance = new InMemoryDocumentClientImpl()

  const proxied = new Proxy(instance as any, {
    get(target, prop, receiver) {
      if (typeof prop === "string") {
        if (prop in target) {
          const value = Reflect.get(target, prop, receiver)
          return typeof value === "function" ? value.bind(target) : value
        }

        if (IN_MEMORY_SPEC.unsupportedMethods.includes(prop)) {
          return createUnsupportedRequest(prop)
        }

        return createUnsupportedRequest(prop)
      }

      return Reflect.get(target, prop, receiver)
    },
  })

  return proxied as InMemoryDocumentClient
}
