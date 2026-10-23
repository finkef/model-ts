import { NotSupportedError } from "./errors.ts"
import {
  type DocumentPathPart,
  evaluateConditionExpression,
  matchesRangeCondition,
  parseKeyConditionExpression,
  type ParsedUpdateExpression,
  parseUpdateExpression,
  type RangeCondition,
} from "./expression.ts"
import {
  InMemoryTableState,
  PRIMARY_INDEX_NAME,
  isSupportedIndexName,
  matchesKeyConditionDescriptor,
  isGSI,
} from "./store.ts"
import { IN_MEMORY_SPEC, type InMemoryIndexName } from "./spec.ts"
import {
  cloneItem,
  compareStrings,
  encodeCompositeKey,
  encodeItemKey,
} from "./utils.ts"

type AnyParams = { [key: string]: any }

export interface MemoryEngine {
  get(params: AnyParams): Promise<{ Item?: any }>
  put(params: AnyParams): Promise<{}>
  update(params: AnyParams): Promise<{ Attributes?: any }>
  delete(params: AnyParams): Promise<{}>
  query(params: AnyParams): Promise<{
    Items?: any[]
    Count: number
    ScannedCount: number
    LastEvaluatedKey?: any
  }>
  scan(params: AnyParams): Promise<{
    Items?: any[]
    Count: number
    ScannedCount: number
    LastEvaluatedKey?: any
  }>
  batchGet(
    params: AnyParams,
  ): Promise<{ Responses?: { [tableName: string]: any[] } }>
  batchWrite(
    params: AnyParams,
  ): Promise<{ UnprocessedItems: { [tableName: string]: any[] } }>
  transactWrite(params: AnyParams): Promise<{}>
  snapshot(tableName: string): { [key: string]: any }
  resetTable(tableName: string): void
}

class MemoryEngineImpl implements MemoryEngine {
  private readonly tables = new Map<string, InMemoryTableState>()

  async get(params: AnyParams): Promise<{ Item?: any }> {
    this.assertSupportedParams("get", params)
    const tableName = this.getRequiredTableName(params)
    const key = this.getRequiredKey(params, "Key")

    const table = this.getTable(tableName)
    const item = table.cloneItemByKey(key)

    return item ? { Item: item } : {}
  }

  async put(params: AnyParams): Promise<{}> {
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

    this.assertPrimaryKey(item)
    this.assertExpressionAttributeInputs(params, [params.ConditionExpression])

    const existing = table.cloneItemByKey({ PK: item.PK, SK: item.SK })
    this.assertCondition("put", params, existing)

    table.put(item)

    return {}
  }

  async update(params: AnyParams): Promise<{ Attributes?: any }> {
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
        `Unsupported ReturnValues value: ${returnValues}`,
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
      },
    )

    const next = cloneItem(base)
    this.applyParsedUpdateExpression(next, parsed)

    this.assertPrimaryKey(next)
    table.put(next)

    if (returnValues === "ALL_NEW") {
      return { Attributes: cloneItem(next) }
    }

    return {}
  }

  async delete(params: AnyParams): Promise<{}> {
    this.assertSupportedParams("delete", params)
    const tableName = this.getRequiredTableName(params)
    const key = this.getRequiredKey(params, "Key")

    const table = this.getTable(tableName)
    const existing = table.cloneItemByKey(key)
    this.assertExpressionAttributeInputs(params, [params.ConditionExpression])

    this.assertCondition("delete", params, existing)

    table.deleteByKey(key)
    return {}
  }

  async query(params: AnyParams): Promise<{
    Items?: any[]
    Count: number
    ScannedCount: number
    LastEvaluatedKey?: any
  }> {
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
        "Consistent read cannot be true when querying a GSI",
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
      },
    )

    if (!matchesKeyConditionDescriptor(indexName, condition)) {
      throw new NotSupportedError({
        method: "query",
        featurePath: "query.KeyConditionExpression",
        reason:
          "KeyConditionExpression attributes do not match the selected index.",
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
    const limit = this.normalizeLimit(params.Limit)
    const exclusiveStartKey = params.ExclusiveStartKey
      ? this.resolveExclusiveStartKey(
          params.ExclusiveStartKey,
          indexName,
          table,
          condition.hashValue,
          condition.range?.condition,
        )
      : undefined

    const iterator = table
      .iterateQueryCandidates({
        indexName,
        hashKey: condition.hashValue,
        rangeCondition: condition.range?.condition,
        scanIndexForward,
        exclusiveStartKey,
      })
      [Symbol.iterator]()

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
            "FilterExpression",
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
  }

  async scan(params: AnyParams): Promise<{
    Items?: any[]
    Count: number
    ScannedCount: number
    LastEvaluatedKey?: any
  }> {
    this.assertSupportedParams("scan", params)

    const tableName = this.getRequiredTableName(params)
    const table = this.getTable(tableName)
    const limit = this.normalizeLimit(params.Limit)
    this.assertExpressionAttributeInputs(params, [params.FilterExpression])

    const exclusiveStartKey = params.ExclusiveStartKey
      ? this.getRequiredKey(
          { Key: params.ExclusiveStartKey },
          "ExclusiveStartKey",
        )
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
            "FilterExpression",
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
  }

  async batchGet(
    params: AnyParams,
  ): Promise<{ Responses?: { [tableName: string]: any[] } }> {
    this.assertSupportedParams("batchGet", params)

    if (!params.RequestItems || typeof params.RequestItems !== "object") {
      throw new NotSupportedError({
        method: "batchGet",
        featurePath: "batchGet.RequestItems",
        reason: "RequestItems is required.",
      })
    }

    const responses: { [tableName: string]: any[] } = {}

    for (const [tableName, request] of Object.entries<any>(
      params.RequestItems,
    )) {
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
          "Too many items requested for the BatchGetItem call",
        )
      }

      const table = this.getTable(tableName)
      const tableResponses: any[] = []
      const unique = new Set<string>()

      for (const key of keys) {
        const parsedKey = this.getRequiredKey({ Key: key }, "Key")
        const dedupKey = encodeItemKey(parsedKey.PK, parsedKey.SK)
        if (unique.has(dedupKey)) {
          throw this.validationError(
            "Provided list of item keys contains duplicates",
          )
        }
        unique.add(dedupKey)

        const item = table.cloneItemByKey(parsedKey)
        if (item) tableResponses.push(item)
      }

      responses[tableName] = tableResponses
    }

    return { Responses: responses }
  }

  async batchWrite(
    params: AnyParams,
  ): Promise<{ UnprocessedItems: { [tableName: string]: any[] } }> {
    this.assertSupportedParams("batchWrite", params)

    if (!params.RequestItems || typeof params.RequestItems !== "object") {
      throw new NotSupportedError({
        method: "batchWrite",
        featurePath: "batchWrite.RequestItems",
        reason: "RequestItems is required.",
      })
    }

    for (const [tableName, requests] of Object.entries<any[]>(
      params.RequestItems,
    )) {
      if (!Array.isArray(requests)) {
        throw new NotSupportedError({
          method: "batchWrite",
          featurePath: "batchWrite.RequestItems",
          reason: "Each RequestItems entry must be an array.",
        })
      }

      if (requests.length > 25) {
        throw this.validationError(
          "Too many items requested for the BatchWriteItem call",
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

          this.assertPrimaryKey(item)
          table.put(item)
          continue
        }

        if (entry.DeleteRequest) {
          const key = this.getRequiredKey(
            { Key: entry.DeleteRequest.Key },
            "DeleteRequest.Key",
          )
          table.deleteByKey(key)
          continue
        }

        throw this.validationError(
          "Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes",
        )
      }
    }

    return { UnprocessedItems: {} }
  }

  async transactWrite(params: AnyParams): Promise<{}> {
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
        "Member must have length less than or equal to 100",
      )
    }

    const touched = new Set<string>()
    const journal = new Map<
      string,
      { tableName: string; key: { PK: string; SK: string }; before: any | null }
    >()

    const remember = (
      tableName: string,
      key: { PK: string; SK: string },
      before: any | undefined,
    ) => {
      const marker = encodeCompositeKey(tableName, key.PK, key.SK)
      if (!journal.has(marker)) {
        journal.set(marker, {
          tableName,
          key,
          before: before ? cloneItem(before) : null,
        })
      }
    }

    const ensureNoDuplicate = (
      tableName: string,
      key: { PK: string; SK: string },
    ) => {
      const marker = encodeCompositeKey(tableName, key.PK, key.SK)
      if (touched.has(marker)) {
        throw this.validationError(
          "Transaction request cannot include multiple operations on one item",
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

    const reasons: { Code: string }[] = transactItems.map(() => ({
      Code: "None",
    }))
    const conditionPasses = (
      params: AnyParams,
      existing: AnyParams | undefined,
      index: number,
    ) => {
      try {
        this.assertCondition("transactWrite", params, existing)
        return true
      } catch (error: any) {
        if (error?.code !== "ConditionalCheckFailedException") throw error
        reasons[index] = { Code: "ConditionalCheckFailed" }
        return false
      }
    }
    try {
      for (const [index, transactionEntry] of transactItems.entries()) {
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

          this.assertPrimaryKey(item)
          const key = { PK: item.PK, SK: item.SK }
          ensureNoDuplicate(tableName, key)

          const existing = table.cloneItemByKey(key)
          this.assertExpressionAttributeInputs(put, [put.ConditionExpression])
          if (!conditionPasses(put, existing, index)) continue

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
          if (!conditionPasses(update, existing, index)) continue

          if (typeof update.UpdateExpression !== "string") {
            throw new NotSupportedError({
              method: "transactWrite",
              featurePath:
                "transactWrite.TransactItems.Update.UpdateExpression",
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
            },
          )

          const next = cloneItem(base)
          this.applyParsedUpdateExpression(next, parsed)

          this.assertPrimaryKey(next)

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
          if (!conditionPasses(del, existing, index)) continue
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
          this.assertExpressionAttributeInputs(check, [
            check.ConditionExpression,
          ])
          if (!conditionPasses(check, existing, index)) continue
          continue
        }

        throw new NotSupportedError({
          method: "transactWrite",
          featurePath: "transactWrite.TransactItems",
          reason:
            "Each transaction entry must include Put, Update, Delete, or ConditionCheck.",
        })
      }
      if (reasons.some(({ Code }) => Code !== "None")) {
        throw Object.assign(
          this.transactionCanceledError(
            `Transaction cancelled, please refer cancellation reasons for specific reasons [${reasons.map(({ Code }) => Code).join(", ")}]`,
          ),
          { CancellationReasons: reasons },
        )
      }
    } catch (error: any) {
      rollback()

      if (error instanceof NotSupportedError) throw error

      if (
        error?.code === "ValidationException" &&
        /Cannot update attribute (PK|SK)\. This attribute is part of the key/.test(
          String(error?.message ?? ""),
        )
      ) {
        throw this.transactionCanceledError(
          "Transaction cancelled, please refer cancellation reasons for specific reasons [ValidationError]",
        )
      }

      if (error?.code === "ValidationException") {
        throw error
      }

      if (error?.code === "TransactionCanceledException") {
        throw error
      }

      throw this.transactionCanceledError(
        error?.message ?? "Transaction failed.",
      )
    }

    return {}
  }

  snapshot(tableName: string): { [key: string]: any } {
    return this.getTable(tableName).snapshot()
  }

  resetTable(tableName: string): void {
    this.getTable(tableName).clear()
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
    featurePath: string,
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
        "One or more parameter values were invalid: Type mismatch for key",
      )
    }

    return { PK: key.PK, SK: key.SK }
  }

  private assertPrimaryKey(item: any) {
    if (typeof item.PK !== "string" || typeof item.SK !== "string") {
      throw this.validationError(
        "One or more parameter values were invalid: Type mismatch for key",
      )
    }
  }

  private applyParsedUpdateExpression(
    item: any,
    parsed: ParsedUpdateExpression,
  ) {
    for (const assignment of parsed.set) {
      // DynamoDB refuses any SET of a key attribute, even to its own value.
      const keyAttribute = this.getTopLevelKeyAttribute(assignment.path)
      if (keyAttribute) {
        throw this.validationError(
          `One or more parameter values were invalid: Cannot update attribute ${keyAttribute}. This attribute is part of the key`,
        )
      }

      this.setValueAtDocumentPath(item, assignment.path, assignment.value)
    }

    // Every path names an element of the item as it was before the update,
    // so list elements are removed from the highest index down: removing
    // a[0] first would move the element a[1] names.
    const removals = [...parsed.remove].sort((left, right) => {
      const parents = compareStrings(
        parentPathKey(left.path),
        parentPathKey(right.path),
      )
      return parents !== 0
        ? parents
        : leafIndex(right.path) - leafIndex(left.path)
    })
    for (const removal of removals) {
      const keyAttribute = this.getTopLevelKeyAttribute(removal.path)
      if (keyAttribute) {
        throw this.validationError(
          `One or more parameter values were invalid: Cannot update attribute ${keyAttribute}. This attribute is part of the key`,
        )
      }

      this.removeValueAtDocumentPath(item, removal.path)
    }
  }

  private getTopLevelKeyAttribute(
    path: DocumentPathPart[],
  ): "PK" | "SK" | null {
    const head = path[0]
    if (!head || head.type !== "attribute") return null
    if (head.value === "PK" || head.value === "SK") return head.value
    return null
  }

  private setValueAtDocumentPath(
    item: any,
    path: DocumentPathPart[],
    value: any,
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

    if (!Array.isArray(parent)) {
      throw this.invalidUpdatePathValidationError()
    }

    // An index past the end of the list appends, as in DynamoDB.
    if (leaf.value >= parent.length) parent.push(value)
    else parent[leaf.value] = value
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
    path: DocumentPathPart[],
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
      "The document path provided in the update expression is invalid for update",
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
      "ConditionExpression",
    )

    if (!ok) {
      throw this.awsError(
        "ConditionalCheckFailedException",
        "The conditional request failed.",
      )
    }
  }

  private resolveIndexName(indexName?: string): InMemoryIndexName {
    if (!indexName) return PRIMARY_INDEX_NAME
    if (!isSupportedIndexName(indexName)) {
      throw new NotSupportedError({
        method: "query",
        featurePath: "query.IndexName",
        reason: `Unsupported index: ${indexName}`,
      })
    }

    return indexName
  }

  private resolveExclusiveStartKey(
    startKey: any,
    indexName: InMemoryIndexName,
    table: InMemoryTableState,
    hashValue: string,
    rangeCondition: RangeCondition | undefined,
  ) {
    const key = this.getRequiredKey(
      { Key: startKey },
      "query.ExclusiveStartKey",
    )

    const descriptor = table.getDescriptor(indexName)
    const rangeKey = startKey?.[descriptor.rangeAttribute]
    const hashKey = startKey?.[descriptor.hashAttribute]

    if (typeof rangeKey !== "string" || typeof hashKey !== "string") {
      throw this.validationError(
        "Exclusive Start Key must have same size as table's key schema",
      )
    }

    // DynamoDB only resumes a query from a key the query itself could return.
    if (hashKey !== hashValue) {
      throw this.validationError(
        "The provided starting key is outside query boundaries based on provided conditions",
      )
    }
    if (
      rangeCondition !== undefined &&
      !matchesRangeCondition(rangeKey, rangeCondition)
    ) {
      throw this.validationError(
        "The provided starting key does not match the range key predicate",
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

  private normalizeLimit(value: any): number | undefined {
    if (typeof value === "undefined") return undefined

    if (typeof value !== "number" || !Number.isFinite(value) || value < 1) {
      throw this.validationError("Limit must be greater than or equal to 1")
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
    },
  ) {
    try {
      return parseKeyConditionExpression(expression, context)
    } catch (error: any) {
      if (error instanceof NotSupportedError) {
        throw this.validationError(
          this.toDynamoExpressionValidationMessage(
            "KeyConditionExpression",
            error,
          ),
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
    },
  ) {
    try {
      return parseUpdateExpression(expression, context)
    } catch (error: any) {
      if (error instanceof NotSupportedError) {
        throw this.validationError(
          this.toDynamoExpressionValidationMessage("UpdateExpression", error),
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
    expressionType: "ConditionExpression" | "FilterExpression",
  ): boolean {
    try {
      return evaluateConditionExpression(expression, item, context)
    } catch (error: any) {
      if (error instanceof NotSupportedError) {
        throw this.validationError(
          this.toDynamoExpressionValidationMessage(expressionType, error),
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
    error: NotSupportedError,
  ): string {
    const reason = String(error.reason ?? "").replace(/\.$/, "")

    const namePlaceholder = error.featurePath.match(
      /ExpressionAttributeNames\.(#[A-Za-z_][A-Za-z0-9_]*)$/,
    )?.[1]
    if (namePlaceholder) {
      return `Invalid ${expressionType}: An expression attribute name used in the document path is not defined; attribute name: ${namePlaceholder}`
    }

    const valuePlaceholder = error.featurePath.match(
      /ExpressionAttributeValues\.(:[A-Za-z_][A-Za-z0-9_]*)$/,
    )?.[1]
    if (
      reason ===
        "ExpressionAttributeValues are required for value placeholders" ||
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
      /^Unsupported clause:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(/,
    )?.[1]
    if (invalidFunctionFromClause) {
      return `Invalid ${expressionType}: Invalid function name; function: ${invalidFunctionFromClause}`
    }

    const unsupportedValue = reason.match(
      /^Unsupported value token:\s*(.+)$/,
    )?.[1]
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
    expressions: Array<string | undefined>,
  ) {
    const activeExpressions = expressions.filter(
      (value): value is string =>
        typeof value === "string" && value.trim().length > 0,
    )
    if (activeExpressions.length === 0) return

    if (typeof params.ExpressionAttributeValues !== "undefined") {
      const values = params.ExpressionAttributeValues
      if (!values || typeof values !== "object" || Array.isArray(values)) {
        throw this.validationError(
          "ExpressionAttributeValues must not be empty",
        )
      }

      const valueKeys = Object.keys(values)
      if (valueKeys.length === 0) {
        throw this.validationError(
          "ExpressionAttributeValues must not be empty",
        )
      }

      const usedValueTokens = new Set<string>()
      for (const expression of activeExpressions) {
        for (const token of expression.match(/:[A-Za-z_][A-Za-z0-9_]*/g) ??
          []) {
          usedValueTokens.add(token)
        }
      }

      const unusedValues = valueKeys
        .filter((token) => !usedValueTokens.has(token))
        .sort()
      if (unusedValues.length > 0) {
        throw this.validationError(
          `Value provided in ExpressionAttributeValues unused in expressions: keys: {${unusedValues.join(", ")}}`,
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
        for (const token of expression.match(/#[A-Za-z_][A-Za-z0-9_]*/g) ??
          []) {
          usedNameTokens.add(token)
        }
      }

      const unusedNames = nameKeys
        .filter((token) => !usedNameTokens.has(token))
        .sort()
      if (unusedNames.length > 0) {
        throw this.validationError(
          `Value provided in ExpressionAttributeNames unused in expressions: keys: {${unusedNames.join(", ")}}`,
        )
      }
    }
  }
}

/** An in-process DynamoDB table engine, used by the sandbox's request handler. */
export const makeMemoryEngine = (): MemoryEngine => new MemoryEngineImpl()

/** The path of the list or map that holds the last element of `path`. */
const parentPathKey = (path: ReadonlyArray<DocumentPathPart>) =>
  JSON.stringify(path.slice(0, -1))

/** The list index `path` ends with, or -1 when it ends with an attribute. */
const leafIndex = (path: ReadonlyArray<DocumentPathPart>) => {
  const leaf = path[path.length - 1]
  return leaf?.type === "index" ? leaf.value : -1
}
