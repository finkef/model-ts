import { NotSupportedError } from "../errors"
import { InMemoryItem } from "./utils"

const MISSING = Symbol("missing")

type Missing = typeof MISSING

type ResolvedValue = Missing | any

export interface ExpressionContext {
  method: string
  expressionAttributeNames?: { [key: string]: string }
  expressionAttributeValues?: { [key: string]: any }
  item?: InMemoryItem
}

export type RangeCondition =
  | { type: "begins_with"; value: string }
  | { type: "=" | "<" | "<=" | ">" | ">="; value: any }
  | { type: "between"; lower: any; upper: any }

export interface ParsedKeyCondition {
  hashAttribute: string
  hashValue: any
  range?: {
    attribute: string
    condition: RangeCondition
  }
}

export interface ParsedUpdateExpression {
  set: Array<{ attribute: string; value: any }>
  remove: string[]
}

const KEY_COND_BEGINS_WITH = /^(.+?)\s*=\s*(.+?)\s+and\s+begins_with\((.+?),\s*(.+?)\)$/i
const KEY_COND_BETWEEN = /^(.+?)\s*=\s*(.+?)\s+and\s+(.+?)\s+between\s+(.+?)\s+and\s+(.+)$/i
const KEY_COND_COMPARE = /^(.+?)\s*=\s*(.+?)\s+and\s+(.+?)\s*(<=|<|>=|>|=)\s*(.+)$/i
const KEY_COND_HASH_ONLY = /^(.+?)\s*=\s*(.+)$/i

export const parseKeyConditionExpression = (
  expression: string,
  context: ExpressionContext
): ParsedKeyCondition => {
  const source = expression.trim()

  const beginsMatch = source.match(KEY_COND_BEGINS_WITH)
  if (beginsMatch) {
    const [, hashAttrToken, hashValueToken, rangeAttrToken, rangeValueToken] =
      beginsMatch

    const hashAttribute = resolveAttributeToken(hashAttrToken, context)
    const rangeAttribute = resolveAttributeToken(rangeAttrToken, context)
    const rangeValue = resolveValueToken(rangeValueToken, undefined, context)

    if (typeof rangeValue !== "string") {
      throw new NotSupportedError({
        method: context.method,
        featurePath: "KeyConditionExpression.begins_with.value",
        reason: "begins_with currently supports string operands only.",
      })
    }

    return {
      hashAttribute,
      hashValue: resolveValueToken(hashValueToken, undefined, context),
      range: {
        attribute: rangeAttribute,
        condition: { type: "begins_with", value: rangeValue },
      },
    }
  }

  const betweenMatch = source.match(KEY_COND_BETWEEN)
  if (betweenMatch) {
    const [, hashAttrToken, hashValueToken, rangeAttrToken, lowerToken, upperToken] =
      betweenMatch

    return {
      hashAttribute: resolveAttributeToken(hashAttrToken, context),
      hashValue: resolveValueToken(hashValueToken, undefined, context),
      range: {
        attribute: resolveAttributeToken(rangeAttrToken, context),
        condition: {
          type: "between",
          lower: resolveValueToken(lowerToken, undefined, context),
          upper: resolveValueToken(upperToken, undefined, context),
        },
      },
    }
  }

  const compareMatch = source.match(KEY_COND_COMPARE)
  if (compareMatch) {
    const [, hashAttrToken, hashValueToken, rangeAttrToken, operator, rangeValueToken] =
      compareMatch

    return {
      hashAttribute: resolveAttributeToken(hashAttrToken, context),
      hashValue: resolveValueToken(hashValueToken, undefined, context),
      range: {
        attribute: resolveAttributeToken(rangeAttrToken, context),
        condition: {
          type: operator as "=" | "<" | "<=" | ">" | ">=",
          value: resolveValueToken(rangeValueToken, undefined, context),
        },
      },
    }
  }

  const hashOnlyMatch = source.match(KEY_COND_HASH_ONLY)
  if (hashOnlyMatch) {
    const [, hashAttrToken, hashValueToken] = hashOnlyMatch

    return {
      hashAttribute: resolveAttributeToken(hashAttrToken, context),
      hashValue: resolveValueToken(hashValueToken, undefined, context),
    }
  }

  throw new NotSupportedError({
    method: context.method,
    featurePath: "KeyConditionExpression",
    reason: "Unsupported key condition expression grammar.",
  })
}

export const evaluateConditionExpression = (
  expression: string,
  item: InMemoryItem | undefined,
  context: ExpressionContext
): boolean => {
  const orGroups = splitTopLevelByKeyword(expression, "or")

  if (orGroups.length === 0) {
    throw new NotSupportedError({
      method: context.method,
      featurePath: "ConditionExpression",
      reason: "Empty condition expressions are not supported.",
    })
  }

  return orGroups.some((group) => {
    const andClauses = splitTopLevelByKeyword(group, "and")
    return andClauses.every((clause) =>
      evaluateSingleClause(clause, item, context)
    )
  })
}

export const parseUpdateExpression = (
  expression: string,
  context: ExpressionContext
): ParsedUpdateExpression => {
  const normalized = expression.trim()
  if (!normalized) {
    throw new NotSupportedError({
      method: context.method,
      featurePath: "UpdateExpression",
      reason: "UpdateExpression must not be empty.",
    })
  }

  const setMatch = normalized.match(/\bSET\b/i)
  const removeMatch = normalized.match(/\bREMOVE\b/i)

  if (!setMatch && !removeMatch) {
    throw new NotSupportedError({
      method: context.method,
      featurePath: "UpdateExpression",
      reason: "Only SET and REMOVE update operators are supported.",
    })
  }

  const setStart = setMatch?.index ?? -1
  const removeStart = removeMatch?.index ?? -1

  let setClause = ""
  let removeClause = ""

  if (setStart >= 0) {
    const setBodyStart = setStart + setMatch![0].length
    const setBodyEnd = removeStart >= 0 ? removeStart : normalized.length
    setClause = normalized.slice(setBodyStart, setBodyEnd).trim()
    if (!setClause) {
      throw new NotSupportedError({
        method: context.method,
        featurePath: "UpdateExpression.SET",
        reason: "Malformed SET assignment.",
      })
    }
  }

  if (removeStart >= 0) {
    const removeBodyStart = removeStart + removeMatch![0].length
    removeClause = normalized.slice(removeBodyStart).trim()
    if (!removeClause) {
      throw new NotSupportedError({
        method: context.method,
        featurePath: "UpdateExpression.REMOVE",
        reason: "Malformed REMOVE assignment.",
      })
    }
  }

  const set = setClause
    ? splitTopLevelByDelimiter(setClause, ",")
        .map((segment) => segment.trim())
        .filter(Boolean)
        .map((assignment) => {
          const split = splitTopLevelAssignment(assignment)
          if (!split) {
            throw new NotSupportedError({
              method: context.method,
              featurePath: "UpdateExpression.SET",
              reason: "Malformed SET assignment.",
            })
          }

          const attribute = resolveAttributeToken(split.left, context)
          const value = resolveUpdateSetValueToken(
            split.right,
            context.item,
            context
          )

          return { attribute, value }
        })
    : []

  const remove = removeClause
    ? splitTopLevelByDelimiter(removeClause, ",")
        .map((segment) => segment.trim())
        .filter(Boolean)
        .map((token) => resolveAttributeToken(token, context))
    : []

  return { set, remove }
}

export const matchesRangeCondition = (
  value: any,
  condition: RangeCondition
): boolean => {
  if (value === undefined || value === null) return false

  switch (condition.type) {
    case "begins_with":
      return String(value).startsWith(condition.value)
    case "between":
      return compareValues(value, condition.lower) >= 0 && compareValues(value, condition.upper) <= 0
    case "=":
      return compareValues(value, condition.value) === 0
    case "<":
      return compareValues(value, condition.value) < 0
    case "<=":
      return compareValues(value, condition.value) <= 0
    case ">":
      return compareValues(value, condition.value) > 0
    case ">=":
      return compareValues(value, condition.value) >= 0
  }
}

export const compareValues = (left: any, right: any): number => {
  if (typeof left === "number" && typeof right === "number") {
    return left - right
  }

  const leftString = String(left)
  const rightString = String(right)

  if (leftString < rightString) return -1
  if (leftString > rightString) return 1
  return 0
}

function evaluateSingleClause(
  clause: string,
  item: InMemoryItem | undefined,
  context: ExpressionContext
): boolean {
  const source = clause.trim()

  const existsMatch = source.match(/^attribute_exists\((.+)\)$/i)
  if (existsMatch) {
    const value = resolveAttributeValue(existsMatch[1].trim(), item, context)
    return value !== MISSING
  }

  const notExistsMatch = source.match(/^attribute_not_exists\((.+)\)$/i)
  if (notExistsMatch) {
    const value = resolveAttributeValue(notExistsMatch[1].trim(), item, context)
    return value === MISSING
  }

  const beginsWithMatch = source.match(/^begins_with\((.+?),\s*(.+)\)$/i)
  if (beginsWithMatch) {
    const [, attrToken, valueToken] = beginsWithMatch
    const current = resolveAttributeValue(attrToken.trim(), item, context)
    const expected = resolveValueToken(valueToken.trim(), item, context)

    if (current === MISSING || expected === MISSING) return false
    return String(current).startsWith(String(expected))
  }

  const containsMatch = source.match(/^contains\((.+?),\s*(.+)\)$/i)
  if (containsMatch) {
    const [, attrToken, valueToken] = containsMatch
    const current = resolveAttributeValue(attrToken.trim(), item, context)
    const expected = resolveValueToken(valueToken.trim(), item, context)

    if (current === MISSING || expected === MISSING) return false
    return containsValue(current, expected)
  }

  const attributeTypeMatch = source.match(/^attribute_type\((.+?),\s*(.+)\)$/i)
  if (attributeTypeMatch) {
    const [, attrToken, typeToken] = attributeTypeMatch
    const current = resolveAttributeValue(attrToken.trim(), item, context)
    const expectedType = resolveValueToken(typeToken.trim(), item, context)

    if (current === MISSING) return false
    if (typeof expectedType !== "string") {
      throw new NotSupportedError({
        method: context.method,
        featurePath: "ConditionExpression.attribute_type",
        reason: "attribute_type expects a DynamoDB type string.",
      })
    }

    return attributeMatchesType(current, expectedType)
  }

  const betweenMatch = source.match(/^(.+?)\s+between\s+(.+?)\s+and\s+(.+)$/i)
  if (betweenMatch) {
    const [, attrToken, lowerToken, upperToken] = betweenMatch
    const current = resolveAttributeValue(attrToken.trim(), item, context)
    const lower = resolveValueToken(lowerToken.trim(), item, context)
    const upper = resolveValueToken(upperToken.trim(), item, context)

    if (current === MISSING || lower === MISSING || upper === MISSING) return false

    return compareValues(current, lower) >= 0 && compareValues(current, upper) <= 0
  }

  const compareMatch = source.match(/^(.+?)\s*(=|<>|<=|<|>=|>)\s*(.+)$/)
  if (compareMatch) {
    const [, leftToken, operator, rightToken] = compareMatch

    const left = resolveValueToken(leftToken.trim(), item, context)
    const right = resolveValueToken(rightToken.trim(), item, context)

    if (left === MISSING || right === MISSING) return false

    const result = compareValues(left, right)

    switch (operator) {
      case "=":
        return result === 0
      case "<>":
        return result !== 0
      case "<":
        return result < 0
      case "<=":
        return result <= 0
      case ">":
        return result > 0
      case ">=":
        return result >= 0
      default:
        return false
    }
  }

  throw new NotSupportedError({
    method: context.method,
    featurePath: "ConditionExpression",
    reason: `Unsupported clause: ${source}`,
  })
}

const PLACEHOLDER_VALUE = /^:[A-Za-z_][A-Za-z0-9_]*$/
const PLACEHOLDER_NAME = /^#[A-Za-z_][A-Za-z0-9_]*$/
const NUMBER_LITERAL = /^-?\d+(?:\.\d+)?$/
const STRING_LITERAL = /^".*"$|^'.*'$/
const ATTRIBUTE_NAME = /^[A-Za-z_][A-Za-z0-9_.-]*$/
const DOCUMENT_PATH_TOKEN =
  /^(?:#[A-Za-z_][A-Za-z0-9_]*|[A-Za-z_][A-Za-z0-9_-]*)(?:\[\d+\])*(?:\.(?:#[A-Za-z_][A-Za-z0-9_]*|[A-Za-z_][A-Za-z0-9_-]*)(?:\[\d+\])*)*$/
const ATTRIBUTE_SEGMENT = /^[A-Za-z_][A-Za-z0-9_-]*/
const PLACEHOLDER_SEGMENT = /^#[A-Za-z_][A-Za-z0-9_]*/

type DocumentPathPart =
  | { type: "attribute"; value: string }
  | { type: "index"; value: number }

function resolveAttributeToken(token: string, context: ExpressionContext): string {
  const trimmed = token.trim()

  if (PLACEHOLDER_NAME.test(trimmed)) {
    const resolved = context.expressionAttributeNames?.[trimmed]
    if (!resolved) {
      throw new NotSupportedError({
        method: context.method,
        featurePath: `ExpressionAttributeNames.${trimmed}`,
        reason: "Missing expression attribute name placeholder.",
      })
    }

    return resolved
  }

  if (!ATTRIBUTE_NAME.test(trimmed)) {
    throw new NotSupportedError({
      method: context.method,
      featurePath: "ExpressionAttributeNames",
      reason: `Unsupported attribute token: ${trimmed}`,
    })
  }

  return trimmed
}

function resolveValueToken(
  token: string,
  item: InMemoryItem | undefined,
  context: ExpressionContext
): ResolvedValue {
  const trimmed = token.trim()

  if (PLACEHOLDER_VALUE.test(trimmed)) {
    if (!context.expressionAttributeValues) {
      throw new NotSupportedError({
        method: context.method,
        featurePath: "ExpressionAttributeValues",
        reason: "ExpressionAttributeValues are required for value placeholders.",
      })
    }

    if (!(trimmed in context.expressionAttributeValues)) {
      throw new NotSupportedError({
        method: context.method,
        featurePath: `ExpressionAttributeValues.${trimmed}`,
        reason: "Missing expression attribute value placeholder.",
      })
    }

    return context.expressionAttributeValues[trimmed]
  }

  if (PLACEHOLDER_NAME.test(trimmed)) {
    return resolveAttributeValue(trimmed, item, context)
  }

  if (STRING_LITERAL.test(trimmed)) {
    return trimmed.slice(1, -1)
  }

  const sizeMatch = trimmed.match(/^size\((.+)\)$/i)
  if (sizeMatch) {
    const value = resolveAttributeValue(sizeMatch[1].trim(), item, context)
    if (value === MISSING) return MISSING

    return sizeOfValue(value, context)
  }

  if (NUMBER_LITERAL.test(trimmed)) {
    return Number(trimmed)
  }

  if (trimmed === "true") return true
  if (trimmed === "false") return false
  if (trimmed === "null") return null

  if (DOCUMENT_PATH_TOKEN.test(trimmed) || ATTRIBUTE_NAME.test(trimmed)) {
    return resolveAttributeValue(trimmed, item, context)
  }

  throw new NotSupportedError({
    method: context.method,
    featurePath: "ExpressionValue",
    reason: `Unsupported value token: ${trimmed}`,
  })
}

function resolveAttributeValue(
  token: string,
  item: InMemoryItem | undefined,
  context: ExpressionContext
): ResolvedValue {
  const path = parseDocumentPath(token, context)
  if (!item) return MISSING

  let current: any = item

  for (const part of path) {
    if (part.type === "attribute") {
      if (!current || typeof current !== "object") return MISSING
      if (!(part.value in current)) return MISSING
      current = current[part.value]
      continue
    }

    if (!Array.isArray(current)) return MISSING
    if (!(part.value in current)) return MISSING
    current = current[part.value]
  }

  return current
}

function parseDocumentPath(
  token: string,
  context: ExpressionContext
): DocumentPathPart[] {
  const source = token.trim()
  if (!source) {
    throwUnsupportedAttributeToken(token, context)
  }

  const parts: DocumentPathPart[] = []
  let cursor = 0

  while (cursor < source.length) {
    const remaining = source.slice(cursor)
    const placeholderMatch = remaining.match(PLACEHOLDER_SEGMENT)
    const segmentMatch = remaining.match(ATTRIBUTE_SEGMENT)
    const segmentToken = placeholderMatch?.[0] ?? segmentMatch?.[0]

    if (!segmentToken) {
      throwUnsupportedAttributeToken(token, context)
    }

    if (segmentToken.startsWith("#")) {
      const resolved = context.expressionAttributeNames?.[segmentToken]
      if (!resolved) {
        throw new NotSupportedError({
          method: context.method,
          featurePath: `ExpressionAttributeNames.${segmentToken}`,
          reason: "Missing expression attribute name placeholder.",
        })
      }
      parts.push({ type: "attribute", value: resolved })
    } else {
      parts.push({ type: "attribute", value: segmentToken })
    }

    cursor += segmentToken.length

    while (source[cursor] === "[") {
      const start = cursor + 1
      let end = start
      while (end < source.length && /\d/.test(source[end])) end += 1

      if (start === end || source[end] !== "]") {
        throwUnsupportedAttributeToken(token, context)
      }

      parts.push({
        type: "index",
        value: Number(source.slice(start, end)),
      })
      cursor = end + 1
    }

    if (cursor >= source.length) break

    if (source[cursor] !== ".") {
      throwUnsupportedAttributeToken(token, context)
    }

    cursor += 1
    if (cursor >= source.length) {
      throwUnsupportedAttributeToken(token, context)
    }
  }

  return parts
}

function throwUnsupportedAttributeToken(
  token: string,
  context: ExpressionContext
): never {
  throw new NotSupportedError({
    method: context.method,
    featurePath: "ExpressionAttributeNames",
    reason: `Unsupported attribute token: ${token.trim()}`,
  })
}

function splitTopLevelByKeyword(
  expression: string,
  keyword: "and" | "or"
): string[] {
  const clauses: string[] = []
  let current = ""
  let depth = 0
  const marker = ` ${keyword} `

  for (let i = 0; i < expression.length; i += 1) {
    const char = expression[i]

    if (char === "(") depth += 1
    if (char === ")") depth = Math.max(0, depth - 1)

    if (
      depth === 0 &&
      expression.slice(i, i + marker.length).toLowerCase() === marker
    ) {
      clauses.push(current.trim())
      current = ""
      i += marker.length - 1
      continue
    }

    current += char
  }

  if (current.trim()) clauses.push(current.trim())

  return clauses
}

function splitTopLevelByDelimiter(
  expression: string,
  delimiter: "," | "+" | "-"
): string[] {
  const segments: string[] = []
  let current = ""
  let depth = 0

  for (let i = 0; i < expression.length; i += 1) {
    const char = expression[i]
    if (char === "(") depth += 1
    if (char === ")") depth = Math.max(0, depth - 1)

    if (depth === 0 && char === delimiter) {
      segments.push(current)
      current = ""
      continue
    }

    current += char
  }

  if (current.length > 0) segments.push(current)
  return segments
}

function splitTopLevelAssignment(
  assignment: string
): { left: string; right: string } | null {
  let depth = 0

  for (let i = 0; i < assignment.length; i += 1) {
    const char = assignment[i]
    if (char === "(") depth += 1
    if (char === ")") depth = Math.max(0, depth - 1)

    if (depth === 0 && char === "=") {
      const left = assignment.slice(0, i).trim()
      const right = assignment.slice(i + 1).trim()
      if (!left || !right) return null
      return { left, right }
    }
  }

  return null
}

function resolveUpdateSetValueToken(
  token: string,
  item: InMemoryItem | undefined,
  context: ExpressionContext
): any {
  const trimmed = token.trim()

  const arithmetic = splitTopLevelArithmetic(trimmed)
  if (arithmetic) {
    const left = resolveUpdateSetValueToken(arithmetic.left, item, context)
    const right = resolveUpdateSetValueToken(arithmetic.right, item, context)

    if (typeof left !== "number" || typeof right !== "number") {
      throw new NotSupportedError({
        method: context.method,
        featurePath: "UpdateExpression.SET",
        reason: "Arithmetic update operands must be numbers.",
      })
    }

    return arithmetic.operator === "+" ? left + right : left - right
  }

  const ifNotExists = parseFunctionCall(trimmed, "if_not_exists")
  if (ifNotExists) {
    if (ifNotExists.length !== 2) {
      throw new NotSupportedError({
        method: context.method,
        featurePath: "UpdateExpression.SET",
        reason: "if_not_exists expects exactly two arguments.",
      })
    }

    const existing = resolveAttributeValue(ifNotExists[0], item, context)
    if (existing !== MISSING) return existing

    return resolveUpdateSetValueToken(ifNotExists[1], item, context)
  }

  const listAppend = parseFunctionCall(trimmed, "list_append")
  if (listAppend) {
    if (listAppend.length !== 2) {
      throw new NotSupportedError({
        method: context.method,
        featurePath: "UpdateExpression.SET",
        reason: "list_append expects exactly two arguments.",
      })
    }

    const left = resolveUpdateSetValueToken(listAppend[0], item, context)
    const right = resolveUpdateSetValueToken(listAppend[1], item, context)

    if (!Array.isArray(left) || !Array.isArray(right)) {
      throw new NotSupportedError({
        method: context.method,
        featurePath: "UpdateExpression.SET",
        reason: "list_append expects list operands.",
      })
    }

    return [...left, ...right]
  }

  const value = resolveValueToken(trimmed, item, context)
  if (value === MISSING) {
    throw new NotSupportedError({
      method: context.method,
      featurePath: "UpdateExpression.SET",
      reason: "The provided expression refers to an attribute that does not exist in the item.",
    })
  }

  return value
}

function splitTopLevelArithmetic(
  source: string
): { left: string; right: string; operator: "+" | "-" } | null {
  let depth = 0

  for (let i = 0; i < source.length; i += 1) {
    const char = source[i]
    if (char === "(") depth += 1
    if (char === ")") depth = Math.max(0, depth - 1)
    if (depth !== 0) continue

    if (char === "+" || char === "-") {
      const left = source.slice(0, i).trim()
      const right = source.slice(i + 1).trim()

      if (!left || !right) continue
      return {
        left,
        right,
        operator: char,
      }
    }
  }

  return null
}

function parseFunctionCall(source: string, fnName: string): string[] | null {
  const regex = new RegExp(`^${fnName}\\((.*)\\)$`, "i")
  const match = source.match(regex)
  if (!match) return null

  return splitTopLevelByDelimiter(match[1], ",")
    .map((segment) => segment.trim())
    .filter(Boolean)
}

function sizeOfValue(value: any, context: ExpressionContext): number {
  if (typeof value === "string" || Array.isArray(value)) return value.length
  if (value instanceof Uint8Array || Buffer.isBuffer(value)) return value.length
  if (value instanceof Set) return value.size
  if (value && typeof value === "object") return Object.keys(value).length

  throw new NotSupportedError({
    method: context.method,
    featurePath: "ConditionExpression.size",
    reason: "size supports string, binary, list, set, and map values only.",
  })
}

function containsValue(container: any, expected: any): boolean {
  if (typeof container === "string") return container.includes(String(expected))
  if (Array.isArray(container)) return container.some((entry) => entry === expected)
  if (container instanceof Set) return container.has(expected)
  if (container && typeof container === "object") {
    return Object.prototype.hasOwnProperty.call(container, String(expected))
  }

  return false
}

function attributeMatchesType(value: any, expectedType: string): boolean {
  const t = expectedType.toUpperCase()

  if (t === "S") return typeof value === "string"
  if (t === "N") return typeof value === "number"
  if (t === "BOOL") return typeof value === "boolean"
  if (t === "NULL") return value === null
  if (t === "L") return Array.isArray(value)
  if (t === "M") return !!value && typeof value === "object" && !Array.isArray(value) && !Buffer.isBuffer(value) && !(value instanceof Set)
  if (t === "B") return Buffer.isBuffer(value) || value instanceof Uint8Array

  if (value instanceof Set) {
    const values = [...value.values()]
    if (t === "SS") return values.every((entry) => typeof entry === "string")
    if (t === "NS") return values.every((entry) => typeof entry === "number")
    if (t === "BS")
      return values.every(
        (entry) => Buffer.isBuffer(entry) || entry instanceof Uint8Array
      )
  }

  return false
}
