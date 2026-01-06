const INDENT_SIZE = 2
const ITEM_FIELD_INDENT = 4
const PRIORITY_FIELDS = ["PK", "SK"]
const PRIORITY_FIELD_SET = new Set(PRIORITY_FIELDS)
const ARRAY_ITEM_KEY = "[]"

type Snapshot = Record<string, any>
type RenderResult = { lines: string[]; changed: boolean }

const indent = (level: number) => " ".repeat(level)

const isPlainObject = (value: any): value is Record<string, any> =>
  value !== null && typeof value === "object" && !Array.isArray(value)

const orderKeys = (keys: string[]) => {
  const uniqueKeys = Array.from(new Set(keys))
  const prioritized = PRIORITY_FIELDS.filter((key) => uniqueKeys.includes(key))
  const rest = uniqueKeys
    .filter((key) => !PRIORITY_FIELD_SET.has(key))
    .sort()
  return [...prioritized, ...rest]
}

const isEqual = (left: any, right: any): boolean => {
  if (left === right) return true
  if (left === null || right === null) return left === right
  if (left === undefined || right === undefined) return left === right
  if (Array.isArray(left) && Array.isArray(right)) {
    if (left.length !== right.length) return false
    for (let i = 0; i < left.length; i += 1) {
      if (!isEqual(left[i], right[i])) return false
    }
    return true
  }
  if (isPlainObject(left) && isPlainObject(right)) {
    const leftKeys = Object.keys(left).sort()
    const rightKeys = Object.keys(right).sort()
    if (leftKeys.length !== rightKeys.length) return false
    for (let i = 0; i < leftKeys.length; i += 1) {
      const key = leftKeys[i]
      if (key !== rightKeys[i]) return false
      if (!isEqual(left[key], right[key])) return false
    }
    return true
  }
  if (left instanceof Date && right instanceof Date) {
    return left.toISOString() === right.toISOString()
  }
  return false
}

const formatScalar = (value: any): string => {
  if (value === null) return "null"
  if (value === undefined) return "undefined"
  if (typeof value === "string") return JSON.stringify(value)
  if (typeof value === "number" || typeof value === "boolean")
    return String(value)
  if (typeof value === "bigint") return `${value}n`
  if (value instanceof Date) return JSON.stringify(value.toISOString())

  return JSON.stringify(value)
}

const parseItemKey = (key: string, item?: Record<string, any>) => {
  const pk = typeof item?.PK === "string" ? item.PK : undefined
  const sk = typeof item?.SK === "string" ? item.SK : undefined

  if (pk && sk) return { pk, sk }

  const [rawPk, rawSk] = key.split("__")
  return {
    pk: pk ?? rawPk ?? "?",
    sk: sk ?? rawSk ?? "?",
  }
}

const formatItemHeader = (key: string, item?: Record<string, any>) => {
  const { pk, sk } = parseItemKey(key, item)
  return `[${pk} / ${sk}]`
}

const renderValueLines = (
  key: string | null,
  value: any,
  indentLevel: number
): string[] => {
  const prefix = indent(indentLevel)

  if (key === ARRAY_ITEM_KEY) {
    return renderArrayItemLines(value, indentLevel)
  }

  if (isPlainObject(value)) {
    const keys = orderKeys(Object.keys(value))
    if (keys.length === 0) {
      const label = key ? `${key}: {}` : "{}"
      return [`${prefix}${label}`]
    }

    const lines: string[] = []
    if (key) lines.push(`${prefix}${key}:`)
    keys.forEach((childKey) => {
      lines.push(
        ...renderValueLines(childKey, value[childKey], indentLevel + INDENT_SIZE)
      )
    })
    return lines
  }

  if (Array.isArray(value)) {
    if (value.length === 0) {
      const label = key ? `${key}: []` : "[]"
      return [`${prefix}${label}`]
    }

    const lines: string[] = []
    if (key) lines.push(`${prefix}${key}:`)
    value.forEach((item) => {
      lines.push(
        ...renderValueLines(ARRAY_ITEM_KEY, item, indentLevel + INDENT_SIZE)
      )
    })
    return lines
  }

  const label = key ? `${key}: ${formatScalar(value)}` : formatScalar(value)
  return [`${prefix}${label}`]
}

const applyPrefix = (line: string, prefix: "+" | "-") => {
  if (line.startsWith(" ")) return `${prefix}${line.slice(1)}`
  return `${prefix} ${line}`
}

const renderValueLinesWithPrefix = (
  key: string | null,
  value: any,
  indentLevel: number,
  prefix: "+" | "-"
) =>
  renderValueLines(key, value, indentLevel).map((line) =>
    applyPrefix(line, prefix)
  )

const renderArrayItemLines = (value: any, indentLevel: number): string[] => {
  const prefix = indent(indentLevel)

  if (isPlainObject(value)) {
    const keys = orderKeys(Object.keys(value))
    if (keys.length === 0) return [`${prefix}- {}`]

    const lines: string[] = []
    const [firstKey, ...restKeys] = keys
    const firstLines = renderValueLines(
      firstKey,
      value[firstKey],
      indentLevel + INDENT_SIZE
    )
    const trimmedFirstLine = firstLines[0].slice(indentLevel + INDENT_SIZE)
    lines.push(`${prefix}- ${trimmedFirstLine}`)
    lines.push(...firstLines.slice(1))

    restKeys.forEach((key) => {
      lines.push(...renderValueLines(key, value[key], indentLevel + INDENT_SIZE))
    })
    return lines
  }

  if (Array.isArray(value)) {
    if (value.length === 0) return [`${prefix}- []`]

    const lines: string[] = [`${prefix}-`]
    value.forEach((item) => {
      lines.push(
        ...renderValueLines(ARRAY_ITEM_KEY, item, indentLevel + INDENT_SIZE)
      )
    })
    return lines
  }

  return [`${prefix}- ${formatScalar(value)}`]
}

const findArrayMatches = (before: any[], after: any[]) => {
  const rows = before.length + 1
  const cols = after.length + 1
  const dp: number[][] = Array.from({ length: rows }, () =>
    Array(cols).fill(0)
  )

  for (let i = before.length - 1; i >= 0; i -= 1) {
    for (let j = after.length - 1; j >= 0; j -= 1) {
      if (isEqual(before[i], after[j])) {
        dp[i][j] = dp[i + 1][j + 1] + 1
      } else {
        dp[i][j] = Math.max(dp[i + 1][j], dp[i][j + 1])
      }
    }
  }

  const matches: Array<{ beforeMatch: number; afterMatch: number }> = []
  let i = 0
  let j = 0
  while (i < before.length && j < after.length) {
    if (isEqual(before[i], after[j])) {
      matches.push({ beforeMatch: i, afterMatch: j })
      i += 1
      j += 1
    } else if (dp[i + 1][j] >= dp[i][j + 1]) {
      i += 1
    } else {
      j += 1
    }
  }

  return matches
}

const renderArraySegment = (
  beforeSegment: any[],
  afterSegment: any[],
  indentLevel: number
): RenderResult => {
  const lines: string[] = []
  let changed = false

  if (beforeSegment.length === 0 && afterSegment.length === 0) {
    return { lines, changed: false }
  }

  if (beforeSegment.length === afterSegment.length) {
    for (let i = 0; i < beforeSegment.length; i += 1) {
      const result = renderFieldDiff(
        ARRAY_ITEM_KEY,
        beforeSegment[i],
        afterSegment[i],
        indentLevel
      )
      if (result.lines.length > 0) lines.push(...result.lines)
      if (result.changed) changed = true
    }
    return { lines, changed }
  }

  beforeSegment.forEach((value) => {
    lines.push(
      ...renderValueLinesWithPrefix(ARRAY_ITEM_KEY, value, indentLevel, "-")
    )
    changed = true
  })
  afterSegment.forEach((value) => {
    lines.push(
      ...renderValueLinesWithPrefix(ARRAY_ITEM_KEY, value, indentLevel, "+")
    )
    changed = true
  })
  return { lines, changed }
}

const renderArrayDiff = (
  key: string,
  before: any[],
  after: any[],
  indentLevel: number
): RenderResult => {
  const lines: string[] = [`${indent(indentLevel)}${key}:`]
  let changed = false

  const matches = findArrayMatches(before, after)
  let beforeIndex = 0
  let afterIndex = 0

  matches.forEach(({ beforeMatch, afterMatch }) => {
    const segment = renderArraySegment(
      before.slice(beforeIndex, beforeMatch),
      after.slice(afterIndex, afterMatch),
      indentLevel + INDENT_SIZE
    )
    if (segment.lines.length > 0) lines.push(...segment.lines)
    if (segment.changed) changed = true

    lines.push(
      ...renderValueLines(
        ARRAY_ITEM_KEY,
        before[beforeMatch],
        indentLevel + INDENT_SIZE
      )
    )

    beforeIndex = beforeMatch + 1
    afterIndex = afterMatch + 1
  })

  const tailSegment = renderArraySegment(
    before.slice(beforeIndex),
    after.slice(afterIndex),
    indentLevel + INDENT_SIZE
  )
  if (tailSegment.lines.length > 0) lines.push(...tailSegment.lines)
  if (tailSegment.changed) changed = true

  return { lines, changed }
}

const renderObjectDiff = (
  key: string,
  before: Record<string, any>,
  after: Record<string, any>,
  indentLevel: number
): RenderResult => {
  const lines: string[] = [`${indent(indentLevel)}${key}:`]
  let changed = false
  const keys = orderKeys([...Object.keys(before), ...Object.keys(after)])

  keys.forEach((childKey) => {
    const result = renderFieldDiff(
      childKey,
      before[childKey],
      after[childKey],
      indentLevel + INDENT_SIZE
    )
    if (result.lines.length > 0) lines.push(...result.lines)
    if (result.changed) changed = true
  })

  return { lines, changed }
}

const renderFieldDiff = (
  key: string,
  beforeValue: any,
  afterValue: any,
  indentLevel: number
): RenderResult => {
  if (beforeValue === undefined && afterValue === undefined) {
    return { lines: [], changed: false }
  }

  if (isEqual(beforeValue, afterValue)) {
    return {
      lines: renderValueLines(key, beforeValue, indentLevel),
      changed: false,
    }
  }

  if (beforeValue === undefined) {
    return {
      lines: renderValueLinesWithPrefix(key, afterValue, indentLevel, "+"),
      changed: true,
    }
  }

  if (afterValue === undefined) {
    return {
      lines: renderValueLinesWithPrefix(key, beforeValue, indentLevel, "-"),
      changed: true,
    }
  }

  if (Array.isArray(beforeValue) && Array.isArray(afterValue)) {
    return renderArrayDiff(key, beforeValue, afterValue, indentLevel)
  }

  if (isPlainObject(beforeValue) && isPlainObject(afterValue)) {
    return renderObjectDiff(key, beforeValue, afterValue, indentLevel)
  }

  return {
    lines: [
      ...renderValueLinesWithPrefix(key, beforeValue, indentLevel, "-"),
      ...renderValueLinesWithPrefix(key, afterValue, indentLevel, "+"),
    ],
    changed: true,
  }
}

const renderItemFields = (item: Record<string, any>, prefix: "+" | "-") => {
  const keys = orderKeys(Object.keys(item))

  const lines: string[] = []
  keys.forEach((key) => {
    lines.push(
      ...renderValueLinesWithPrefix(key, item[key], ITEM_FIELD_INDENT, prefix)
    )
  })
  return lines
}

const renderItemFieldsDiff = (
  before: Record<string, any>,
  after: Record<string, any>
): RenderResult => {
  const keys = orderKeys([...Object.keys(before), ...Object.keys(after)])
  const lines: string[] = []
  let changed = false

  keys.forEach((key) => {
    const result = renderFieldDiff(
      key,
      before[key],
      after[key],
      ITEM_FIELD_INDENT
    )
    if (result.lines.length > 0) lines.push(...result.lines)
    if (result.changed) changed = true
  })

  return { lines, changed }
}

export const formatSnapshotDiff = (before: Snapshot, after: Snapshot) => {
  const keys = new Set([...Object.keys(before), ...Object.keys(after)])
  const sortedKeys = Array.from(keys).sort((a, b) => {
    const itemA = after[a] ?? before[a]
    const itemB = after[b] ?? before[b]
    return formatItemHeader(a, itemA).localeCompare(formatItemHeader(b, itemB))
  })

  const lines: string[] = []

  sortedKeys.forEach((key) => {
    const beforeItem = before[key]
    const afterItem = after[key]
    const header = formatItemHeader(key, afterItem ?? beforeItem)

    let itemLines: string[] = []
    if (!beforeItem && afterItem) {
      itemLines = [`+ ${header}`, ...renderItemFields(afterItem, "+")]
    } else if (beforeItem && !afterItem) {
      itemLines = [`- ${header}`, ...renderItemFields(beforeItem, "-")]
    } else if (beforeItem && afterItem) {
      const diffResult = renderItemFieldsDiff(beforeItem, afterItem)
      if (diffResult.changed) itemLines = [header, ...diffResult.lines]
    }

    if (itemLines.length > 0) {
      if (lines.length > 0) lines.push("")
      lines.push(...itemLines)
    }
  })

  return lines.join("\n")
}
