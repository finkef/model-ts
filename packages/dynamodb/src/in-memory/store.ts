import { GSI, GSI_NAMES } from "../gsi"
import {
  ParsedKeyCondition,
  RangeCondition,
  compareValues,
  matchesRangeCondition,
} from "./expression"
import { DeterministicTreap, TreapBounds } from "./treap"
import {
  InMemoryItem,
  cloneItem,
  encodeIndexEntryKey,
  encodeItemKey,
  stablePriority,
  sortItemsByPKSK,
} from "./utils"
import { InMemoryIndexName } from "./spec"

interface IndexDescriptor {
  name: InMemoryIndexName
  hashAttribute: string
  rangeAttribute: string
}

export const PRIMARY_INDEX_NAME: InMemoryIndexName = "primary"

const INDEX_DESCRIPTORS: IndexDescriptor[] = [
  {
    name: PRIMARY_INDEX_NAME,
    hashAttribute: "PK",
    rangeAttribute: "SK",
  },
  ...GSI_NAMES.map((name) => ({
    name,
    hashAttribute: `${name}PK`,
    rangeAttribute: `${name}SK`,
  })),
]

const INDEX_BY_NAME = Object.fromEntries(
  INDEX_DESCRIPTORS.map((descriptor) => [descriptor.name, descriptor])
) as Record<InMemoryIndexName, IndexDescriptor>

export interface QueryCandidate {
  entryKey: string
  itemKey: string
  item: InMemoryItem
}

export interface QueryCursor {
  itemKey: string
  rangeKey: string
}

export class InMemoryTableState {
  private readonly itemStore = new Map<string, InMemoryItem>()

  private readonly indexes = new Map<
    InMemoryIndexName,
    Map<string, DeterministicTreap<string>>
  >(
    INDEX_DESCRIPTORS.map((descriptor) => [descriptor.name, new Map()])
  )

  cloneItemByKey(key: { PK: string; SK: string }): InMemoryItem | undefined {
    return this.cloneItemByItemKey(encodeItemKey(key.PK, key.SK))
  }

  cloneItemByItemKey(itemKey: string): InMemoryItem | undefined {
    const existing = this.itemStore.get(itemKey)
    return existing ? cloneItem(existing) : undefined
  }

  put(item: InMemoryItem): InMemoryItem | undefined {
    const key = this.getValidatedPrimaryKey(item)
    const itemKey = encodeItemKey(key.PK, key.SK)
    const previous = this.itemStore.get(itemKey)

    if (previous) {
      this.removeFromIndexes(itemKey, previous)
    }

    const stored = cloneItem(item)
    this.itemStore.set(itemKey, stored)
    this.addToIndexes(itemKey, stored)

    return previous ? cloneItem(previous) : undefined
  }

  deleteByKey(key: { PK: string; SK: string }): InMemoryItem | undefined {
    const itemKey = encodeItemKey(key.PK, key.SK)
    const previous = this.itemStore.get(itemKey)

    if (!previous) return undefined

    this.itemStore.delete(itemKey)
    this.removeFromIndexes(itemKey, previous)

    return cloneItem(previous)
  }

  iterateQueryCandidates(args: {
    indexName: InMemoryIndexName
    hashKey: string
    rangeCondition?: RangeCondition
    scanIndexForward: boolean
    exclusiveStartKey?: QueryCursor
  }): IterableIterator<QueryCandidate> {
    const descriptor = INDEX_BY_NAME[args.indexName]
    const partition = this.indexes.get(args.indexName)?.get(args.hashKey)

    if (!partition) {
      return [][Symbol.iterator]()
    }

    const bounds = this.toTreapBounds(args.rangeCondition)
    const direction = args.scanIndexForward ? "asc" : "desc"

    const iterator = partition.iterate(direction, bounds)
    const exclusiveStartEntryKey = args.exclusiveStartKey
      ? encodeIndexEntryKey(args.exclusiveStartKey.rangeKey, args.exclusiveStartKey.itemKey)
      : undefined

    const table = this

    function* generate(): IterableIterator<QueryCandidate> {
      for (const { key: entryKey, value: itemKey } of iterator) {
        if (exclusiveStartEntryKey) {
          if (direction === "asc" && entryKey <= exclusiveStartEntryKey) {
            continue
          }

          if (direction === "desc" && entryKey >= exclusiveStartEntryKey) {
            continue
          }
        }

        const item = table.itemStore.get(itemKey)
        if (!item) continue

        if (args.rangeCondition) {
          const rangeValue = item[descriptor.rangeAttribute]
          if (!matchesRangeCondition(rangeValue, args.rangeCondition)) continue
        }

        yield {
          entryKey,
          itemKey,
          item: cloneItem(item),
        }
      }
    }

    return generate()
  }

  scanItems(exclusiveStartKey?: { PK: string; SK: string }): InMemoryItem[] {
    const sorted = sortItemsByPKSK([...this.itemStore.values()].map(cloneItem))
    if (!exclusiveStartKey) return sorted

    const startPK = exclusiveStartKey.PK
    const startSK = exclusiveStartKey.SK

    return sorted.filter((item) => {
      const pk = String(item.PK)
      const sk = String(item.SK)

      if (pk > startPK) return true
      if (pk < startPK) return false

      return sk > startSK
    })
  }

  createQueryCursor(indexName: InMemoryIndexName, item: InMemoryItem): QueryCursor {
    const descriptor = INDEX_BY_NAME[indexName]

    return {
      itemKey: encodeItemKey(String(item.PK), String(item.SK)),
      rangeKey: String(item[descriptor.rangeAttribute]),
    }
  }

  getIndexKeyFromItem(
    indexName: InMemoryIndexName,
    item: InMemoryItem
  ): { hash: string; range: string } | null {
    const descriptor = INDEX_BY_NAME[indexName]
    const hash = item[descriptor.hashAttribute]
    const range = item[descriptor.rangeAttribute]

    if (indexName === PRIMARY_INDEX_NAME) {
      if (typeof hash !== "string" || typeof range !== "string") return null
      return { hash, range }
    }

    if (typeof hash !== "string" || typeof range !== "string") return null
    return { hash, range }
  }

  getDescriptor(indexName: InMemoryIndexName): IndexDescriptor {
    return INDEX_BY_NAME[indexName]
  }

  hasItem(key: { PK: string; SK: string }): boolean {
    return this.itemStore.has(encodeItemKey(key.PK, key.SK))
  }

  snapshot(): { [key: string]: any } {
    const entries = sortItemsByPKSK([...this.itemStore.values()]).map(cloneItem)

    return Object.fromEntries(
      entries.map((item) => [`${item.PK}__${item.SK}`, item])
    )
  }

  clear() {
    this.itemStore.clear()
    for (const partitionMap of this.indexes.values()) {
      partitionMap.clear()
    }
  }

  private getValidatedPrimaryKey(item: InMemoryItem): { PK: string; SK: string } {
    if (typeof item.PK !== "string" || typeof item.SK !== "string") {
      throw new Error("Primary key attributes PK and SK must be strings.")
    }

    return { PK: item.PK, SK: item.SK }
  }

  private addToIndexes(itemKey: string, item: InMemoryItem) {
    for (const descriptor of INDEX_DESCRIPTORS) {
      const projected = this.getIndexKeyFromItem(descriptor.name, item)
      if (!projected) continue

      const partitionMap = this.indexes.get(descriptor.name)!
      const tree =
        partitionMap.get(projected.hash) ??
        (() => {
          const created = new DeterministicTreap<string>()
          partitionMap.set(projected.hash, created)
          return created
        })()

      const entryKey = encodeIndexEntryKey(projected.range, itemKey)
      tree.insert(
        entryKey,
        itemKey,
        stablePriority(descriptor.name, projected.hash, projected.range, itemKey)
      )
    }
  }

  private removeFromIndexes(itemKey: string, item: InMemoryItem) {
    for (const descriptor of INDEX_DESCRIPTORS) {
      const projected = this.getIndexKeyFromItem(descriptor.name, item)
      if (!projected) continue

      const partitionMap = this.indexes.get(descriptor.name)!
      const tree = partitionMap.get(projected.hash)
      if (!tree) continue

      const entryKey = encodeIndexEntryKey(projected.range, itemKey)
      tree.remove(entryKey)

      if (tree.size === 0) {
        partitionMap.delete(projected.hash)
      }
    }
  }

  private toTreapBounds(rangeCondition?: RangeCondition): TreapBounds {
    if (!rangeCondition) return {}

    switch (rangeCondition.type) {
      case "begins_with": {
        const lower = encodeIndexEntryKey(rangeCondition.value, "")
        const upper = encodeIndexEntryKey(`${rangeCondition.value}\uffff`, "")
        return {
          lower: { key: lower, inclusive: true },
          upper: { key: upper, inclusive: true },
        }
      }
      case "between": {
        const lower = encodeIndexEntryKey(String(rangeCondition.lower), "")
        const upper = encodeIndexEntryKey(String(rangeCondition.upper), "\uffff")
        return {
          lower: { key: lower, inclusive: true },
          upper: { key: upper, inclusive: true },
        }
      }
      case "=": {
        const key = String(rangeCondition.value)
        return {
          lower: { key: encodeIndexEntryKey(key, ""), inclusive: true },
          upper: { key: encodeIndexEntryKey(key, "\uffff"), inclusive: true },
        }
      }
      case ">":
        return {
          lower: {
            key: encodeIndexEntryKey(String(rangeCondition.value), "\uffff"),
            inclusive: false,
          },
        }
      case ">=":
        return {
          lower: {
            key: encodeIndexEntryKey(String(rangeCondition.value), ""),
            inclusive: true,
          },
        }
      case "<":
        return {
          upper: {
            key: encodeIndexEntryKey(String(rangeCondition.value), ""),
            inclusive: false,
          },
        }
      case "<=":
        return {
          upper: {
            key: encodeIndexEntryKey(String(rangeCondition.value), "\uffff"),
            inclusive: true,
          },
        }
    }
  }
}

export const isGSI = (indexName: InMemoryIndexName): indexName is GSI =>
  indexName !== PRIMARY_INDEX_NAME

export const parseIndexName = (indexName?: string): InMemoryIndexName =>
  (indexName ?? PRIMARY_INDEX_NAME) as InMemoryIndexName

export const isSupportedIndexName = (indexName: string): indexName is InMemoryIndexName =>
  indexName === PRIMARY_INDEX_NAME || GSI_NAMES.includes(indexName as GSI)

export const matchesKeyConditionDescriptor = (
  indexName: InMemoryIndexName,
  condition: ParsedKeyCondition
): boolean => {
  const descriptor = INDEX_BY_NAME[indexName]

  if (condition.hashAttribute !== descriptor.hashAttribute) return false
  if (!condition.range) return true

  return condition.range.attribute === descriptor.rangeAttribute
}

export const compareItemKey = (
  left: { PK: string; SK: string },
  right: { PK: string; SK: string }
): number => {
  const pkCmp = compareValues(left.PK, right.PK)
  if (pkCmp !== 0) return pkCmp
  return compareValues(left.SK, right.SK)
}
