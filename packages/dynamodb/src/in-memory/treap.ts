interface TreapNode<V> {
  key: string
  priority: number
  value: V
  left: TreapNode<V> | null
  right: TreapNode<V> | null
}

export interface TreapBounds {
  lower?: { key: string; inclusive: boolean }
  upper?: { key: string; inclusive: boolean }
}

export class DeterministicTreap<V> {
  private root: TreapNode<V> | null = null
  private _size = 0

  get size() {
    return this._size
  }

  insert(key: string, value: V, priority: number): void {
    const [nextRoot, inserted] = this.insertNode(this.root, key, value, priority)
    this.root = nextRoot
    if (inserted) this._size += 1
  }

  remove(key: string): boolean {
    const [nextRoot, removed] = this.removeNode(this.root, key)
    this.root = nextRoot
    if (removed) this._size -= 1
    return removed
  }

  has(key: string): boolean {
    let current = this.root
    while (current) {
      if (key === current.key) return true
      current = key < current.key ? current.left : current.right
    }
    return false
  }

  *iterate(
    direction: "asc" | "desc",
    bounds: TreapBounds = {}
  ): IterableIterator<{ key: string; value: V }> {
    if (direction === "asc") {
      yield* this.iterateAsc(bounds)
      return
    }

    yield* this.iterateDesc(bounds)
  }

  clear() {
    this.root = null
    this._size = 0
  }

  private rotateRight(node: TreapNode<V>): TreapNode<V> {
    const left = node.left
    if (!left) return node

    node.left = left.right
    left.right = node
    return left
  }

  private rotateLeft(node: TreapNode<V>): TreapNode<V> {
    const right = node.right
    if (!right) return node

    node.right = right.left
    right.left = node
    return right
  }

  private insertNode(
    node: TreapNode<V> | null,
    key: string,
    value: V,
    priority: number
  ): [TreapNode<V>, boolean] {
    if (!node) {
      return [
        {
          key,
          priority,
          value,
          left: null,
          right: null,
        },
        true,
      ]
    }

    if (key === node.key) {
      node.value = value
      return [node, false]
    }

    if (key < node.key) {
      const [next, inserted] = this.insertNode(node.left, key, value, priority)
      node.left = next

      if (node.left && node.left.priority < node.priority) {
        node = this.rotateRight(node)
      }

      return [node, inserted]
    }

    const [next, inserted] = this.insertNode(node.right, key, value, priority)
    node.right = next

    if (node.right && node.right.priority < node.priority) {
      node = this.rotateLeft(node)
    }

    return [node, inserted]
  }

  private removeNode(
    node: TreapNode<V> | null,
    key: string
  ): [TreapNode<V> | null, boolean] {
    if (!node) return [null, false]

    if (key < node.key) {
      const [next, removed] = this.removeNode(node.left, key)
      node.left = next
      return [node, removed]
    }

    if (key > node.key) {
      const [next, removed] = this.removeNode(node.right, key)
      node.right = next
      return [node, removed]
    }

    if (!node.left) return [node.right, true]
    if (!node.right) return [node.left, true]

    if (node.left.priority < node.right.priority) {
      const rotated = this.rotateRight(node)
      const [next, removed] = this.removeNode(rotated.right, key)
      rotated.right = next
      return [rotated, removed]
    }

    const rotated = this.rotateLeft(node)
    const [next, removed] = this.removeNode(rotated.left, key)
    rotated.left = next
    return [rotated, removed]
  }

  private *iterateAsc(bounds: TreapBounds): IterableIterator<{ key: string; value: V }> {
    const stack: TreapNode<V>[] = []

    const pushLeft = (node: TreapNode<V> | null) => {
      while (node) {
        if (this.isBelowLowerBound(node.key, bounds.lower)) {
          node = node.right
        } else {
          stack.push(node)
          node = node.left
        }
      }
    }

    pushLeft(this.root)

    while (stack.length) {
      const node = stack.pop()!

      if (this.isAboveUpperBound(node.key, bounds.upper)) {
        return
      }

      if (!this.isBelowLowerBound(node.key, bounds.lower)) {
        yield { key: node.key, value: node.value }
      }

      pushLeft(node.right)
    }
  }

  private *iterateDesc(bounds: TreapBounds): IterableIterator<{ key: string; value: V }> {
    const stack: TreapNode<V>[] = []

    const pushRight = (node: TreapNode<V> | null) => {
      while (node) {
        if (this.isAboveUpperBound(node.key, bounds.upper)) {
          node = node.left
        } else {
          stack.push(node)
          node = node.right
        }
      }
    }

    pushRight(this.root)

    while (stack.length) {
      const node = stack.pop()!

      if (this.isBelowLowerBound(node.key, bounds.lower)) {
        return
      }

      if (!this.isAboveUpperBound(node.key, bounds.upper)) {
        yield { key: node.key, value: node.value }
      }

      pushRight(node.left)
    }
  }

  private isBelowLowerBound(
    key: string,
    lower?: { key: string; inclusive: boolean }
  ): boolean {
    if (!lower) return false

    if (lower.inclusive) return key < lower.key
    return key <= lower.key
  }

  private isAboveUpperBound(
    key: string,
    upper?: { key: string; inclusive: boolean }
  ): boolean {
    if (!upper) return false

    if (upper.inclusive) return key > upper.key
    return key >= upper.key
  }
}
