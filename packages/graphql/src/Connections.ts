import { Effect } from "effect"
import {
  GraphQLBoolean,
  GraphQLInt,
  GraphQLString,
  type GraphQLFieldConfigArgumentMap,
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
  type GraphQLFieldConfigMap,
  type GraphQLNamedType,
  type GraphQLInterfaceType,
  type GraphQLUnionType,
  type GraphQLResolveInfo,
} from "graphql"
import { type ErrorPolicy, fieldError, protect } from "./Errors.ts"
import { type Field, type RequestContext } from "./FieldTypes.ts"
import {
  type AnyResolve,
  type AnyConfig,
  config,
  resolverField,
} from "./Resolver.ts"
import { invalid } from "./SchemaTypes.ts"

export interface PageInfo {
  readonly hasNextPage: boolean
  readonly hasPreviousPage: boolean
  readonly startCursor: string | null
  readonly endCursor: string | null
}

export interface Edge<A> {
  readonly cursor: string
  readonly node: A
}
/** Structurally identical to model-ts' PaginationResult, so paginate() results pass through. */

export interface Connection<A> {
  readonly edges: ReadonlyArray<Edge<A>>
  readonly pageInfo: PageInfo
}

export interface ConnectionArgs {
  readonly first?: number | null
  readonly after?: string | null
  readonly last?: number | null
  readonly before?: string | null
}

export const encodeCursor = (offset: number) => btoa(`cursor:${offset}`)

export function decodeCursor(cursor: string): number {
  try {
    const match = /^cursor:(\d+)$/.exec(atob(cursor))
    if (match && Number.isSafeInteger(Number(match[1]))) return Number(match[1])
  } catch {
    /* Reject invalid base64 using the same public error. */
  }
  throw fieldError("Invalid cursor", { code: "BAD_USER_INPUT" })
}

/** graphql-js has already validated the four built-in pagination arguments. */
export function paginationArgs(
  raw: Readonly<Record<string, unknown>>,
): ConnectionArgs {
  const pick = <K extends keyof ConnectionArgs>(key: K) =>
    raw[key] == null ? {} : { [key]: raw[key] }
  return {
    ...pick("first"),
    ...pick("after"),
    ...pick("last"),
    ...pick("before"),
  } as ConnectionArgs
}

export function validatePagination(
  args: ConnectionArgs,
  coordinate: string,
  max: number,
): void {
  const reject = (message: string): never => {
    throw fieldError(`${coordinate}: ${message}`, { code: "BAD_USER_INPUT" })
  }
  if (args.after != null && args.before != null)
    reject('use "after" or "before", not both')
  if (args.first != null && args.last != null)
    reject('use "first" or "last", not both')
  if (args.first != null && args.before != null)
    reject('"before" cannot be combined with "first"')
  if (args.last != null && args.after != null)
    reject('"after" cannot be combined with "last"')
  for (const key of ["first", "last"] as const) {
    const size = args[key]
    if (size != null && (size < 0 || size > max))
      reject(`"${key}" must be between 0 and ${max}`)
  }
}

/** Validation sees only caller arguments; a before-only request defaults backward. */
export function pageDefaults(
  args: ConnectionArgs,
  size: number,
): ConnectionArgs {
  if (args.first != null || args.last != null) return args
  return args.before != null
    ? { ...args, last: size }
    : { ...args, first: size }
}

export function slice<A>(
  nodes: ReadonlyArray<A>,
  args: ConnectionArgs,
): Connection<A> {
  let start =
    args.after == null
      ? 0
      : Math.min(nodes.length, decodeCursor(args.after) + 1)
  let end =
    args.before == null
      ? nodes.length
      : Math.min(nodes.length, decodeCursor(args.before))
  if (args.first != null) end = Math.min(end, start + args.first)
  if (args.last != null) start = Math.max(start, end - args.last)
  const edges = nodes
    .slice(start, Math.max(start, end))
    .map((node, index) => ({ cursor: encodeCursor(start + index), node }))
  return {
    edges,
    pageInfo: {
      hasPreviousPage: start > 0,
      hasNextPage: end < nodes.length,
      startCursor: edges[0]?.cursor ?? null,
      endCursor: edges.at(-1)?.cursor ?? null,
    },
  }
}

export const connectionArgs: GraphQLFieldConfigArgumentMap = {
  first: {
    type: GraphQLInt,
    description: "Returns the first n elements from the list.",
  },
  after: {
    type: GraphQLString,
    description:
      "Returns the elements in the list that come after the specified cursor",
  },
  last: {
    type: GraphQLInt,
    description: "Returns the last n elements from the list.",
  },
  before: {
    type: GraphQLString,
    description:
      "Returns the elements in the list that come before the specified cursor",
  },
}

export const connectionField = (
  policy: ErrorPolicy,
  maxPageSize: number,
  defaultPageSize: number,
  node: string,
  input: AnyResolve | AnyConfig = {},
): Field<unknown> => {
  const options = config(input)
  const { resolve, nodes, totalCount } = options
  for (const key of Object.keys(options.args ?? {}))
    if (Object.hasOwn(connectionArgs, key))
      invalid(`Reserved connection argument: ${key}`)
  return resolverField(policy, options, {
    type: (context, name) =>
      context.connection(
        node,
        options.name,
        name,
        options.totalCount !== undefined,
        options.edgeName,
      ),
    args: options.args ?? {},
    extraArgs: connectionArgs,
    members: [],
    call: (source, raw, decoded, coordinate, context) => {
      const supplied = paginationArgs(raw)
      validatePagination(supplied, coordinate, maxPageSize)
      const page = nodes ? pageDefaults(supplied, defaultPageSize) : supplied
      if (nodes) {
        if (page.after != null) decodeCursor(page.after)
        if (page.before != null) decodeCursor(page.before)
      }
      // Resolver and nodes overloads prove these shapes; this runtime path
      // handles heterogeneous field values after their type parameters are erased.
      const value = resolve
        ? resolve(source, { ...(decoded as object), ...page }, context)
        : nodes
          ? nodes(source, decoded, context)
          : Reflect.get(
              Object(source),
              coordinate.slice(coordinate.lastIndexOf(".") + 1),
            )
      if (!totalCount) return value
      const finish = (result: unknown) => ({
        ...(resolve
          ? (result as object)
          : slice(result as readonly unknown[], page)),
        // graphql-js invokes function-valued fields only when selected, including
        // fragments and directives. Keep the Effect bridge and policy at that boundary.
        totalCount: (
          _args: unknown,
          request: RequestContext,
          info: GraphQLResolveInfo,
        ) =>
          request.run(
            protect(() => totalCount(source, decoded, context), [], policy, {
              coordinate: `${info.parentType.name}.${info.fieldName}`,
              phase: "resolve",
            }),
          ),
      })
      return Effect.isEffect(value) ? Effect.map(value, finish) : finish(value)
    },
    finish: (value, raw) =>
      !nodes || options.totalCount
        ? value
        : slice(
            value as ReadonlyArray<unknown>,
            pageDefaults(paginationArgs(raw), defaultPageSize),
          ),
  })
}

/** Derived names are reserved only when a field actually requests them. */
export function makeConnectionTypes(
  named: Map<string, GraphQLNamedType>,
  add: <T extends GraphQLNamedType>(type: T) => T,
  nodeType: (
    name: string,
    coordinate: string,
  ) => GraphQLObjectType | GraphQLInterfaceType | GraphQLUnionType,
  isNode: (name: string) => boolean,
) {
  const generated = new Map<
    string,
    { readonly type: GraphQLObjectType; readonly shape: string }
  >()
  const derivedObject = (
    name: string,
    shape: string,
    fields: () => GraphQLFieldConfigMap<unknown, unknown>,
    description?: string,
  ) => {
    const existing = generated.get(name)
    if (existing) {
      if (existing.shape !== shape)
        invalid(`Incompatible derived type ${name}; use a different edgeName`)
      return existing.type
    }
    // add owns the global namespace: a real object may use a synthetic-looking
    // name, but deriving that same name must report a collision.
    const type = add(new GraphQLObjectType({ name, fields, description }))
    generated.set(name, { type, shape })
    return type
  }
  const pageInfo = () =>
    derivedObject(
      "PageInfo",
      "pageInfo",
      () => ({
        hasNextPage: {
          type: new GraphQLNonNull(GraphQLBoolean),
          description:
            "Used to indicate whether more edges exist following the set defined by the clients arguments.",
        },
        hasPreviousPage: {
          type: new GraphQLNonNull(GraphQLBoolean),
          description:
            "Used to indicate whether more edges exist prior to the set defined by the clients arguments.",
        },
        startCursor: {
          type: GraphQLString,
          description:
            "The cursor corresponding to the first nodes in edges. Null if the connection is empty.",
        },
        endCursor: {
          type: GraphQLString,
          description:
            "The cursor corresponding to the last nodes in edges. Null if the connection is empty.",
        },
      }),
      "PageInfo cursor, as defined in https://facebook.github.io/relay/graphql/connections.htm#sec-undefined.PageInfo",
    )
  const edgeNodes = new Map<string, string>()
  const edgeType = (node: string, name = `${node}Edge`) => {
    const owner = edgeNodes.get(name)
    if (owner !== undefined && owner !== node)
      invalid(
        `Incompatible edge ${name}: nodes ${owner} and ${node}; use a different edgeName`,
      )
    edgeNodes.set(name, node)
    return derivedObject(name, `edge:${node}`, () => ({
      cursor: {
        type: new GraphQLNonNull(GraphQLString),
        description:
          "https://facebook.github.io/relay/graphql/connections.htm#sec-Cursor",
      },
      node: {
        type: new GraphQLNonNull(nodeType(node, `${name}.node`)),
        description:
          "https://facebook.github.io/relay/graphql/connections.htm#sec-Node",
      },
    }))
  }
  const connectionEdges = new Map<string, string>()
  const connectionNodes = new Map<string, string>()
  const countedConnections = new Map<string, boolean>()
  const connectionType = (
    node: string,
    name: string,
    counted = false,
    edgeName = `${node}Edge`,
  ): GraphQLObjectType => {
    const existing = named.get(name)
    if (existing) {
      if (
        connectionNodes.get(name) !== node ||
        countedConnections.get(name) !== counted ||
        connectionEdges.get(name) !== edgeName
      )
        invalid(
          `Duplicate GraphQL name: ${name}; already used for ${connectionNodes.get(name) ?? "another type"}`,
        )
      if (!(existing instanceof GraphQLObjectType))
        return invalid(`Expected connection object: ${name}`)
      return existing
    }
    connectionEdges.set(name, edgeName)
    connectionNodes.set(name, node)
    countedConnections.set(name, counted)
    return add(
      new GraphQLObjectType({
        name,
        fields: () => ({
          edges: {
            description:
              "https://facebook.github.io/relay/graphql/connections.htm#sec-Edge-Types",
            type: new GraphQLNonNull(
              new GraphQLList(new GraphQLNonNull(edgeType(node, edgeName))),
            ),
          },
          pageInfo: {
            type: new GraphQLNonNull(pageInfo()),
            description:
              "https://facebook.github.io/relay/graphql/connections.htm#sec-undefined.PageInfo",
          },
          ...(counted
            ? { totalCount: { type: new GraphQLNonNull(GraphQLInt) } }
            : {}),
        }),
      }),
    )
  }
  function derived(name: string): GraphQLNamedType | undefined {
    if (name === "PageInfo") return pageInfo()
    for (const suffix of ["Edge", "Connection"] as const) {
      const node = name.slice(0, -suffix.length)
      if (!name.endsWith(suffix) || !isNode(node)) continue
      return suffix === "Edge" ? edgeType(node) : connectionType(node, name)
    }
    return undefined
  }

  return { derived, connectionType }
}
