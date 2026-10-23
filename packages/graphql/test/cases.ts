export interface NegativeCase {
  name: string
  file: string
  before: string
  after: string
  prefix?: string
  root?: boolean
  message: RegExp
  expectedFile?: string
  lineAnchor?: string
  additional?: readonly { file: string; before: string; after: string }[]
}
const list = "src/api/graphql/list.ts"
const workspace = "src/api/graphql/workspace.ts"
const update = "src/api/graphql/mutation/list/list-update.ts"
const query = "src/api/graphql/query/workspace.ts"
const fixture = "tests/fixtures/definitions/objects.ts"
const field = (
  name: string,
  before: string,
  after: string,
  message: RegExp,
): NegativeCase => ({ name, file: fixture, before, after, message })
export const negativeCases: readonly NegativeCase[] = [
  ...["Query", "Event"].map((kind): NegativeCase => ({
    name: `formatted executor ${kind.toLowerCase()} errors expose message and extensions, not originalError`,
    file: "tests/fixtures/execution.ts",
    before: `export const formatted${kind} = adapter${kind === "Query" ? "Query" : "Events"}.pipe(${kind === "Query" ? "Effect" : "Stream"}.map((result) => result.errors?.[0]?.message),)`,
    after: `export const formatted${kind} = adapter${kind === "Query" ? "Query" : "Events"}.pipe(${kind === "Query" ? "Effect" : "Stream"}.map((result) => result.errors?.[0]?.originalError),)`,
    message: /Property 'originalError' does not exist/,
  })),
  ...["first", "last", "before", "after"].map((key): NegativeCase => ({
    name: `connection args cannot redeclare ${key}`,
    file: "src/api/graphql/query/search.ts",
    before: "args: { terms: Schema.Array(Schema.String) }",
    after: `args: { terms: Schema.Array(Schema.String), ${key}: Schema.String }`,
    message:
      /Reserved pagination argument; remove it from args and use the built-in connection arguments/,
  })),
  {
    name: "abstract connection list args preserve element type",
    file: "src/api/graphql/query/search.ts",
    before: "terms.length === 0",
    after: "terms[0].toFixed() === 0",
    message: /Property 'toFixed' does not exist on type 'string'/,
  },
  {
    name: "abstract edge validates node model",
    file: "src/api/graphql/query/search.ts",
    before: "({ node, cursor })",
    after: "({ node: 123, cursor })",
    message: /number.*List.*Workspace/,
  },
  {
    name: "public wire codes require public error declarations",
    file: "src/api/graphql.ts",
    before: "public: [ItemNotFoundError, PaginationError],",
    after: 'publicCodes: { ItemNotFoundError: "NOT_FOUND_ERROR" },',
    message: /not assignable to type 'undefined'/,
  },
  {
    name: "builder execution rejects another schema context",
    file: "tests/fixtures/execution.ts",
    before:
      'tenant.execute(schema, { source: "{value}" }, { tenant: "tenant" })',
    after: 'user.execute(schema, { source: "{value}" }, { user: "user" })',
    message: /TenantContext.*UserContext|tenant|user/,
  },
  {
    name: "builder subscription rejects another schema context",
    file: "tests/fixtures/execution.ts",
    before:
      'tenant.subscribe(schema, { source: "subscription {event}" }, { tenant: "tenant" })',
    after:
      'user.subscribe(schema, { source: "subscription {event}" }, { user: "user" })',
    message: /TenantContext.*UserContext|tenant|user/,
  },
  {
    name: "subscription authorization receives the emitted value",
    file: "tests/fixtures/execution.ts",
    before:
      "authorize: (_, __, ctx) => Effect.map(Service, () => ctx.tenant.length > 0)",
    after:
      "authorize: (value, __, ctx) => Effect.map(Service, () => value.missing && ctx.tenant.length > 0)",
    message: /Property 'missing' does not exist on type 'string'/,
  },
  {
    name: "public wire codes must name a declared public error",
    file: "src/api/graphql.ts",
    before: "public: [ItemNotFoundError, PaginationError],",
    after:
      'public: [ItemNotFoundError, PaginationError], publicCodes: { Undeclared: "PUBLIC" },',
    message: /Undeclared.*does not exist/,
  },
  {
    name: "execution requires the schema context",
    file: "tests/fixtures/execution.ts",
    before:
      'export const runQuery = execute(schema, { source: "{value}" }, { tenant: "tenant" })',
    after: 'export const runQuery = execute(schema, { source: "{value}" })',
    message: /Expected 3 arguments/,
  },
  {
    name: "execution cannot widen the schema context",
    file: "tests/fixtures/execution.ts",
    before:
      'export const runQuery = execute(schema, { source: "{value}" }, { tenant: "tenant" })',
    after:
      'export const runQuery = execute(schema, { source: "{value}" }, { user: "user" })',
    message: /tenant|UserContext|user/,
  },
  {
    name: "subscription requires the schema context",
    file: "tests/fixtures/execution.ts",
    before:
      'export const runEvents = subscribe(schema, { source: "subscription {event}" }, { tenant: "tenant" })',
    after:
      'export const runEvents = subscribe(schema, { source: "subscription {event}" })',
    message: /Expected 3 arguments/,
  },
  {
    name: "subscription cannot widen the schema context",
    file: "tests/fixtures/execution.ts",
    before:
      'export const runEvents = subscribe(schema, { source: "subscription {event}" }, { tenant: "tenant" })',
    after:
      'export const runEvents = subscribe(schema, { source: "subscription {event}" }, { user: "user" })',
    message: /tenant|user/,
  },
  {
    name: "subscription definition retains its context at assembly",
    file: "tests/fixtures/execution.ts",
    before: "tenant.schema({ query, subscription })",
    after: "user.schema({ query, subscription })",
    message: /Definition requires a different request context; use definitions/,
  },
  {
    name: "subscription field retains its context at composition",
    file: "tests/fixtures/execution.ts",
    before: "tenant.subscriptionField(",
    after: "user.subscriptionField(",
    message: /tenant|UserContext/,
  },
  {
    name: "executor query requires its schema context",
    file: "tests/fixtures/execution.ts",
    before: 'context: { tenant: "tenant" } })\nexport const adapterEvents',
    after: 'context: { user: "user" } })\nexport const adapterEvents',
    message: /tenant|user/,
  },
  {
    name: "executor subscription requires its schema context",
    file: "tests/fixtures/execution.ts",
    before: 'context: { tenant: "tenant" } })\nexport const snapshotQuery',
    after: 'context: { user: "user" } })\nexport const snapshotQuery',
    message: /tenant|user/,
  },
  {
    name: "testing query requires its schema context",
    file: "tests/fixtures/execution.ts",
    before:
      'Testing.execute(schema, { source: "{value}" }, { tenant: "tenant" })',
    after: 'Testing.execute(schema, { source: "{value}" })',
    message: /Expected 3 arguments/,
  },
  {
    name: "testing subscription requires its schema context",
    file: "tests/fixtures/execution.ts",
    before:
      'Testing.subscribe(schema, { source: "subscription {event}" }, { tenant: "tenant" })',
    after: 'Testing.subscribe(schema, { source: "subscription {event}" })',
    message: /Expected 3 arguments/,
  },
  {
    name: "subscription authorization services reach execution",
    file: "tests/fixtures/execution.ts",
    before:
      'runQuery.pipe(Effect.provideService(Service, { value: "service" }))',
    after: "runQuery",
    message: /Service/,
  },
  {
    name: "subscription authorization rejects promises",
    file: "tests/fixtures/execution.ts",
    before:
      "authorize: (_, __, ctx) => Effect.map(Service, () => ctx.tenant.length > 0)",
    after: "authorize: (_, __, ctx) => Promise.resolve(ctx.tenant.length > 0)",
    message: /Promise.*boolean/,
  },
  {
    name: "resolvers remain value or Effect",
    file: "tests/fixtures/execution.ts",
    before: "(_, __, ctx) => ctx.tenant",
    after: "async (_, __, ctx) => ctx.tenant",
    message: /Promise.*string/,
  },
  {
    name: "methods cannot default to scalars",
    file: list,
    before: "createdBy: GraphQL.string()",
    after: "cursor: GraphQL.string()",
    message: /model has/,
  },
  {
    name: "ID argument wrapper preserves wrong brand",
    file: update,
    before: "GraphQL.id(ListId)",
    after: "GraphQL.id(WorkspaceId)",
    prefix: 'import { WorkspaceId } from "../../../../store/ids";\n',
    message: /WorkspaceId.*ListId|ListId.*WorkspaceId/,
  },
  field(
    "enum literal sets must match",
    'status: GraphQL.field("Status")',
    'status: GraphQL.field("OtherStatus")',
    /model has/,
  ),
  {
    name: "missing model key",
    file: list,
    before: "starred: GraphQL.nullable.boolean()",
    after: "starrred: GraphQL.nullable.boolean()",
    message: /starrred: missing source property/,
  },
  {
    name: "string versus Int",
    file: list,
    before: "createdBy: GraphQL.string()",
    after: "createdBy: GraphQL.int()",
    message: /model has/,
  },
  {
    name: "optional boolean versus non-null",
    file: list,
    before: "starred: GraphQL.nullable.boolean()",
    after: "starred: GraphQL.boolean()",
    message: /model has/,
  },
  {
    name: "optional string versus non-null",
    file: fixture,
    before: "optional: GraphQL.nullable.string()",
    after: "optional: GraphQL.string()",
    message: /model has/,
  },
  {
    name: "wrong ID brand",
    file: update,
    before: "List.load({ id })",
    after: "Workspace.load({ id })",
    prefix: 'import { Workspace } from "../../../../store/models";\n',
    message: /WorkspaceId.*ListId|ListId.*WorkspaceId/,
  },
  {
    name: "missing layer",
    file: "src/api/handler.ts",
    before: "ManagedRuntime.make(DynamoDBMemory)",
    after: "ManagedRuntime.make(Layer.empty)",
    prefix: 'import { Layer } from "effect";\n',
    root: true,
    message: /DynamoDB/,
  },
  {
    name: "missing request context",
    file: "src/api/handler.ts",
    before: "GraphQL.execute(schema, request, context)",
    after: "GraphQL.execute(schema, request)",
    root: true,
    message: /Expected 3 arguments/,
  },
  {
    name: "unknown context field",
    file: "src/api/graphql/list.ts",
    before: "(list, _, { identity }) => identity.hasWorkspaceScope",
    after: "(list, _, { identiti }) => identiti.hasWorkspaceScope",
    message: /identiti/,
  },
  {
    name: "wrong Effect object result",
    file: list,
    before: "Workspace.load({ id: list.workspaceId })",
    after: "List.load({ id: list.id })",
    message: /Workspace/,
  },
  {
    name: "parent inferred as model",
    file: list,
    before: "Workspace.load({ id: list.workspaceId })",
    after: "Workspace.load({ id: list.wrongKey })",
    message: /Property 'wrongKey' does not exist on type 'List'/,
  },
  {
    name: "decoded input remains class typed",
    file: update,
    before: "list.update(input)",
    after: "list.update(input.wrongKey)",
    message: /Property 'wrongKey' does not exist on type 'ListUpdateInput'/,
  },
  {
    name: "typed error must be declared",
    file: update,
    before: "errors: [NotFoundError, UserInputError]",
    after: "errors: [UserInputError]",
    message: /NotFoundError/,
  },
  {
    name: "wrong mutation edge",
    file: update,
    before: "return { cursor: updated.cursor(), node: updated };",
    after: "return updated;",
    message: /Edge<List>/,
  },
  {
    name: "wrong connection page",
    file: workspace,
    before: "List.paginate(args, { workspaceId: workspace.id })",
    after: "Workspace.load({ id: workspace.id })",
    message: /Connection<List>/,
  },
  {
    name: "misspelled named enum",
    file: list,
    before: 'GraphQL.field("ListStatus",',
    after: 'GraphQL.field("ListStatu",',
    message: /ListStatu/,
  },
  {
    name: "misspelled object ref",
    file: list,
    before: 'GraphQL.nullable("Workspace",',
    after: 'GraphQL.nullable("Workspaec",',
    message: /Workspaec/,
  },
  {
    name: "misspelled derived edge",
    file: update,
    before: 'GraphQL.field("ListEdge",',
    after: 'GraphQL.field("ListEgde",',
    message: /ListEgde/,
  },
  {
    name: "misspelled connection node",
    file: workspace,
    before: 'GraphQL.connection("List",',
    after: 'GraphQL.connection("Lsit",',
    message: /Lsit/,
  },
  {
    name: "wrong authorize result",
    file: query,
    before: 'identity.hasWorkspaceScope(id, "workspace:read")',
    after: '"yes"',
    message: /boolean/,
  },
  {
    name: "wrong default authorization result",
    file: list,
    before: "createdBy: GraphQL.string()",
    after: 'createdBy: GraphQL.string({ authorize: () => "yes" })',
    message: /boolean/,
  },
  {
    name: "interface ref checked locally",
    file: list,
    before: 'implements: ["Node"]',
    after: 'implements: ["Ndoe"]',
    message: /Ndoe/,
  },
  {
    name: "malformed field rejected locally",
    file: workspace,
    before: "id: GraphQL.id()",
    after: 'id: "bad"',
    message: /not assignable/,
  },
  field(
    "default ID rejects number",
    "integer: GraphQL.int()",
    'integer: GraphQL.field("ID")',
    /model has/,
  ),
  field(
    "default String rejects boolean",
    "boolean: GraphQL.boolean()",
    'boolean: GraphQL.field("String")',
    /model has/,
  ),
  field(
    "default Float rejects string",
    'text: GraphQL.string({ description: "Text", deprecationReason: "Prefer computed", authorize: () => true })',
    'text: GraphQL.field("Float")',
    /model has/,
  ),
  field(
    "default Boolean rejects string",
    'text: GraphQL.string({ description: "Text", deprecationReason: "Prefer computed", authorize: () => true })',
    'text: GraphQL.field("Boolean")',
    /model has/,
  ),
  field(
    "default DateTime rejects number",
    "integer: GraphQL.int()",
    'integer: GraphQL.field("DateTime")',
    /model has/,
  ),
  field(
    "nullable DateTime rejects number",
    "integer: GraphQL.int()",
    "integer: GraphQL.nullable.dateTime()",
    /model has/,
  ),
  field(
    "default custom scalar checks brand",
    'text: GraphQL.string({ description: "Text", deprecationReason: "Prefer computed", authorize: () => true })',
    'text: GraphQL.field("Token")',
    /model has/,
  ),
  field(
    "default enum checks literals",
    'text: GraphQL.string({ description: "Text", deprecationReason: "Prefer computed", authorize: () => true })',
    'text: GraphQL.field("Status")',
    /model has/,
  ),
  field(
    "default object checks model",
    'workspace: GraphQL.field("Workspace")',
    'workspace: GraphQL.field("List")',
    /model has/,
  ),
  field(
    "default list checks elements",
    'nodes: GraphQL.list("List")',
    'nodes: GraphQL.list("Workspace")',
    /model has/,
  ),
  field(
    "default connection checks model",
    'page: GraphQL.connection("List")',
    'page: GraphQL.connection("Workspace")',
    /model has/,
  ),
  field(
    "default edge checks model",
    'edge: GraphQL.field("ListEdge")',
    'edge: GraphQL.field("WorkspaceEdge")',
    /model has/,
  ),
  field(
    "default PageInfo checks shape",
    'workspace: GraphQL.field("Workspace")',
    'workspace: GraphQL.field("PageInfo")',
    /model has/,
  ),
  field(
    "null requires nullable",
    "nullish: GraphQL.nullable.string()",
    "nullish: GraphQL.string()",
    /model has/,
  ),
  field(
    "Option requires nullable",
    "option: GraphQL.nullable.string()",
    "option: GraphQL.string()",
    /model has/,
  ),
  field(
    "nullable still checks value",
    "optional: GraphQL.nullable.string()",
    "optional: GraphQL.nullable.boolean()",
    /model has/,
  ),
  field(
    "nullable object checks model",
    'maybeWorkspace: GraphQL.nullable("Workspace",',
    'maybeWorkspace: GraphQL.nullable("List",',
    /model has/,
  ),
  field(
    "nullable list checks elements",
    'maybeNodes: GraphQL.nullable(GraphQL.list("List"))',
    'maybeNodes: GraphQL.nullable(GraphQL.list("Workspace"))',
    /model has/,
  ),
  field(
    "nullable list elements checked",
    'nullableElements: GraphQL.list(GraphQL.nullable("String"))',
    'nullableElements: GraphQL.list("String")',
    /model has/,
  ),
  field(
    "enum cannot be connection node",
    'page: GraphQL.connection("List")',
    'page: GraphQL.connection("Status")',
    /Status/,
  ),
  field(
    "misspelled built-in scalar",
    'custom: GraphQL.field("Token")',
    'custom: GraphQL.field("Strng")',
    /Strng/,
  ),
  field(
    "misspelled custom scalar",
    'custom: GraphQL.field("Token")',
    'custom: GraphQL.field("Tokne")',
    /Tokne/,
  ),
  field(
    "misspelled list ref",
    'nodes: GraphQL.list("List")',
    'nodes: GraphQL.list("Lst")',
    /Lst/,
  ),
  field(
    "misspelled PageInfo",
    'pageInfo: GraphQL.field("PageInfo")',
    'pageInfo: GraphQL.field("PageInf")',
    /PageInf/,
  ),
  field(
    "misspelled derived connection",
    'page: GraphQL.connection("List")',
    'page: GraphQL.field("ListConnetion")',
    /ListConnetion/,
  ),
  field(
    "wrong plain scalar resolver",
    'computed: GraphQL.field("String", source => source.text)',
    'computed: GraphQL.field("Int", source => source.text)',
    /number/,
  ),
  field(
    "wrong enum resolver",
    'computed: GraphQL.field("String", source => source.text)',
    'computed: GraphQL.field("Status", source => source.text)',
    /ACTIVE/,
  ),
  field(
    "wrong custom scalar resolver",
    'computed: GraphQL.field("String", source => source.text)',
    'computed: GraphQL.field("Token", source => source.text)',
    /Token/,
  ),
  field(
    "wrong list resolver",
    'resolvedList: GraphQL.list("List", () => fixture.nodes)',
    'resolvedList: GraphQL.list("List", () => [fixture.workspace])',
    /List/,
  ),
  field(
    "wrong nodes resolver",
    "nodes: () => fixture.nodes, totalCount:",
    "nodes: () => [fixture.workspace], totalCount:",
    /List/,
  ),
  field(
    "wrong connection count",
    "totalCount: () => fixture.nodes.length",
    'totalCount: () => "one"',
    /number/,
  ),
  field(
    "undefined rejected by non-null resolver",
    'computed: GraphQL.field("String", source => source.text)',
    'computed: GraphQL.field("String", () => undefined)',
    /undefined/,
  ),
  field(
    "nullable Effect still checks result",
    'effectNullable: GraphQL.nullable("String", () => Effect.succeed(undefined))',
    'effectNullable: GraphQL.nullable("String", () => Effect.succeed(42))',
    /number/,
  ),
]

export const registryCases: readonly NegativeCase[] = [
  {
    name: "subscription field requires subscribe",
    file: "tests/fixtures/execution.ts",
    before: "subscribe: (_, __, ctx) => Stream.succeed(ctx.tenant)",
    after: "resolve: (_, __, ctx) => ctx.tenant",
    message: /~subscription.*incompatible.*false.*true/,
  },
  ...["query", "mutation"].map((operation): NegativeCase => ({
    name: `${operation} field rejects subscribe`,
    file: "tests/fixtures/execution.ts",
    before: "tenant.subscriptionField(",
    after: `tenant.${operation}Field(`,
    message: /~subscription.*incompatible.*true.*false/,
  })),
  ...[
    ["object", 'name: G.string({ deprecationReason: "Use label instead." })'],
    ["interface", "id: G.id()"],
  ].map(([kind, before]): NegativeCase => ({
    name: `${kind} field rejects subscribe`,
    file: "tests/fixtures/type-contracts.ts",
    before: before!,
    after:
      'event: G.field("String", { subscribe: () => Stream.succeed("value") })',
    prefix: 'import { Stream } from "effect";\n',
    message: /~subscription.*incompatible.*true.*false/,
  })),

  {
    name: "4a missing Workspace registration errors at reference",
    file: workspace,
    before:
      'declare module "../graphql" { interface Types extends Register<typeof Workspace> {} }',
    after: "// Registration forgotten",
    expectedFile: list,
    lineAnchor: 'workspace: GraphQL.nullable("Workspace",',
    message: /Argument of type '"Workspace"'/,
  },
  ...[
    [
      workspace,
      'declare module "../graphql" { interface Types extends Register<typeof Workspace> {} }',
      "GraphQL.object(Workspace,",
      "Workspace",
    ],
    [
      "src/api/graphql/list-status.ts",
      'declare module "../graphql" { interface Types extends Register<typeof ListStatus, "ListStatus"> {} }',
      'GraphQL.enum("ListStatus",',
      "ListStatus",
    ],
    [
      "src/api/graphql/slug.ts",
      'declare module "../graphql" { interface Types extends Register<typeof Slug, "Slug"> {} }',
      'GraphQL.scalar("Slug",',
      "Slug",
    ],
    [
      "src/api/graphql/node.ts",
      'declare module "../graphql" { interface Types extends Register<typeof Node, "Node", "interface"> {} }',
      'GraphQL.interface("Node",',
      "Node",
    ],
    [
      "src/api/graphql/favorite-target.ts",
      'declare module "../graphql" { interface Types extends Register<typeof FavoriteTarget, "FavoriteTarget"> {} }',
      'GraphQL.union("FavoriteTarget",',
      "FavoriteTarget",
    ],
  ].map(([file, before, lineAnchor, name]): NegativeCase => ({
    name: `4b unregistered ${name} definition`,
    file: file!,
    before: before!,
    after: "// Registration forgotten",
    lineAnchor: lineAnchor!,
    message: new RegExp(`Register.*${name}`),
  })),
  {
    name: "4b wrong registered object model",
    file: workspace,
    before: "Register<typeof Workspace>",
    after: 'Register<typeof List, "Workspace">',
    lineAnchor: "GraphQL.object(Workspace,",
    message: /Registration model mismatch/,
  },
  {
    name: "4b wrong registered enum model",
    file: "src/api/graphql/list-status.ts",
    before: 'Register<typeof ListStatus, "ListStatus">',
    after: 'Register<{Type:"DRAFT"}, "ListStatus", "enum">',
    lineAnchor: 'GraphQL.enum("ListStatus",',
    message: /Registration model mismatch/,
  },
  {
    name: "4b wrong registered scalar model",
    file: "src/api/graphql/slug.ts",
    before: 'Register<typeof Slug, "Slug">',
    after: 'Register<{Type:number}, "Slug", "scalar">',
    lineAnchor: 'GraphQL.scalar("Slug",',
    message: /Registration model mismatch/,
  },
  {
    name: "4b wrong registered interface model",
    file: "src/api/graphql/node.ts",
    before: 'Register<typeof Node, "Node", "interface">',
    after: 'Register<{Type:{id:number}}, "Node", "interface">',
    lineAnchor: 'GraphQL.interface("Node",',
    message: /Registration model mismatch/,
  },
  {
    name: "4b wrong registered union model",
    file: "src/api/graphql/favorite-target.ts",
    before: 'Register<typeof FavoriteTarget, "FavoriteTarget">',
    after: 'Register<typeof List, "FavoriteTarget", "union">',
    lineAnchor: 'GraphQL.union("FavoriteTarget",',
    message: /Registration model mismatch/,
  },
  {
    name: "4c registered type without definition",
    file: "src/api/graphql.ts",
    before: "export interface Types {}",
    after: "export interface Types extends importNever {}",
    prefix:
      'import type { Register as R } from "../builder";\ntype importNever = R<{Type:{id:string}}, "Ghost", "object">;\n',
    root: true,
    message: /Missing definition: Ghost/,
  },
  {
    name: "4c definition omitted from barrel",
    file: "src/api/graphql/index.ts",
    before: 'export * from "./workspace";',
    after: "// Workspace definition omitted",
    root: true,
    message: /Missing definition: Workspace/,
  },
  {
    name: "4d conflicting registrations in two files",
    file: list,
    before: "interface Types extends Register<typeof List> {}",
    after:
      'interface Types extends Register<typeof List>, Register<typeof List, "Workspace"> {}',
    expectedFile: "src/api/graphql.ts",
    message:
      /cannot simultaneously extend.*Register<typeof List, "Workspace">.*Register<typeof Workspace>|Named property 'Workspace'.*not identical/,
  },
  {
    name: "4e enum cannot be connection node (app)",
    file: workspace,
    before: 'GraphQL.connection("List",',
    after: 'GraphQL.connection("ListStatus",',
    message: /ListStatus/,
  },
  {
    name: "4e object cannot be implemented",
    file: list,
    before: 'implements: ["Node"]',
    after: 'implements: ["List"]',
    message: /List/,
  },
  {
    name: "4e enum cannot be a union member",
    file: "src/api/graphql/favorite-target.ts",
    before: "Schema.Union([List, Workspace])",
    after: "Schema.Union([ListStatus, Workspace])",
    prefix: 'import { ListStatus } from "../../store/models";\n',
    lineAnchor: 'GraphQL.union("FavoriteTarget",',
    message: /Union members must be tagged object models/,
  },
  {
    name: "4e enum registration cannot define an object",
    file: list,
    before: "GraphQL.object(List,",
    after: 'GraphQL.object("ListStatus", List,',
    message: /Registration kind mismatch/,
  },
  {
    name: "4e enum has no derived edge",
    file: list,
    before: 'GraphQL.field("ListStatus",',
    after: 'GraphQL.field("ListStatusEdge",',
    message: /ListStatusEdge/,
  },
  {
    name: "union members must be registered objects",
    file: workspace,
    before: "Register<typeof Workspace>",
    after: 'Register<typeof Workspace, "Workspace", "interface">',
    expectedFile: "src/api/graphql/favorite-target.ts",
    lineAnchor: 'GraphQL.union("FavoriteTarget",',
    message: /Registration kind mismatch.*Workspace/,
  },
  {
    name: "wrong union resolver result",
    file: "src/api/graphql/query/examples.ts",
    before: "() => Effect.all([",
    after: '() => Effect.all([Effect.succeed({id:"bad"}),',
    message: /List|Workspace/,
  },
  {
    name: "subscription result checked",
    file: "src/api/graphql/subscription/list-changed.ts",
    before: "Stream.fromEffect(List.load({ id }))",
    after: "Stream.succeed(42)",
    message: /number/,
  },
  {
    name: "subscription args preserve ID brand",
    file: "src/api/graphql/subscription/list-changed.ts",
    before: "List.load({ id })",
    after: 'List.load({ id: "unbranded" })',
    message: /ListId/,
  },
  {
    name: "boolean authorize parent inferred",
    file: list,
    before: "createdBy: GraphQL.string()",
    after: "createdBy: GraphQL.string({authorize: list => list.wrongKey})",
    message: /Property 'wrongKey' does not exist on type 'List'/,
  },
]

const typeContract = (
  name: string,
  before: string,
  after: string,
  message: RegExp,
): NegativeCase => ({
  name,
  file: "tests/fixtures/type-contracts.ts",
  before,
  after,
  message,
})

export const definitionCases: readonly NegativeCase[] = [
  typeContract(
    "mapped enum requires every internal value",
    'WORKSPACE_WRITE: { value: "workspace:write", deprecationReason: "Use a scoped key." },',
    "",
    /Missing enum values.*map every model literal/,
  ),
  typeContract(
    "mapped enum rejects unknown internal values",
    'USER_READ: { value: "user:read", description: "Read the user." }',
    'USER_READ: { value: "user:unknown", description: "Read the user." }',
    /Use a literal from the registered enum model/,
  ),
  typeContract(
    "enum shorthand rejects wrong literal",
    'G.queryField("status", G.field("Status", () => "ACTIVE"))',
    'G.queryField("status", G.field("Status", () => "WRONG"))',
    /ACTIVE.*ARCHIVED/,
  ),
  typeContract(
    "interface resolver returns registered objects",
    'resolveType: (value) => value.id.startsWith("item") ? "Item" : "Alternative"',
    'resolveType: (value) => value.id.startsWith("item") ? "Status" : "Alternative"',
    /Item.*Alternative.*Missing/,
  ),
  typeContract(
    "interface resolver parent is typed",
    'value.id.startsWith("item")',
    'value.missing.startsWith("item")',
    /Property 'missing' does not exist/,
  ),
  typeContract(
    "inherited default field needs compatible source",
    'id: Schema.String, kind: Schema.Literal("alternative")',
    'id: Schema.Number, kind: Schema.Literal("alternative")',
    /Alternative.id.*cannot inherit Identity field; supply a compatible source property or override with a resolver/,
  ),
  typeContract(
    "inherited resolver needs compatible source",
    "export const Identity = Schema.Struct({ id: Schema.String })",
    "export const Identity = Schema.Struct({ id: Schema.String, tenant: Schema.String })",
    /cannot inherit Identity field; supply a compatible source property or override with a resolver/,
  ),
  typeContract(
    "named union members match their registered models",
    'members: ["Item", "Alternative"]',
    'members: ["Alternative", "Item"]',
    /Registration model mismatch.*register the definition's model/,
  ),
  typeContract(
    "named union members must be registered",
    'members: ["Item", "Alternative"]',
    'members: ["Item", "Unknown"]',
    /Register.*Unknown.*add declare module/,
  ),
  typeContract(
    "named union members must be objects",
    'members: ["Item", "Alternative"]',
    'members: ["Item", "Identity"]',
    /Registration kind mismatch.*expected object/,
  ),
  typeContract(
    "named union requires one name per model",
    'members: ["Item", "Alternative"]',
    'members: ["Item"]',
    /Provide one registered object name per union model, in the same order/,
  ),
  typeContract(
    "named union resolver stays within members",
    'value.kind === "item" ? "Item" : "Alternative"',
    'value.kind === "item" ? "Item" : "Missing"',
    /Item.*Alternative/,
  ),
  typeContract(
    "nullable typed results reject scalar successes",
    'G.mutationField("empty", G.nullable("Item", { errors: [Missing], resolve: () => null }))',
    'G.mutationField("empty", G.nullable("String", { errors: [Missing], resolve: () => null }))',
    /Result unions need an object type or object union; use a registered object or union/,
  ),
  typeContract(
    "no-error result wrapper rejects scalar successes",
    'G.queryField("status", G.field("Status", () => "ACTIVE"))',
    'G.queryField("status", G.field("Status", { result: {}, resolve: () => "ACTIVE" as const }))',
    /Result unions need an object type or object union; use a registered object or union/,
  ),
]
