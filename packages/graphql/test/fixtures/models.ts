import { Schema, Option } from "effect"
import { ListId, WorkspaceId } from ".././app/store/ids.ts"
export const Status = Schema.Literals(["ACTIVE", "ARCHIVED"])
export const Token = Schema.String.pipe(Schema.brand("Token"))
export class Workspace extends Schema.Class<Workspace>("WorkspaceModel")({
  id: WorkspaceId,
  title: Schema.String,
}) {}
export class List extends Schema.Class<List>("ListModel")({
  id: ListId,
  name: Schema.String,
}) {}
const Edge = Schema.Struct({ cursor: Schema.String, node: List })
const PageInfo = Schema.Struct({
  hasNextPage: Schema.Boolean,
  hasPreviousPage: Schema.Boolean,
  startCursor: Schema.NullOr(Schema.String),
  endCursor: Schema.NullOr(Schema.String),
})
export class Fixture extends Schema.Class<Fixture>("FixtureModel")({
  id: ListId,
  text: Schema.String,
  integer: Schema.Int,
  float: Schema.Number,
  boolean: Schema.Boolean,
  date: Schema.Date,
  json: Schema.Json,
  custom: Token,
  status: Status,
  workspace: Workspace,
  nodes: Schema.Array(List),
  edge: Edge,
  page: Schema.Struct({ edges: Schema.Array(Edge), pageInfo: PageInfo }),
  pageInfo: PageInfo,
  optional: Schema.optionalKey(Schema.String),
  nullish: Schema.NullOr(Schema.String),
  option: Schema.Option(Schema.String),
  maybeNodes: Schema.optionalKey(Schema.Array(List)),
  maybeWorkspace: Schema.optionalKey(Workspace),
  maybeStatus: Schema.optionalKey(Status),
  nullableElements: Schema.Array(Schema.Option(Schema.String)),
}) {}
export const list = new List({ id: ListId.make("l1"), name: "Example" })
export const workspace = new Workspace({
  id: WorkspaceId.make("w1"),
  title: "Workspace",
})
const pageInfo = {
  hasNextPage: false,
  hasPreviousPage: false,
  startCursor: null,
  endCursor: null,
}
export const fixture = new Fixture({
  id: list.id,
  text: "hello",
  integer: 1,
  float: 1.5,
  boolean: true,
  date: new Date("2026-10-22T00:00:00Z"),
  json: { x: 1 },
  custom: Token.make("token"),
  status: "ACTIVE",
  workspace,
  nodes: [list],
  edge: { cursor: "1", node: list },
  page: { edges: [{ cursor: "1", node: list }], pageInfo },
  pageInfo,
  nullish: null,
  option: Option.none(),
  nullableElements: [Option.some("yes"), Option.none()],
})
