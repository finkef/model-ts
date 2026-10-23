import { Model } from "@model-ts/core"
import { DynamoDB } from "@model-ts/dynamodb"
import { Sandbox } from "@model-ts/dynamodb/Sandbox"
import { Effect, Layer, Schema } from "effect"
import { ListId, OrganizationId, WorkspaceId } from "./ids.ts"
export { DynamoDB } from "@model-ts/dynamodb"

export const ListStatus = Schema.Literals(["ACTIVE", "ARCHIVED"])
export class Workspace extends Model.Class<Workspace>()(
  "Workspace",
  DynamoDB.provider,
  {
    id: WorkspaceId,
    organizationId: OrganizationId,
    name: Schema.String,
    createdAt: Schema.DateFromString,
  },
) {
  get PK() {
    return `WORKSPACE#${this.id}`
  }
  get SK() {
    return "WORKSPACE"
  }
  static key(id: WorkspaceId) {
    return { PK: `WORKSPACE#${id}`, SK: "WORKSPACE" }
  }
}
export class List extends Model.Class<List>()("List", DynamoDB.provider, {
  id: ListId,
  organizationId: OrganizationId,
  workspaceId: WorkspaceId,
  createdBy: Schema.String,
  name: Schema.String,
  starred: Schema.optionalKey(Schema.Boolean),
  status: ListStatus,
  createdAt: Schema.DateFromString,
  updatedAt: Schema.optionalKey(Schema.DateFromString),
}) {
  get PK() {
    return `LIST#${this.id}`
  }
  get SK() {
    return "LIST"
  }
  get GSI1PK() {
    return `WORKSPACE#${this.workspaceId}`
  }
  get GSI1SK() {
    return this.PK
  }
  static key(id: ListId) {
    return { PK: `LIST#${id}`, SK: "LIST" }
  }
}
const seed = Effect.gen(function* () {
  const sandbox = yield* Sandbox
  yield* sandbox.seed(
    new Workspace({
      id: WorkspaceId.make("w1"),
      organizationId: OrganizationId.make("o1"),
      name: "Product",
      createdAt: new Date("2026-08-01T09:00:00Z"),
    }),
    new List({
      id: ListId.make("l1"),
      organizationId: OrganizationId.make("o1"),
      workspaceId: WorkspaceId.make("w1"),
      createdBy: "u1",
      name: "Launch",
      status: "ACTIVE",
      createdAt: new Date("2026-09-01T09:00:00Z"),
    }),
  )
})
// Seed the same service instance that the request's resolver effects consume.
export const DynamoDBMemory = Layer.effect(
  DynamoDB,
  seed.pipe(Effect.andThen(DynamoDB)),
).pipe(Layer.provide(Sandbox.layerMemory()))
