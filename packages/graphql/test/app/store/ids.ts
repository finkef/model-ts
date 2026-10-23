import { Schema } from "effect"

// One strict ID type per model.
export const OrganizationId = Schema.String.pipe(Schema.brand("OrganizationId"))
export type OrganizationId = typeof OrganizationId.Type
export const WorkspaceId = Schema.String.pipe(Schema.brand("WorkspaceId"))
export type WorkspaceId = typeof WorkspaceId.Type
export const ListId = Schema.String.pipe(Schema.brand("ListId"))
export type ListId = typeof ListId.Type
