import type { OrganizationId, WorkspaceId } from "../store/ids.ts"

export type Scope = "workspace:read" | "workspace:write"

export interface Identity {
  readonly organizationId: OrganizationId | null
  readonly hasWorkspaceScope: (
    workspaceId: WorkspaceId,
    scope: Scope,
  ) => boolean
}

/** Request-level data every resolver receives as its third argument. */
export interface AppContext {
  readonly identity: Identity
  readonly requestId: string
}

export const identityFor = (
  organizationId: OrganizationId | null,
  scopes: ReadonlyArray<Scope>,
): Identity => ({
  organizationId,
  hasWorkspaceScope: (_, scope) => scopes.includes(scope),
})
