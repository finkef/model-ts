import { expect, layer, it } from "@effect/vitest"
import { Sandbox } from "@model-ts/dynamodb/Sandbox"
import { Context, Effect, Stream } from "effect"
import * as Testing from "../src/Testing.ts"
import { graphql } from "../src/Testing.ts"
import { identityFor, type Identity } from "./app/api/context.ts"
import { schema } from "./app/api/schema.ts"
import { ListId, OrganizationId, WorkspaceId } from "./app/store/ids.ts"
import { List, Workspace } from "./app/store/models.ts"
import { testTable } from "./utils.ts"

const context = {
  identity: identityFor(OrganizationId.make("org"), [
    "workspace:read",
    "workspace:write",
  ]),
  requestId: "test",
}
const workspace = new Workspace({
  id: WorkspaceId.make("workspace"),
  organizationId: OrganizationId.make("org"),
  name: "Test workspace",
  createdAt: new Date("2026-10-23T00:00:00Z"),
})
const request = { source: '{workspace(id:"workspace"){id name}}' }
layer(testTable())((it) => {
  it.effect(
    "uses the file's layer and returns plain snapshot-friendly data",
    () =>
      Effect.gen(function* () {
        const sandbox = yield* Sandbox
        yield* sandbox.seed(workspace)
        expect(yield* Testing.execute(schema, request, context))
          .toMatchInlineSnapshot(`
      {
        "data": {
          "workspace": {
            "id": "workspace",
            "name": "Test workspace",
          },
        },
      }
    `)
      }),
  )
})

it.effect(
  "can provide a fresh layer per test and snapshots errors without stacks",
  () =>
    Effect.gen(function* () {
      const result = yield* Testing.execute(schema, request, {
        ...context,
        identity: identityFor(null, []),
      })
      expect(result.errors?.[0]).toEqual({
        message: 'You don\'t have permission to access field "workspace"',
        locations: [{ line: 1, column: 2 }],
        path: ["workspace"],
        extensions: { code: "FORBIDDEN" },
      })
      expect(Object.getPrototypeOf(result.errors?.[0])).toBe(Object.prototype)
    }).pipe(Effect.provide(testTable())),
)

it.effect(
  "a GraphQL mutation changes the same table that Sandbox snapshots",
  () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(
        workspace,
        new List({
          id: ListId.make("list"),
          organizationId: workspace.organizationId,
          workspaceId: workspace.id,
          createdBy: "user",
          name: "Original",
          status: "ACTIVE",
          createdAt: new Date("2026-10-21T00:00:00Z"),
        }),
      )
      const before = yield* sandbox.snapshot
      const result = yield* Testing.execute(
        schema,
        {
          source: `mutation($id: ID!, $input: ListUpdateInput!) {
        listUpdate(id: $id, input: $input) {
          __typename
          ...on ListEdge { node { id name starred } }
        }
      }`,
          variableValues: {
            id: "list",
            input: { name: "Renamed", starred: true },
          },
        },
        context,
      )
      expect(result).toEqual({
        data: {
          listUpdate: {
            __typename: "ListEdge",
            node: { id: "list", name: "Renamed", starred: true },
          },
        },
      })
      expect(yield* sandbox.diff(before)).toMatchInlineSnapshot(`
        "[LIST#list / LIST]
            PK: "LIST#list"
            SK: "LIST"
            GSI1PK: "WORKSPACE#workspace"
            GSI1SK: "LIST#list"
        -   _docVersion: 0
        +   _docVersion: 1
            _tag: "List"
            createdAt: "2026-10-21T00:00:00.000Z"
            createdBy: "user"
            id: "list"
        -   name: "Original"
        +   name: "Renamed"
            organizationId: "org"
        +   starred: true
            status: "ACTIVE"
        +   updatedAt: "2026-10-22T08:00:00.000Z"
            workspaceId: "workspace""
      `)
    }).pipe(Effect.provide(testTable())),
)

const anonymous = identityFor(null, [])
const app = Testing.client(schema, {
  context: (options: { identity?: Identity }) => ({
    requestId: "test",
    identity: options.identity ?? anonymous,
  }),
})
const LIST_UPDATE = graphql`
  mutation ListUpdate($id: ID!, $input: ListUpdateInput!) {
    listUpdate(id: $id, input: $input) {
      __typename

      ... on ListEdge {
        node {
          id
          name
          starred
        }
      }

      ... on NotFoundError {
        message
      }

      ... on UserInputError {
        field
        message
      }
    }
  }
`
const list = new List({
  id: ListId.make("list"),
  organizationId: workspace.organizationId,
  workspaceId: workspace.id,
  createdBy: "user",
  name: "Original",
  status: "ACTIVE",
  createdAt: new Date("2026-10-21T00:00:00Z"),
})

it.effect("client requests fill the default context per call", () =>
  Effect.gen(function* () {
    const sandbox = yield* Sandbox
    yield* sandbox.seed(workspace, list)
    const update = (
      identity: Identity | undefined,
      id: string,
      input: { readonly name?: string; readonly starred?: boolean },
    ) =>
      app.request(LIST_UPDATE, {
        ...(identity ? { identity } : {}),
        variables: { id, input },
      })
    expect({
      anonymous: yield* update(undefined, "list", { name: "Renamed" }),
      missing: yield* update(context.identity, "other", { name: "Renamed" }),
      blank: yield* update(context.identity, "list", { name: " " }),
      renamed: yield* update(context.identity, "list", {
        name: "Renamed",
        starred: true,
      }),
    }).toMatchInlineSnapshot(`
      {
        "anonymous": {
          "data": null,
          "errors": [
            {
              "extensions": {
                "code": "FORBIDDEN",
              },
              "locations": [
                {
                  "column": 53,
                  "line": 1,
                },
              ],
              "message": "You don't have permission to access field "listUpdate"",
              "path": [
                "listUpdate",
              ],
            },
          ],
        },
        "blank": {
          "data": {
            "listUpdate": {
              "__typename": "UserInputError",
              "field": "name",
              "message": "Name must not be blank",
            },
          },
        },
        "missing": {
          "data": {
            "listUpdate": {
              "__typename": "NotFoundError",
              "message": "List not found",
            },
          },
        },
        "renamed": {
          "data": {
            "listUpdate": {
              "__typename": "ListEdge",
              "node": {
                "id": "list",
                "name": "Renamed",
                "starred": true,
              },
            },
          },
        },
      }
    `)
  }).pipe(Effect.provide(testTable())),
)

it.effect("client subscriptions use the same context factory", () =>
  Effect.gen(function* () {
    const sandbox = yield* Sandbox
    yield* sandbox.seed(workspace, list)
    const source = graphql`
      subscription ListChanged($id: ID!) {
        listChanged(id: $id) {
          id
          name
        }
      }
    `
    const variables = { id: "list" }
    expect({
      member: yield* Stream.runCollect(
        app.subscribe(source, { identity: context.identity, variables }),
      ),
      anonymous: yield* Stream.runCollect(app.subscribe(source, { variables })),
    }).toMatchInlineSnapshot(`
      {
        "anonymous": [
          {
            "data": null,
            "errors": [
              {
                "extensions": {
                  "code": "FORBIDDEN",
                },
                "locations": [
                  {
                    "column": 35,
                    "line": 1,
                  },
                ],
                "message": "You don't have permission to access field "listChanged"",
                "path": [
                  "listChanged",
                ],
              },
            ],
          },
        ],
        "member": [
          {
            "data": {
              "listChanged": {
                "id": "list",
                "name": "Original",
              },
            },
          },
        ],
      }
    `)
  }).pipe(Effect.provide(testTable())),
)

class CurrentIdentity extends Context.Service<CurrentIdentity, Identity>()(
  "test/CurrentIdentity",
) {}

it.effect("an Effect context factory reads services for each request", () =>
  Effect.gen(function* () {
    const sandbox = yield* Sandbox
    yield* sandbox.seed(workspace)
    const services = Testing.client(schema, {
      context: () =>
        Effect.map(CurrentIdentity, (identity) => ({
          requestId: "test",
          identity,
        })),
    })
    const query = graphql`
      {
        workspace(id: "workspace") {
          name
        }
      }
    `
    expect(
      yield* services
        .request(query)
        .pipe(Effect.provideService(CurrentIdentity, context.identity)),
    ).toEqual({ data: { workspace: { name: "Test workspace" } } })
    expect(
      (yield* services
        .request(query)
        .pipe(Effect.provideService(CurrentIdentity, anonymous))).errors?.[0]
        ?.extensions,
    ).toEqual({ code: "FORBIDDEN" })
  }).pipe(Effect.provide(testTable())),
)

it.effect(
  "graphql tags give formatting-independent documents and locations",
  () =>
    Effect.gen(function* () {
      const name = "two  spaces"
      expect(graphql`
      query Named {
        workspace(id: "${name}") {
          id
        }
      }
    `).toBe('query Named{workspace(id:"two  spaces"){id}}')
      expect(graphql`{ workspace(id: "a\nb") { id } }`).toBe(
        '{workspace(id:"a\\nb"){id}}',
      )
      const compact = yield* app.request(graphql`
        {
          workspace(id: "w") {
            missing
          }
        }
      `)
      const spread = yield* app.request(graphql`
        {
          workspace(id: "w") {
            missing
          }
        }
      `)
      expect(spread).toEqual(compact)
      expect(compact.errors?.[0]?.locations).toEqual([{ line: 1, column: 20 }])
      // Lexical errors are left for execution to report with a location.
      const invalid = graphql`{ workspace(id: "unterminated) { id } }`
      expect(invalid).toBe('{ workspace(id: "unterminated) { id } }')
      expect((yield* app.request(invalid)).errors?.[0]?.message).toContain(
        "Unterminated string",
      )
    }).pipe(Effect.provide(testTable())),
)
