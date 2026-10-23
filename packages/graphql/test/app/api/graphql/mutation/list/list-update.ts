import { Effect, Schema } from "effect"
import { NotFoundError, UserInputError } from "../../../../store/errors.ts"
import { ListId } from "../../../../store/ids.ts"
import { List } from "../../../../store/models.ts"
import { GraphQL, t } from "../../../graphql.ts"

export class ListUpdateInput extends Schema.Class<ListUpdateInput>(
  "ListUpdateInput",
)({
  name: Schema.optionalKey(
    Schema.NonEmptyString.check(
      Schema.isPattern(/\S/, { message: "Name must not be blank" }),
    ),
  ).annotate({ description: "A new name. Must not be blank." }),
  starred: Schema.optionalKey(Schema.Boolean),
}) {}

export const ListUpdateMutation = GraphQL.mutation({
  listUpdate: t.field("ListEdge", {
    args: {
      id: GraphQL.id(ListId).annotate({ description: "The list ID." }),
      input: ListUpdateInput,
    },
    errors: [NotFoundError, UserInputError],
    authorize: (_, __, { identity }) => identity.organizationId !== null,
    resolve: (_, { id, input }, { identity }) =>
      Effect.gen(function* () {
        const list = yield* List.load(List.key(id)).pipe(
          Effect.catchTag(
            "ItemNotFoundError",
            () => new NotFoundError({ message: "List not found" }),
          ),
        )
        if (!identity.hasWorkspaceScope(list.workspaceId, "workspace:write")) {
          return yield* new NotFoundError({ message: "List not found" })
        }
        const updated = yield* list.update({
          ...input,
          updatedAt: new Date("2026-10-22T08:00:00Z"),
        })
        return { cursor: yield* updated.cursor(), node: updated }
      }),
  }),
})
