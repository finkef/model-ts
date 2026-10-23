import { Schema } from "effect"
export const Slug = Schema.String.check(
  Schema.isPattern(/^[a-z][a-z0-9-]*$/, {
    message: "Use lowercase letters, digits and hyphens",
  }),
).pipe(Schema.brand("Slug"))
