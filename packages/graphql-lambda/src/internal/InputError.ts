/** Marks application input callbacks without conflating them with graphql-js diagnostics. */
export class InputError extends Error {
  constructor(override readonly cause: unknown) {
    super("GraphQL input callback failed", { cause })
  }
}
