import type { Schema, Stream } from "effect"
import type { Topic } from "../Identifiers.ts"

/** Supplied to the context factory for this operation, never shared between requests. */
export interface Source {
  readonly subscribe: (
    topics: Topic | ReadonlyArray<Topic>,
  ) => AsyncIterableIterator<Schema.Json>
  /** Effect-native source for builders whose subscription resolvers return Streams. */
  readonly stream: (
    topics: Topic | ReadonlyArray<Topic>,
  ) => Stream.Stream<Schema.Json>
}

/** Strict-true filtering, with iterator cancellation forwarded on completion/error. */
export async function* withFilter<A>(
  source: AsyncIterable<A>,
  predicate: (payload: A) => boolean | Promise<boolean>,
): AsyncGenerator<A> {
  for await (const payload of source)
    if ((await predicate(payload)) === true) yield payload
}
