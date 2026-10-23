import { Effect, type Tracer } from "effect"

/**
 * Wraps an operation in a span the way Effect's own modules do: without a
 * stack trace. A captured trace would only point into model-ts and costs an
 * `Error` per call.
 *
 * @internal
 */
export const span = (name: string, options?: Tracer.SpanOptionsNoTrace) =>
  Effect.withSpan(name, options, { captureStackTrace: false })
