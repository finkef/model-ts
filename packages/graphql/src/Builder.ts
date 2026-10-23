import type { Effect, Stream } from "effect"
import { assemble } from "./Assembly.ts"
import {
  makeDefinitions,
  type DefinitionBuilder,
  type CheckedInterfaces,
} from "./Definitions.ts"
import { type ErrorPolicyOptions, type ErrorPolicy } from "./Errors.ts"
import {
  schemaRequirements,
  schemaErrorPolicy,
  type BuiltSchema,
  type ExecutionResult,
  type Request,
  type ContextArgument,
  subscribe,
  execute,
} from "./Execution.ts"
import { makeFields } from "./Fields.ts"
import { type Fields, type Requirements } from "./FieldTypes.ts"
import { type Complete, type CompatibleContexts } from "./Registry.ts"
import { type ErrorSchema, type ErrorsOf } from "./Results.ts"
import { id, input, defaulted, invalid } from "./SchemaTypes.ts"

export interface BuilderOptions {
  /** Largest "first"/"last" a connection accepts. Defaults to 100. */
  readonly maxPageSize?: number
  /** Size for nodes-based slicing when neither first nor last is supplied. Defaults to 20 (capped at maxPageSize). */
  readonly defaultPageSize?: number
}

export interface Builder<Types, G, C = {}> extends DefinitionBuilder<Types, C> {
  readonly fields: Fields<Types, G, C>
  input: typeof input
  id: typeof id
  default: typeof defaulted
  /** Errors any resolver may fail with, and how each reaches the response. */
  withErrorPolicy<
    const I extends readonly ErrorSchema[] = readonly [],
    const P extends readonly ErrorSchema[] = readonly [],
  >(
    policy: ErrorPolicyOptions<I, P>,
  ): Builder<Types, G | ErrorsOf<I> | ErrorsOf<P>, C>
  schema<const M extends Readonly<Record<string, unknown>>>(
    module: M &
      Complete<Types, M> &
      CompatibleContexts<M, C> &
      CheckedInterfaces<Types, M>,
  ): BuiltSchema<Requirements<M>, C>
  /** The request context is per request: it reaches every resolver of this execution and nothing else. */
  subscribe<R>(
    schema: BuiltSchema<R, C>,
    request: Request,
    ...context: ContextArgument<C>
  ): Stream.Stream<ExecutionResult, never, R>
  execute<R>(
    schema: BuiltSchema<R, C>,
    request: Request,
    ...context: ContextArgument<C>
  ): Effect.Effect<ExecutionResult, never, R>
}

export function createBuilder<Types, C = {}>(
  options: BuilderOptions = {},
): Builder<Types, never, C> {
  const max = options.maxPageSize ?? 100
  if (!Number.isSafeInteger(max) || max < 0)
    invalid("maxPageSize must be a non-negative safe integer")
  const size = options.defaultPageSize ?? Math.min(20, max)
  if (!Number.isSafeInteger(size) || size < 0 || size > max)
    invalid(
      "defaultPageSize must be a non-negative safe integer no larger than maxPageSize",
    )
  return make<Types, never, C>({ internal: [], public: [] }, max, size)
}

function make<Types, G, C>(
  policy: ErrorPolicy,
  maxPageSize: number,
  defaultPageSize: number,
): Builder<Types, G, C> {
  const fields = makeFields<Types, G, C>(policy, maxPageSize, defaultPageSize)
  return {
    fields,
    ...makeDefinitions<Types, C>(),
    input,
    id,
    default: defaulted,
    withErrorPolicy: (options) =>
      make(
        {
          internal: [...policy.internal, ...(options.internal ?? [])],
          public: [...policy.public, ...(options.public ?? [])],
          publicCodes: { ...policy.publicCodes, ...options.publicCodes },
        },
        maxPageSize,
        defaultPageSize,
      ),
    schema: (module) => ({
      schema: assemble(module),
      [schemaErrorPolicy]: policy,
      [schemaRequirements]: undefined,
    }),
    subscribe,
    execute,
  }
}
