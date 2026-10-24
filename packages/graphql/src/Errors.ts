import { Cause, Context, Effect, Option, Schema } from "effect"
import { GraphQLError } from "graphql"
import {
  type ErrorSchema,
  type ErrorsOf,
  errorMember,
  errorName,
} from "./Results.ts"

export interface ErrorPolicyOptions<
  I extends readonly ErrorSchema[],
  P extends readonly ErrorSchema[],
> {
  /** Infrastructure failures are always masked. */
  readonly internal?: I
  /** Only declared fields of these errors may reach clients. */
  readonly public?: P
  /** Preserve an existing API's codes; unspecified tags use their own name. */
  readonly publicCodes?: [P[number]] extends [never]
    ? never
    : Partial<Record<ErrorsOf<NoInfer<P>>["_tag"], string>>
  /**
   * A fixed message for a public error. Clients get only this message and the
   * error's code: neither its own message nor any declared field is exposed.
   * Use it to expose errors whose fields are internal, such as a not-found
   * error that carries a storage key.
   */
  readonly publicMessages?: [P[number]] extends [never]
    ? never
    : Partial<Record<ErrorsOf<NoInfer<P>>["_tag"], string>>
}

export interface ErrorPolicy {
  readonly internal: readonly ErrorSchema[]
  readonly public: readonly ErrorSchema[]
  readonly publicCodes?: Readonly<Record<string, string>>
  readonly publicMessages?: Readonly<Record<string, string>>
}

export const defaultErrorPolicy: ErrorPolicy = { internal: [], public: [] }

export interface ErrorReport {
  /** Unmodified Effect cause, including defects and tracing annotations. */
  readonly cause: Cause.Cause<unknown>
  readonly coordinate: string
  readonly phase: "resolve" | "authorize" | "subscribe" | "execute"
}

/** Optional per-runtime telemetry. Capture dependencies when constructing its layer. */
export class ErrorReporter extends Context.Service<
  ErrorReporter,
  {
    readonly report: (event: ErrorReport) => Effect.Effect<void>
  }
>()("@model-ts/graphql/ErrorReporter") {}

// A GraphQLError from user code is not permission to disclose its message.
// Only errors created by our boundaries, or declared in a policy, are trusted.
const trusted = new WeakSet<GraphQLError>()
export const isBoundaryError = (error: unknown): boolean =>
  error instanceof GraphQLError && trusted.has(error)

export const fieldError = (
  message: string,
  extensions: Readonly<Record<string, unknown>>,
  cause?: Cause.Cause<unknown>,
): GraphQLError => {
  const error = new GraphQLError(message, {
    ...(cause ? { originalError: new Error(message, { cause }) } : {}),
    extensions,
  })
  trusted.add(error)
  return error
}

export const forbidden = (field: string): GraphQLError =>
  fieldError(`You don't have permission to access field "${field}"`, {
    code: "FORBIDDEN",
  })

// Wrappers are not Errors (graphql-js treats Error values as failures). Keep the
// class instance private while the enumerable projection exposes only declared fields.
const resultInstances = new WeakMap<object, object>()
export const resultSource = (value: unknown): unknown =>
  typeof value === "object" && value !== null
    ? (resultInstances.get(value) ?? value)
    : value

/** Maps declared typed failures; undefined means the failure must be masked. */
function visibleFailure(
  error: unknown,
  members: readonly ErrorSchema[],
  policy: ErrorPolicy,
): unknown {
  if (isBoundaryError(error)) return error
  if (typeof error !== "object" || error === null) return undefined
  const matches = (schema: ErrorSchema) => Schema.is(schema)(error)
  if (policy.internal.some(matches)) return undefined
  const member = members.find(matches)
  if (member) {
    const wrapper = {
      ...declaredFields(member, error),
      [errorMember]: errorName(member),
    }
    resultInstances.set(wrapper, error)
    return wrapper
  }
  const visible = policy.public.find(matches)
  if (visible) {
    const tag = errorName(visible)
    const code = entry(policy.publicCodes, tag) ?? tag
    const fixed = entry(policy.publicMessages, tag)
    if (fixed !== undefined) return fieldError(fixed, { code })
    const { _tag, message, ...extensions } = declaredFields(visible, error)
    return fieldError(String(message ?? tag), { ...extensions, code })
  }
  return undefined
}

// Own entries only, so a tag such as "constructor" never reads the prototype.
const entry = (
  record: Readonly<Record<string, string>> | undefined,
  tag: string,
): string | undefined =>
  record && Object.hasOwn(record, tag) ? record[tag] : undefined

export const recoverCause = (
  cause: Cause.Cause<unknown>,
  members: readonly ErrorSchema[],
  policy: ErrorPolicy,
  location: Omit<ErrorReport, "cause">,
): Effect.Effect<unknown> =>
  Effect.gen(function* () {
    // Do not turn cancellation into an error response or a telemetry incident.
    if (Cause.hasInterruptsOnly(cause)) return yield* Effect.interrupt
    const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
    const original =
      reason?._tag === "Fail"
        ? reason.error
        : reason?._tag === "Die"
          ? reason.defect
          : undefined
    const visible = visibleFailure(original, members, policy)
    if (visible !== undefined) return visible
    const reporter = yield* Effect.serviceOption(ErrorReporter)
    if (Option.isSome(reporter)) {
      // Telemetry must never replace the original failure or disclose its own defect.
      yield* Effect.suspend(() =>
        reporter.value.report({ cause, ...location }),
      ).pipe(Effect.catchCause(() => Effect.void))
    }
    return fieldError(
      "Internal server error",
      { code: "INTERNAL_SERVER_ERROR" },
      cause,
    )
  })

/** Handles both synchronous throws and Effect failures, before the Promise bridge. */
export const protect = (
  evaluate: () => unknown,
  members: readonly ErrorSchema[],
  policy: ErrorPolicy,
  location: Omit<ErrorReport, "cause">,
): Effect.Effect<unknown, never, unknown> =>
  Effect.suspend(() => {
    const output = evaluate()
    if (output instanceof Error) return Effect.fail(output)
    return Effect.isEffect(output) ? output : Effect.succeed(output)
  }).pipe(
    Effect.flatMap((value) => {
      if (value instanceof Error) return Effect.fail(value)
      // JavaScript callers can bypass the type contract. A truthy Promise must
      // never authorize a request or run outside the Effect cancellation boundary.
      if (
        value !== null &&
        (typeof value === "object" || typeof value === "function") &&
        "then" in value &&
        typeof value.then === "function"
      ) {
        return Effect.die(
          new Error(
            "Return a value or Effect; wrap Promise APIs with Effect.tryPromise",
          ),
        )
      }
      return Effect.succeed(value)
    }),
    Effect.catchCause((cause) =>
      recoverCause(cause, members, policy, location),
    ),
    Effect.withSpan(`GraphQL.${location.phase}`, {
      attributes: { "graphql.field": location.coordinate },
    }),
  )

export function declaredFields(
  schema: ErrorSchema,
  error: object,
): Record<string, unknown> {
  // Error.message can be non-enumerable; never copy stack/cause/incidental fields.
  return Object.fromEntries(
    Object.keys(schema.fields).map((key) => [key, Reflect.get(error, key)]),
  )
}
