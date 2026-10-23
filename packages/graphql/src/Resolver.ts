import type { Stream } from "effect"
import { Effect, Schema, SchemaIssue } from "effect"
import {
  type GraphQLFieldConfigArgumentMap,
  type GraphQLOutputType,
} from "graphql"
import {
  type ErrorPolicy,
  fieldError,
  protect,
  forbidden,
  resultSource,
} from "./Errors.ts"
import { describe } from "./FieldRuntime.ts"
import {
  type ArgFields,
  type Description,
  type BuildContext,
  fieldRequirements,
  type Field,
} from "./FieldTypes.ts"
import { argumentDecoder, optionalNullMessage } from "./InputTypes.ts"
import { type ErrorSchema, type ResultOptions, errorName } from "./Results.ts"

export interface Resolver {
  readonly type: (context: BuildContext, name: string) => GraphQLOutputType
  readonly args: ArgFields
  readonly extraArgs?: GraphQLFieldConfigArgumentMap
  readonly members: readonly ErrorSchema[]
  readonly call: (
    source: unknown,
    args: Readonly<Record<string, unknown>>,
    decoded: unknown,
    coordinate: string,
    context: unknown,
  ) => unknown
  readonly finish: (
    value: unknown,
    args: Readonly<Record<string, unknown>>,
  ) => unknown
}

export type AnyResolve = (
  source: unknown,
  args: unknown,
  context: unknown,
) => unknown
/** The runtime view of every field input, before type parameters are fixed. */

export interface AnyConfig extends Description {
  readonly edgeName?: string
  readonly name?: string
  readonly args?: ArgFields
  readonly result?: ResultOptions
  readonly errors?: readonly ErrorSchema[]
  readonly resolve?: AnyResolve
  readonly nodes?: AnyResolve
  readonly authorize?: AnyResolve
  readonly totalCount?: AnyResolve
  readonly subscribe?: (
    source: unknown,
    args: unknown,
    context: unknown,
  ) => Stream.Stream<unknown, unknown, unknown>
}

export const config = (input: AnyResolve | AnyConfig): AnyConfig =>
  typeof input === "function" ? { resolve: input } : input

export const resolverField = <R>(
  policy: ErrorPolicy,
  options: AnyConfig,
  resolver: Resolver,
): Field<R> => ({
  kind: "field",
  [fieldRequirements]: undefined,
  build(context, name, coordinate = name) {
    const decode = argumentDecoder(resolver.args)
    return {
      type: resolver.type(context, coordinate),
      ...describe(options),
      args: {
        ...resolver.extraArgs,
        ...Object.fromEntries(
          Object.entries(resolver.args).map(([key, schema]) => [
            key,
            context.input.argument(schema, `${name}(${key})`),
          ]),
        ),
      },
      resolve(source, raw: Readonly<Record<string, unknown>>, request, info) {
        const coordinate = `${info.parentType.name}.${info.fieldName}`
        return request.run(
          protect(
            () => {
              source = resultSource(source)
              let decoded: unknown
              try {
                decoded = decode(raw)
              } catch (error) {
                if (Schema.isSchemaError(error)) {
                  const member = resolver.members.find(
                    (member) => errorName(member) === "UserInputError",
                  )
                  if (member) {
                    const issue = SchemaIssue.makeFormatterStandardSchemaV1()(
                      error.issue,
                    ).issues[0]
                    const path =
                      issue?.path?.map((key) =>
                        typeof key === "object" ? String(key.key) : String(key),
                      ) ?? []
                    const field =
                      (path[0] === "input" ? path.slice(1) : path).join(".") ||
                      name
                    return Effect.fail(
                      Schema.decodeUnknownSync(member)({
                        _tag: "UserInputError",
                        message:
                          optionalNullMessage(resolver.args, raw) ??
                          issue?.message ??
                          "Invalid arguments",
                        field,
                      }),
                    )
                  }
                }
                throw fieldError(
                  optionalNullMessage(resolver.args, raw) ??
                    "Invalid arguments",
                  {
                    code: "BAD_USER_INPUT",
                  },
                )
              }
              return Effect.gen(function* () {
                const allowed = yield* protect(
                  () =>
                    options.authorize
                      ? options.authorize(source, decoded, request.context)
                      : true,
                  [],
                  policy,
                  { coordinate, phase: "authorize" },
                )
                if (allowed instanceof Error) return allowed
                if (allowed !== true) return forbidden(info.fieldName)
                return yield* Effect.suspend(() => {
                  const output = resolver.call(
                    source,
                    raw,
                    decoded,
                    coordinate,
                    request.context,
                  )
                  return Effect.isEffect(output)
                    ? output
                    : Effect.succeed(output)
                }).pipe(
                  Effect.map((value) =>
                    value instanceof Error
                      ? value
                      : resolver.finish(value, raw),
                  ),
                )
              })
            },
            resolver.members,
            policy,
            { coordinate, phase: "resolve" },
          ),
        )
      },
    }
  },
})
