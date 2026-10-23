import { Cause, Effect, Schema } from "effect"
import {
  getOperationAST,
  GraphQLError,
  NoSchemaIntrospectionCustomRule,
  parse,
  specifiedRules,
  SingleFieldSubscriptionsRule,
  validate,
} from "graphql"
import {
  badRequest,
  requestError,
  ExecutionError,
  toGraphQLError,
} from "../Errors.ts"
import type { Executor } from "../Executor.ts"
import { OperationRequest } from "../Models.ts"
import type { ValidationOptions } from "../Request.ts"
import { InputError } from "./InputError.ts"
import { RequestValidationError } from "./RequestValidationError.ts"
import { inputSafeSchema } from "./schema.ts"
import { singleRootFieldSubscriptionsRule } from "./SubscriptionRule.ts"

export const header = (
  event: {
    readonly headers?: Readonly<Record<string, string | undefined>> | null
  },
  name: string,
) =>
  Object.entries(event.headers ?? {}).find(
    ([key]) => key.toLowerCase() === name.toLowerCase(),
  )?.[1]

export const parseJson = (text: string) =>
  Effect.try({
    try: (): unknown => JSON.parse(text),
    catch: () => badRequest("Invalid JSON"),
  })
export const decodeRequest = (input: unknown) =>
  Schema.decodeUnknownEffect(OperationRequest)(input).pipe(
    Effect.mapError(() =>
      badRequest(
        "Expected a GraphQL query, variables object and optional operationName",
      ),
    ),
  )

// Replace, rather than append, the native rule to preserve GraphQL 16 diagnostic counts/order.
const standardRules = specifiedRules.map((rule) =>
  rule === SingleFieldSubscriptionsRule
    ? singleRootFieldSubscriptionsRule
    : rule,
)

export const prepare = (
  executor: Pick<Executor<never>, "schema" | "validation">,
  request: OperationRequest,
  options: ValidationOptions,
) =>
  Effect.gen(function* () {
    if (Buffer.byteLength(request.query) > (options.maxQueryBytes ?? 64 * 1024))
      return yield* badRequest("GraphQL operation is too large")
    const document = yield* Effect.try({
      try: () => parse(request.query),
      catch: (error) =>
        error instanceof GraphQLError
          ? requestError(error, "GRAPHQL_PARSE_FAILED")
          : new ExecutionError({ cause: error }),
    })
    return yield* Effect.try({
      try: () => {
        const owned = executor.validation === "executor"
        const safeSchema = owned
          ? executor.schema
          : inputSafeSchema(executor.schema)
        // Scalar literal parsing belongs to the executor that owns its error policy.
        const rules = [
          ...(owned ? [singleRootFieldSubscriptionsRule] : standardRules),
          ...(options.introspection === false
            ? [NoSchemaIntrospectionCustomRule]
            : []),
        ]
        const errors = rules.length
          ? validate(safeSchema, document, rules).map((error) =>
              requestError(error, "GRAPHQL_VALIDATION_FAILED"),
            )
          : []
        if (options.validationRules?.length && errors.length === 0) {
          // Some application rules assume fragments/types already passed standard validation.
          // An executor-owned gate never exposes its copies of scalar/coercion errors.
          const valid =
            !owned || validate(safeSchema, document, standardRules).length === 0
          if (valid)
            errors.push(
              ...validate(safeSchema, document, options.validationRules).map(
                (error) => toGraphQLError(new InputError(error)),
              ),
            )
        }
        const operation = getOperationAST(document, request.operationName)
        if (!operation && errors.length === 0)
          errors.push(
            requestError(
              new GraphQLError("An unambiguous operationName is required"),
              "GRAPHQL_VALIDATION_FAILED",
            ),
          )
        return { document, operation, errors }
      },
      catch: (cause) => new RequestValidationError({ cause }),
    }).pipe(
      Effect.tapError((error) =>
        Effect.logError(
          "GraphQL request validation failed",
          Cause.die(error.cause),
        ),
      ),
    )
  })
