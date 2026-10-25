import { Effect, Stream } from "effect"
import {
  type ErrorPolicy,
  fieldError,
  forbidden,
  protect,
  recoverCause,
} from "./Errors.ts"
import { describe, normalize } from "./FieldRuntime.ts"
import { type RuntimeRef, type Field, fieldRequirements } from "./FieldTypes.ts"
import {
  argumentDecoder,
  inputField,
  nullMessage,
  rejectedNull,
} from "./InputTypes.ts"
import { type AnyConfig } from "./Resolver.ts"

/** Setup and delivery share the resolver policy; authorization runs on every event. */
export const eventField = (
  policy: ErrorPolicy,
  ref: RuntimeRef,
  options: AnyConfig,
): Field<unknown> => ({
  kind: "field",
  [fieldRequirements]: undefined,
  build(context, name) {
    const decode = argumentDecoder(options.args ?? {})
    return {
      type: context.output(ref, name),
      ...describe(options),
      args: Object.fromEntries(
        Object.entries(options.args ?? {}).map(([key, schema]) => [
          key,
          context.input.argument(schema, `${name}(${key})`),
        ]),
      ),
      subscribe(source, raw, request, info) {
        const coordinate = `${info.parentType.name}.${info.fieldName}`
        return request.stream(
          Stream.suspend(() => {
            let decoded: unknown
            try {
              decoded = decode(raw)
            } catch {
              const nullPath = rejectedNull(options.args ?? {}, raw)
              throw fieldError(
                nullPath
                  ? nullMessage(inputField(nullPath, name))
                  : "Invalid arguments",
                { code: "BAD_USER_INPUT" },
              )
            }
            if (!options.subscribe)
              throw new Error("Subscription needs a stream")
            return options.subscribe(source, decoded, request.context)
          }).pipe(
            Stream.catchCause((cause) =>
              Stream.fromEffect(
                recoverCause(cause, [], policy, {
                  coordinate,
                  phase: "subscribe",
                }),
              ),
            ),
          ),
        )
      },
      resolve(value, raw, request, info) {
        const coordinate = `${info.parentType.name}.${info.fieldName}`
        return request.run(
          protect(
            () => {
              if (value instanceof Error) return value
              const decoded = decode(raw)
              return Effect.gen(function* () {
                const allowed = yield* protect(
                  () =>
                    options.authorize
                      ? options.authorize(value, decoded, request.context)
                      : true,
                  [],
                  policy,
                  { coordinate, phase: "authorize" },
                )
                if (allowed instanceof Error) return allowed
                return allowed === true
                  ? normalize(ref, value)
                  : forbidden(info.fieldName)
              })
            },
            [],
            policy,
            { coordinate, phase: "resolve" },
          ),
        )
      },
    }
  },
})
