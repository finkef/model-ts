import { Schema } from "effect"
import { connectionField } from "./Connections.ts"
import { type ErrorPolicy } from "./Errors.ts"
import { wrap, normalize } from "./FieldRuntime.ts"
import { type RuntimeRef, type Field, type Fields } from "./FieldTypes.ts"
import {
  type AnyResolve,
  type AnyConfig,
  config,
  resolverField,
} from "./Resolver.ts"
import { id, invalid } from "./SchemaTypes.ts"
import { eventField } from "./Subscription.ts"

export function makeFields<Types, G, C>(
  policy: ErrorPolicy,
  maxPageSize: number,
  defaultPageSize: number,
): Fields<Types, G, C> {
  const field = (
    ref: RuntimeRef,
    input: AnyResolve | AnyConfig = {},
  ): Field<unknown> => {
    const options = config(input)
    if (options.subscribe) return eventField(policy, ref, options)
    const members = options.errors ?? []
    if (members.some((member) => policy.internal.includes(member)))
      invalid("Internal errors cannot be exposed as result union members")
    const resolve = options.resolve
    return resolverField(policy, options, {
      type: (context, name) =>
        members.length > 0 || options.result !== undefined
          ? context.result(
              typeof ref === "string"
                ? ref
                : ref.kind === "nullable" && typeof ref.of === "string"
                  ? ref.of
                  : invalid(
                      "Result unions require an object type or object union",
                    ),
              members,
              name,
              options.result,
              typeof ref !== "string" && ref.kind === "nullable",
            )
          : context.output(ref, name),
      args: options.args ?? {},
      members,
      call: (source, _raw, decoded, coordinate, context) =>
        resolve
          ? resolve(source, decoded, context)
          : Reflect.get(
              Object(source),
              coordinate.slice(coordinate.lastIndexOf(".") + 1),
            ),
      finish: (value) => normalize(ref, value),
    })
  }
  const wrapped =
    (kind: "list" | "nullable") =>
    (of: RuntimeRef, input?: AnyResolve | AnyConfig) =>
      input === undefined
        ? Object.assign(field(wrap(kind, of)), wrap(kind, of))
        : field(wrap(kind, of), input)
  const scalarHelpers = (nullable: boolean) =>
    Object.fromEntries(
      Object.entries({
        id: "ID",
        string: "String",
        int: "Int",
        float: "Float",
        boolean: "Boolean",
        dateTime: "DateTime",
        json: "JSON",
      }).map(([method, ref]) => [
        method,
        (options?: AnyConfig) =>
          field(nullable ? { kind: "nullable", of: ref } : ref, options),
      ]),
    )
  // The public overloads and generics describe these runtime functions; they are
  // erased here, at the single boundary between typed definitions and graphql-js.
  return {
    ...scalarHelpers(false),
    // A schema can never be field options, so the argument alone selects the
    // position; the public overload admits only string schemas.
    id: (input?: AnyConfig | Schema.Top) =>
      Schema.isSchema(input)
        ? id(input as Schema.Top & Schema.Schema<string>)
        : field("ID", input),
    field,
    connection: (node: string, input?: AnyResolve | AnyConfig) =>
      connectionField(policy, maxPageSize, defaultPageSize, node, input),
    list: wrapped("list"),
    nullable: Object.assign(wrapped("nullable"), scalarHelpers(true)),
  } as unknown as Fields<Types, G, C>
}
