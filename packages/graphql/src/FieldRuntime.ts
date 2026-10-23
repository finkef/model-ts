import { Option } from "effect"
import type { RuntimeRef, OutputRef, Description } from "./FieldTypes.ts"

export function wrap<A>(
  kind: "list" | "nullable",
  of: RuntimeRef,
): OutputRef<A> {
  // A is a phantom value tracked by the field overloads, never runtime data.
  return { kind, of } as OutputRef<A>
}

export function normalize(ref: RuntimeRef, value: unknown): unknown {
  if (typeof ref === "string") return value
  if (ref.kind === "nullable") {
    const unwrapped = Option.isOption(value) ? Option.getOrNull(value) : value
    return unwrapped == null ? null : normalize(ref.of, unwrapped)
  }
  return Array.isArray(value)
    ? value.map((item) => normalize(ref.of, item))
    : value
}

export function describe(options: Description): Description {
  return {
    ...(options.description === undefined
      ? {}
      : { description: options.description }),
    ...(options.deprecationReason === undefined
      ? {}
      : { deprecationReason: options.deprecationReason }),
  }
}
