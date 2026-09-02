// Keep this module separate from index.ts so Effect remains an optional peer dependency.
import * as Effect from "effect/Effect"
import { isLeft } from "fp-ts/Either"
import * as t from "./t"
import { RuntimeTypeValidationError } from "./runtime-type-validation-error"
import { Union, MemberOf } from "./union"
import { ModelConstructor } from "./utils"

export { RuntimeTypeValidationError }

export type DecodeError = RuntimeTypeValidationError

type Decoder = {
  decode(value: unknown): t.Validation<any>
}

const decodeWithSpan = Effect.fn("@model-ts/core/decode")(function* (
  decoder: Decoder,
  value: unknown
): Effect.fn.Return<any, RuntimeTypeValidationError> {
  return yield* Effect.suspend(() => {
    const result = decoder.decode(value)
    return isLeft(result)
      ? Effect.fail(new RuntimeTypeValidationError(result.left))
      : Effect.succeed(result.right)
  })
})

/**
 * Lazily decodes a model, union, or io-ts codec.
 */
export function decode<M extends ModelConstructor<any>>(
  model: M,
  value: unknown
): Effect.Effect<InstanceType<M>, RuntimeTypeValidationError>
export function decode<U extends Union>(
  union: U,
  value: unknown
): Effect.Effect<MemberOf<U>, RuntimeTypeValidationError>
export function decode<C extends t.Any>(
  codec: C,
  value: unknown
): Effect.Effect<t.TypeOf<C>, RuntimeTypeValidationError>
export function decode(
  decoder: Decoder,
  value: unknown
): Effect.Effect<any, RuntimeTypeValidationError> {
  return decodeWithSpan(decoder, value)
}
