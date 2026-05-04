import { either } from "fp-ts/lib/Either"
import * as t from "io-ts"
import { clone } from "io-ts-types/lib/clone"

// Curated set of io-ts-types helpers exposed through `t`.
export {
  BigIntFromString,
  BooleanFromNumber,
  BooleanFromString,
  DateFromISOString,
  DateFromNumber,
  DateFromUnixTime,
  IntFromString,
  Json,
  JsonArray,
  JsonFromString,
  JsonRecord,
  NonEmptyString,
  nonEmptyArray,
  NumberFromString,
  readonlyNonEmptyArray,
  withFallback,
  withMessage,
} from "io-ts-types"

/**
 * Adapted from https://github.com/gcanti/io-ts-types/blob/master/src/withValidate.ts
 */
export const withValidation = <T extends t.Any>(
  type: T,
  validation: (decoded: t.TypeOf<T>) => boolean,
  message?: string
): T => {
  const r: any = clone(type)

  r.validate = (v: any, c: t.Context) =>
    either.chain(type.validate(v, c), (decoded) =>
      validation(decoded)
        ? t.success(decoded)
        : t.failure(decoded, c, message ?? "Failed validation")
    )

  r.decode = (i: any) => r.validate(i, t.getDefaultContext(r))

  /**
   * Models don't allow setting the name property since it is a readonly property here,
   * so we capture this error gracefully.
   */
  try {
    r.name = `withValidation(${type.name})`
  } catch (_) {}

  return r
}
