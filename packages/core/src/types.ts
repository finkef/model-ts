import { either } from "fp-ts/lib/Either"
import * as t from "io-ts"
import { clone } from "io-ts-types/lib/clone"

// Curated set of io-ts-types helpers exposed through `t`.
export {
  BigIntFromString,
  type BigIntFromStringC,
} from "io-ts-types/lib/BigIntFromString"
export {
  BooleanFromNumber,
  type BooleanFromNumberC,
} from "io-ts-types/lib/BooleanFromNumber"
export {
  BooleanFromString,
  type BooleanFromStringC,
} from "io-ts-types/lib/BooleanFromString"
export {
  DateFromISOString,
  type DateFromISOStringC,
} from "io-ts-types/lib/DateFromISOString"
export {
  DateFromNumber,
  type DateFromNumberC,
} from "io-ts-types/lib/DateFromNumber"
export {
  DateFromUnixTime,
  type DateFromUnixTimeC,
} from "io-ts-types/lib/DateFromUnixTime"
export {
  IntFromString,
  type IntFromStringC,
} from "io-ts-types/lib/IntFromString"
export {
  Json,
  JsonArray,
  JsonFromString,
  JsonRecord,
} from "io-ts-types/lib/JsonFromString"
export {
  NonEmptyString,
  type NonEmptyStringC,
} from "io-ts-types/lib/NonEmptyString"
export {
  nonEmptyArray,
  type NonEmptyArrayC,
} from "io-ts-types/lib/nonEmptyArray"
export {
  NumberFromString,
  type NumberFromStringC,
} from "io-ts-types/lib/NumberFromString"
export {
  readonlyNonEmptyArray,
  type ReadonlyNonEmptyArrayC,
} from "io-ts-types/lib/readonlyNonEmptyArray"
export { withFallback } from "io-ts-types/lib/withFallback"
export { withMessage } from "io-ts-types/lib/withMessage"

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
