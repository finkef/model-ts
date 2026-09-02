import * as t from "io-ts"
import reporter from "io-ts-reporters"
import { left } from "fp-ts/lib/Either"

export class RuntimeTypeValidationError extends Error {
  readonly _tag = "RuntimeTypeValidationError" as const
  errors: t.Errors

  constructor(errors: t.Errors)
  constructor(message: string)
  constructor(errors: t.Errors | string) {
    super(
      typeof errors === "string"
        ? errors
        : reporter.report(left(errors)).join("\n")
    )

    this.errors = typeof errors === "string" ? [] : errors
  }
}
