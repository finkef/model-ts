import { Schema } from "effect"

/** prepare owns the original-cause log; transport boundaries must not report it again. */
export class RequestValidationError extends Schema.TaggedError<RequestValidationError>()(
  "RequestValidationError",
  { cause: Schema.Defect() },
) {}
