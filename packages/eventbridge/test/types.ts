/**
 * Type-level tests, checked by `bun run typecheck`. Every `@ts-expect-error`
 * must stay an error, or the build fails.
 */
import { Model } from "@model-ts/core"
import { type Effect, Schema } from "effect"
import {
  EventBridge,
  type PublishErrors,
  type ResultEntry,
} from "../src/index.ts"
import { type PostPublished } from "./fixtures.ts"

declare const event: PostPublished

// Publishing requires the EventBridge service and fails with PublishErrors.
export const published: Effect.Effect<
  ReadonlyArray<ResultEntry>,
  PublishErrors,
  EventBridge
> = event.publish()

// A model without an address cannot be published.
class Unaddressed extends Model.Class<Unaddressed>()(
  "Unaddressed",
  EventBridge.provider,
  { id: Schema.String },
) {}
declare const unaddressed: Unaddressed
// @ts-expect-error source and detailType are missing
unaddressed.publish()
