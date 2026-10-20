/**
 * Type-level tests, checked by `bun run typecheck`. Every `@ts-expect-error`
 * must stay an error, or the build fails.
 */
import { model } from "@model-ts/core"
import { type Effect, Schema } from "effect"
import {
  defer,
  type Deferrer,
  type EventBridge,
  eventBridge,
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

// Deferring requires the Deferrer service.
export const deferred: Effect.Effect<void, PublishErrors, Deferrer> =
  event.defer(new Date())

// A model without an address cannot be published or deferred.
class Unaddressed extends model<Unaddressed>()(
  "Unaddressed",
  [eventBridge, defer],
  { id: Schema.String },
) {}
declare const unaddressed: Unaddressed
// @ts-expect-error source and detailType are missing
unaddressed.publish()
// @ts-expect-error source and detailType are missing
unaddressed.defer(new Date())
