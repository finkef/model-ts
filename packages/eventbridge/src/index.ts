/**
 * @model-ts/eventbridge: publish @model-ts/core models as EventBridge events.
 *
 * @since 7.0.0
 */
export * from "./Errors.ts"
export {
  EventBridge,
  type EventBridgeOptions,
  type EventBridgeService,
  type PublishableEvent,
  type PublishedEvent,
  PublishedEvents,
} from "./EventBridge.ts"
export {
  type EventAddress,
  type EventBridgeInstance,
  type EventBridgeMembers,
} from "./Provider.ts"
