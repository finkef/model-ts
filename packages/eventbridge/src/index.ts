/**
 * @model-ts/eventbridge: publish and defer @model-ts/core models as
 * EventBridge events.
 *
 * @since 7.0.0
 */
export {
  type DeferredEvent,
  DeferredEvents,
  Deferrer,
  type DeferrerService,
} from "./Deferrer.ts"
export * from "./Errors.ts"
export {
  EventBridge,
  type EventBridgeOptions,
  type EventBridgeService,
  publish,
  type PublishableEvent,
  type PublishedEvent,
  PublishedEvents,
} from "./EventBridge.ts"
export {
  defer,
  type DeferInstance,
  type DeferMembers,
  type EventAddress,
  eventBridge,
  type EventBridgeInstance,
  type EventBridgeMembers,
} from "./Provider.ts"
