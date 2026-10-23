import { Context, type Effect, type Stream, type Schema } from "effect"
import type { StorageError, TransportError, ProtocolClose } from "./Errors.ts"
import type {
  ConnectionId,
  OperationId,
  OperationGeneration,
  Topic,
} from "./Identifiers.ts"
import type {
  Connection,
  Subscription,
  SubscriptionOperation,
  OperationRequest,
  JsonObject,
} from "./Models.ts"

export type StorageFailure = StorageError | TransportError | ProtocolClose

/** Storage owns consistency, expiry, operation limits and complete connection cleanup. */
export interface StorageService {
  readonly create: (
    connection: Connection,
  ) => Effect.Effect<Connection, StorageFailure>
  readonly beginInit: (
    connection: Connection,
  ) => Effect.Effect<Connection, StorageFailure>
  readonly update: (
    connection: Connection,
    changes: {
      readonly context?: Schema.Json
      readonly initParams?: JsonObject
      readonly initialized?: boolean
    },
  ) => Effect.Effect<Connection, StorageFailure>
  readonly liveConnection: (
    id: ConnectionId,
  ) => Effect.Effect<Connection, StorageFailure>
  readonly operation: (
    id: ConnectionId,
    operationId: OperationId,
  ) => Effect.Effect<SubscriptionOperation | undefined, StorageFailure>
  readonly disconnect: (id: ConnectionId) => Effect.Effect<void, StorageFailure>
  readonly reserve: (
    id: ConnectionId,
    operationId: OperationId,
    request: OperationRequest,
    maxOperations: number,
  ) => Effect.Effect<OperationGeneration, StorageFailure>
  readonly activate: (
    id: ConnectionId,
    operationId: OperationId,
    generation: OperationGeneration,
    topics: ReadonlyArray<Topic>,
  ) => Effect.Effect<boolean, StorageFailure>
  readonly stop: (
    id: ConnectionId,
    operationId: OperationId,
    generation?: OperationGeneration,
  ) => Effect.Effect<boolean, StorageFailure>
  readonly subscribers: (
    topic: Topic,
    now: number,
  ) => Stream.Stream<Subscription, StorageFailure>
}

export class Storage extends Context.Service<Storage, StorageService>()(
  "@model-ts/graphql-lambda/Storage",
) {}
