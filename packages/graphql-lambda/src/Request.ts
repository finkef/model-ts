import type { ValidationRule } from "graphql"
import type { Connection, JsonObject, PublishedEvent } from "./Models.ts"
import type { PubSub } from "./PubSub.ts"

/** Structural API Gateway payload v1.0 types: real AWS events are assignable. */
export interface HttpEvent {
  readonly httpMethod: string
  readonly path: string
  readonly headers: Readonly<Record<string, string | undefined>> | null
  readonly multiValueHeaders?: Readonly<
    Record<string, ReadonlyArray<string> | undefined>
  > | null
  readonly queryStringParameters?: Readonly<
    Record<string, string | undefined>
  > | null
  readonly body: string | null
  readonly isBase64Encoded?: boolean
  readonly requestContext: {
    readonly requestId?: string
    readonly identity?: { readonly sourceIp?: string }
  }
}
export interface WebSocketEvent {
  readonly headers?: Readonly<Record<string, string | undefined>> | null
  readonly body?: string | null
  readonly isBase64Encoded?: boolean
  readonly requestContext: {
    readonly routeKey: string
    readonly connectionId: string
    readonly domainName: string
    readonly stage: string
    readonly requestId?: string
    readonly identity?: { readonly sourceIp?: string }
  }
}
export interface StreamRecord {
  readonly eventID?: string
  readonly eventName?: string
  readonly dynamodb?: {
    /** Lambda event images are untrusted JSON, not SDK client AttributeValue instances. */
    readonly NewImage?: unknown
    readonly SequenceNumber?: string
  }
}
/** Inline delivery has no DynamoDB stream record; domain metadata can travel in lambdaContext. */
export interface DirectEvent {
  readonly delivery: "direct"
  readonly publishedEvent: PublishedEvent
}
export interface StreamEvent {
  readonly Records: ReadonlyArray<StreamRecord>
}
export interface ProxyResponse {
  readonly statusCode: number
  readonly headers?: Record<string, string>
  readonly multiValueHeaders?: Record<string, Array<string>>
  readonly body: string
}

/** Response mutations are isolated per HTTP invocation, shared by its batched operations. */
export interface ResponseHeaders {
  readonly headers: Record<string, string>
  readonly cookies: Array<string>
}
export interface ContextRequest<L> {
  readonly event: HttpEvent | WebSocketEvent | StreamRecord | DirectEvent
  readonly lambdaContext: L
  readonly connection?: Connection
  readonly persisted: JsonObject
  readonly pubSub: PubSub
  readonly response: ResponseHeaders
}
export interface ValidationOptions {
  readonly introspection?: boolean
  readonly validationRules?: ReadonlyArray<ValidationRule>
  readonly maxQueryBytes?: number
}
