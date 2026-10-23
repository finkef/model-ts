import type { ValidationRule } from "graphql"
import type { Connection, JsonObject } from "./Models.ts"
import type { Source } from "./PubSub.ts"

/** HTTP API and Lambda function URL payload format 2.0. */
export interface HttpEvent {
  readonly version: "2.0"
  readonly routeKey: string
  readonly rawPath: string
  readonly rawQueryString: string
  readonly cookies?: ReadonlyArray<string>
  /** API Gateway lowercases names and comma-joins duplicate values. */
  readonly headers: Readonly<Record<string, string | undefined>>
  readonly queryStringParameters?: Readonly<Record<string, string | undefined>>
  readonly body?: string
  readonly isBase64Encoded: boolean
  readonly requestContext: {
    readonly requestId?: string
    readonly http: {
      readonly method: string
      readonly path: string
      readonly sourceIp: string
      readonly userAgent: string
    }
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
export interface StreamEvent {
  readonly Records: ReadonlyArray<StreamRecord>
}
export interface ProxyResponse {
  readonly statusCode: number
  readonly headers?: Record<string, string>
  readonly body: string
}
export interface HttpResponse extends ProxyResponse {
  readonly isBase64Encoded: boolean
  readonly cookies?: Array<string>
}

/** Response mutations are isolated per HTTP invocation, shared by its batched operations. */
export interface ResponseHeaders {
  readonly headers: Record<string, string>
  readonly cookies: Array<string>
}
export interface ContextRequest<L, P = JsonObject> {
  readonly event: HttpEvent | WebSocketEvent | StreamRecord
  readonly lambdaContext: L
  readonly connection?: Connection
  readonly persisted: P
  readonly pubSub: Source
  readonly response: ResponseHeaders
}
export interface ValidationOptions {
  readonly introspection?: boolean
  readonly validationRules?: ReadonlyArray<ValidationRule>
  readonly maxQueryBytes?: number
}
