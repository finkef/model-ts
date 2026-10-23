export interface GraphiQLOptions {
  /** Defaults to same-origin. */
  readonly credentials?: "include" | "same-origin" | "omit"
  /** HTTP endpoint; defaults to the request's rawPath. */
  readonly endpoint?: string
  /** Uses graphql-ws with the graphql-transport-ws protocol. */
  readonly subscriptionUrl?: `wss://${string}`
  /** HTTP defaults, also sent as WebSocket connection_init parameters. */
  readonly headers?: Readonly<Record<string, string>>
  readonly query?: string
  readonly title?: string
  /** Applied to the inline scripts and style; the application owns CSP headers. */
  readonly nonce?: string
}
