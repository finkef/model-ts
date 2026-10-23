import type { Effect, Stream } from "effect"

/** Extract each callback independently: TypeScript otherwise infers the first E/R only. */
type Requirements<A> = A extends (...args: never[]) => infer Value
  ? Requirements<Value>
  : A extends Effect.Effect<unknown, unknown, infer R>
    ? R
    : A extends Stream.Stream<unknown, unknown, infer R>
      ? R
      : never

type From<O, K extends PropertyKey> = K extends keyof O
  ? Requirements<O[K]>
  : never
type Execute<O> = O extends { readonly executor: infer Executor }
  ? From<Executor, "execute">
  : never
type Subscribe<O> = O extends { readonly executor: infer Executor }
  ? From<Executor, "subscribe">
  : never

export type HttpRequirements<O> =
  | Execute<O>
  | From<O, "context" | "healthCheck">
export type WebSocketRequirements<O> =
  | Execute<O>
  | Subscribe<O>
  | From<O, "context" | "onWebsocketConnect" | "onConnect">
export type DeliveryRequirements<O> = Subscribe<O> | From<O, "context">
