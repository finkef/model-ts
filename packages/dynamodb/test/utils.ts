/**
 * `it.effect` with a fresh table per test, so test bodies never call
 * `Effect.provide`. The Vitest project decides which table: the in-memory
 * engine (dynamodb:memory) or DynamoDB Local (dynamodb:local).
 */
import { it as vitest } from "@effect/vitest"
import { Effect, type Layer, type Scope, Tracer } from "effect"
import { inject } from "vitest"
import type { DynamoDB } from "../src/index.ts"
import { Sandbox, type SandboxOptions } from "../src/Sandbox.ts"

declare module "vitest" {
  export interface ProvidedContext {
    readonly dynamodb: "memory" | "local"
  }
}

export const testTable = (
  options?: SandboxOptions,
): Layer.Layer<Sandbox | DynamoDB> =>
  inject("dynamodb") === "local"
    ? Sandbox.layerLocal(options)
    : Sandbox.layerMemory(options)

type Body<A, E, R> = () => Effect.Effect<A, E, R | Scope.Scope>

export const testEffect = <R, LE>(layer: Layer.Layer<R, LE>) => {
  const wrap =
    (tester: typeof vitest.effect | typeof vitest.effect.only) =>
    <A, E>(name: string, body: Body<A, E, R>, timeout?: number) =>
      tester(name, () => Effect.provide(Effect.suspend(body), layer), timeout)
  return {
    effect: Object.assign(wrap(vitest.effect), {
      only: wrap(vitest.effect.only),
    }),
  }
}

export const it = testEffect(testTable())

/** Runs `effect` with a tracer that records every span it creates. */
export const recordSpans = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const spans: Array<Tracer.NativeSpan> = []
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options)
        spans.push(span)
        return span
      },
    })
    const value = yield* effect.pipe(Effect.withTracer(tracer))
    return { value, spans }
  })
