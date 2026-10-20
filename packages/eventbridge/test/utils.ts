import { it as vitest } from "@effect/vitest"
import { Effect, type Layer, type Scope, Tracer } from "effect"

type Body<A, E, R> = () => Effect.Effect<A, E, R | Scope.Scope>

/**
 * `it.effect` with `layer` built fresh for every test, so test bodies never
 * call `Effect.provide` and no state leaks between tests.
 */
export const testEffect =
  <R, LE>(layer: Layer.Layer<R, LE>) =>
  <A, E>(name: string, body: Body<A, E, R>) =>
    vitest.effect(name, () => Effect.provide(Effect.suspend(body), layer))

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
