import { expect, it } from "@effect/vitest"
import { Cause, Context, Deferred, Effect, Fiber, Layer } from "effect"
import { describe, inject } from "vitest"
import { PubSub, Topic } from "../src/index.ts"
import * as Local from "../src/Local.ts"
import { GatewayMessages, Sandbox } from "../src/Sandbox.ts"
import { connect, start, stream } from "./fixtures.ts"

const dependencies = Layer.merge(Sandbox.layerMemory(), Sandbox.layerGateway)
const live = Local.delivery(stream, { lambdaContext: () => ({}) }).pipe(
  Layer.provideMerge(dependencies),
)

describe.skipIf(inject("graphqlDynamoDB") !== "memory")(
  "local delivery",
  () => {
    it.effect(
      "publish → settled runs production delivery and filtering without constructing records",
      () =>
        Effect.gen(function* () {
          yield* connect("local")
          yield* start("local")
          yield* PubSub.publish(Topic.make("user"), {
            domainId: "b",
            message: "filtered",
          })
          yield* PubSub.publish(Topic.make("user"), {
            domainId: "a",
            message: "hello",
          })
          yield* (yield* Local.Delivery).settled
          expect(
            (yield* (yield* GatewayMessages).all).map((message) =>
              JSON.parse(message.data),
            ),
          ).toEqual([
            { type: "connection_ack" },
            { type: "next", id: "1", payload: { data: { message: "hello" } } },
          ])
        }).pipe(Effect.provide(live)),
    )

    it.effect(
      "settled waits for in-flight work and concurrent barriers; batches stay bounded",
      () =>
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const batches: Array<number> = []
          const sequences: Array<string> = []
          const requestIds = new Set<string>()
          let wait = true
          const delivery = Local.delivery(
            (event, context) =>
              Effect.gen(function* () {
                requestIds.add(context.awsRequestId)
                expect(context.getRemainingTimeInMillis()).toBeGreaterThan(0)
                batches.push(event.Records.length)
                if (wait) {
                  wait = false
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                }
                for (const record of event.Records)
                  sequences.push(record.dynamodb!.SequenceNumber!)
                return { batchItemFailures: [] }
              }),
            { batchSize: 2 },
          ).pipe(Layer.provideMerge(Sandbox.layerMemory()))
          yield* Effect.gen(function* () {
            const sandbox = yield* Sandbox
            yield* sandbox.seed({ PK: "test", SK: "1", value: "a" })
            yield* Deferred.await(entered)
            yield* sandbox.seed(
              ...Array.from({ length: 5 }, (_, i) => ({
                PK: "test",
                SK: `${i + 2}`,
              })),
            )
            const settled = (yield* Local.Delivery).settled
            const first = yield* settled.pipe(Effect.forkChild)
            const second = yield* settled.pipe(Effect.forkChild)
            yield* Effect.yieldNow
            expect(first.pollUnsafe()).toBeUndefined()
            yield* Deferred.succeed(release, undefined)
            yield* Fiber.join(first)
            yield* Fiber.join(second)
            expect(sequences).toHaveLength(6)
            expect(sequences.map(Number)).toEqual(
              sequences.map(Number).toSorted((a, b) => a - b),
            )
            expect(batches.every((size) => size > 0 && size <= 2)).toBe(true)
            expect(requestIds.size).toBe(batches.length)
            expect(Object.values(yield* sandbox.snapshot)).toHaveLength(6)
            // Flush marker REMOVE records together with another real write.
            yield* sandbox.seed({ PK: "test", SK: "7" })
            yield* settled
            expect(sequences).toHaveLength(7)
            expect(Object.values(yield* sandbox.snapshot)).toHaveLength(7)
          }).pipe(Effect.provide(delivery))
        }),
    )

    it.effect(
      "failed batches do not kill the consumer or strand settled",
      () => {
        let attempts = 0
        const delivery = Local.delivery((event) =>
          Effect.sync(() => {
            attempts++
            return {
              batchItemFailures: event.Records.map((record) => ({
                itemIdentifier: record.dynamodb!.SequenceNumber!,
              })),
            }
          }),
        ).pipe(Layer.provideMerge(Sandbox.layerMemory()))
        return Effect.gen(function* () {
          yield* PubSub.publish(Topic.make("user"), "hello")
          yield* (yield* Local.Delivery).settled
          yield* PubSub.publish(Topic.make("user"), "again")
          yield* (yield* Local.Delivery).settled
          expect(attempts).toBeGreaterThanOrEqual(2)
        }).pipe(Effect.provide(delivery))
      },
    )

    it.effect("invalid batch sizes are configuration defects", () =>
      Effect.gen(function* () {
        const result = yield* Layer.build(
          Local.delivery(() => Effect.succeed({ batchItemFailures: [] }), {
            batchSize: 0,
          }),
        ).pipe(Effect.exit)
        expect(result._tag).toBe("Failure")
        if (result._tag === "Failure")
          expect(Cause.hasDies(result.cause)).toBe(true)
      }).pipe(Effect.provide(Sandbox.layerMemory())),
    )
    it.effect("settled fails promptly after its delivery scope closes", () =>
      Effect.gen(function* () {
        const service = yield* Effect.scoped(
          Effect.gen(function* () {
            const built = yield* Layer.build(live)
            return Context.get(built, Local.Delivery)
          }),
        )
        const failure = yield* service.settled.pipe(Effect.flip)
        expect(failure._tag).toBe("DeliveryStoppedError")
      }),
    )
  },
)
