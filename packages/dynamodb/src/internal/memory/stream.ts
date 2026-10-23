import { randomBytes } from "node:crypto"
import { Effect, PubSub, type Scope, Stream } from "effect"
import type { Image, StreamRecord } from "../../Stream.ts"
import { toWire } from "../wire.ts"
import type { ItemChange } from "./engine.ts"
import type { InMemoryItem } from "./utils.ts"

/**
 * The stream of a memory table. `onChange` takes the engine's changes and
 * `changes` subscribes to their records. Records are only built while
 * someone is subscribed, so writes in tests that never subscribe cost nothing.
 */
export const makeMemoryStream = (tableName: string) =>
  Effect.gen(function* () {
    const pubsub = yield* Effect.acquireRelease(
      PubSub.unbounded<StreamRecord>(),
      PubSub.shutdown,
    )
    const toRecord = makeStreamRecords(tableName)
    let subscribers = 0
    const onChange = (changes: ReadonlyArray<ItemChange>) => {
      if (subscribers === 0) return
      for (const change of changes) {
        if (change.tableName === tableName) {
          PubSub.publishUnsafe(pubsub, toRecord(change))
        }
      }
    }
    const changes: Effect.Effect<
      Stream.Stream<StreamRecord>,
      never,
      Scope.Scope
    > = Effect.acquireRelease(
      Effect.sync(() => {
        subscribers += 1
      }),
      () =>
        Effect.sync(() => {
          subscribers -= 1
        }),
    ).pipe(
      Effect.andThen(PubSub.subscribe(pubsub)),
      Effect.map(Stream.fromSubscription),
    )
    return { onChange, changes }
  })

/** Turns changes into the records a table's stream carries, numbered in commit order. */
const makeStreamRecords = (tableName: string) => {
  const created = new Date().toISOString().slice(0, -1)
  const eventSourceARN = `arn:aws:dynamodb:local:000000000000:table/${tableName}/stream/${created}`
  let sequence = 0
  return ({ before, after }: ItemChange): StreamRecord => {
    const { PK, SK } = after ?? before ?? {}
    const images = {
      Keys: image({ PK, SK }),
      ...(after && { NewImage: image(after) }),
      ...(before && { OldImage: image(before) }),
    }
    sequence += 1
    return {
      eventID: randomBytes(16).toString("hex"),
      eventName: !before ? "INSERT" : !after ? "REMOVE" : "MODIFY",
      eventVersion: "1.1",
      eventSource: "aws:dynamodb",
      awsRegion: "local",
      eventSourceARN,
      dynamodb: {
        ApproximateCreationDateTime: Math.floor(Date.now() / 1000),
        ...images,
        SequenceNumber: String(sequence).padStart(21, "0"),
        SizeBytes: Buffer.byteLength(JSON.stringify(images)),
        StreamViewType: "NEW_AND_OLD_IMAGES",
      },
    }
  }
}

const image = (item: InMemoryItem): Image => toWire(item) ?? {}
