/**
 * Reads a DynamoDB Local table's stream the way Lambda's event source
 * mapping does, from the moment of the subscription.
 */
import type { DynamoDBClientConfig } from "@aws-sdk/client-dynamodb"
import type {
  _Record,
  AttributeValue,
  Shard,
} from "@aws-sdk/client-dynamodb-streams"
import { Duration, Effect, type Scope, Stream } from "effect"
import type { Image, StreamRecord } from "../Stream.ts"
import { imageToWire } from "./wire.ts"

/** How long to wait before polling again after an empty read. */
const IDLE = Duration.millis(50)

export const streamChanges = (
  config: DynamoDBClientConfig,
  streamArn: string,
): Effect.Effect<Stream.Stream<StreamRecord>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const sdk = yield* Effect.promise(
      () => import("@aws-sdk/client-dynamodb-streams"),
    )
    const client = yield* Effect.acquireRelease(
      Effect.sync(() => new sdk.DynamoDBStreamsClient(config)),
      (client) => Effect.sync(() => client.destroy()),
    )
    const shards = async () => {
      const all: Array<Shard> = []
      let ExclusiveStartShardId: string | undefined
      do {
        const { StreamDescription } = await client.send(
          new sdk.DescribeStreamCommand({
            StreamArn: streamArn,
            ExclusiveStartShardId,
          }),
        )
        all.push(...(StreamDescription?.Shards ?? []))
        ExclusiveStartShardId = StreamDescription?.LastEvaluatedShardId
      } while (ExclusiveStartShardId)
      return all
    }
    const iterator = async (
      ShardId: string,
      ShardIteratorType: "LATEST" | "TRIM_HORIZON",
    ) => {
      const { ShardIterator } = await client.send(
        new sdk.GetShardIteratorCommand({
          StreamArn: streamArn,
          ShardId,
          ShardIteratorType,
        }),
      )
      return ShardIterator
    }

    // Open shards hold the records still to come; read them from now on.
    const iterators = new Map<string, string>()
    const start = async (
      opened: ReadonlyArray<Shard>,
      type: "LATEST" | "TRIM_HORIZON",
    ) => {
      for (const { ShardId } of opened) {
        const position = ShardId && (await iterator(ShardId, type))
        if (ShardId && position) iterators.set(ShardId, position)
      }
    }
    yield* Effect.promise(async () =>
      start(
        (await shards()).filter(
          (shard) => !shard.SequenceNumberRange?.EndingSequenceNumber,
        ),
        "LATEST",
      ),
    )

    const poll = async () => {
      const records: Array<StreamRecord> = []
      for (const [shardId, position] of iterators) {
        const page = await client.send(
          new sdk.GetRecordsCommand({ ShardIterator: position }),
        )
        records.push(
          ...(page.Records ?? []).map((record) => toRecord(record, streamArn)),
        )
        if (page.NextShardIterator) {
          iterators.set(shardId, page.NextShardIterator)
          continue
        }
        // A closed shard continues in its children, from their first record.
        iterators.delete(shardId)
        await start(
          (await shards()).filter((shard) => shard.ParentShardId === shardId),
          "TRIM_HORIZON",
        )
      }
      return records
    }

    return Stream.fromEffectRepeat(
      Effect.promise(poll).pipe(
        Effect.tap((records) =>
          records.length === 0 ? Effect.sleep(IDLE) : Effect.void,
        ),
      ),
    ).pipe(Stream.flattenIterable)
  })

const toRecord = (record: _Record, eventSourceARN: string): StreamRecord => {
  const change = record.dynamodb ?? {}
  return {
    eventID: record.eventID ?? "",
    eventName: record.eventName ?? "MODIFY",
    eventVersion: record.eventVersion ?? "1.1",
    eventSource: "aws:dynamodb",
    awsRegion: record.awsRegion ?? "",
    eventSourceARN,
    dynamodb: {
      ApproximateCreationDateTime: Math.floor(
        (change.ApproximateCreationDateTime?.getTime() ?? 0) / 1000,
      ),
      Keys: image(change.Keys),
      ...(change.NewImage && { NewImage: image(change.NewImage) }),
      ...(change.OldImage && { OldImage: image(change.OldImage) }),
      SequenceNumber: change.SequenceNumber ?? "",
      SizeBytes: change.SizeBytes ?? 0,
      StreamViewType: "NEW_AND_OLD_IMAGES",
    },
  }
}

const image = (values: Record<string, AttributeValue> = {}): Image =>
  imageToWire(values)
