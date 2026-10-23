import { PutCommand } from "@aws-sdk/lib-dynamodb"
import { describe, expect } from "@effect/vitest"
import { Effect, Stream } from "effect"
import { DynamoDB } from "../src/index.ts"
import { Sandbox } from "../src/Sandbox.ts"
import type { StreamRecord } from "../src/Stream.ts"
import { makePost, Post } from "./fixtures.ts"
import { it } from "./utils.ts"

/** The parts of a record both sandboxes agree on. */
const summary = ({ eventName, dynamodb }: StreamRecord) => ({
  eventName,
  Keys: dynamodb.Keys,
  ...(dynamodb.OldImage && { OldImage: dynamodb.OldImage }),
  ...(dynamodb.NewImage && { NewImage: dynamodb.NewImage }),
})

const take = (changes: Stream.Stream<StreamRecord>, count: number) =>
  changes.pipe(Stream.take(count), Stream.runCollect)

describe("changes", () => {
  it.effect("emits a record per committed write, as Lambda receives it", () =>
    Effect.gen(function* () {
      const changes = yield* (yield* Sandbox).changes

      const post = yield* makePost(1).put()
      yield* post.update({ title: "Renamed" })
      yield* Post.delete(post)

      const records = yield* take(changes, 3)
      expect(records.map(({ eventName }) => eventName)).toEqual([
        "INSERT",
        "MODIFY",
        "REMOVE",
      ])
      const [insert, modify] = records
      expect(insert).toMatchObject({
        eventSource: "aws:dynamodb",
        eventSourceARN: expect.stringContaining(":table/sandbox-"),
        dynamodb: {
          ApproximateCreationDateTime: expect.any(Number),
          SequenceNumber: expect.any(String),
          SizeBytes: expect.any(Number),
          StreamViewType: "NEW_AND_OLD_IMAGES",
        },
      })
      expect(summary(insert)).toMatchInlineSnapshot(`
        {
          "Keys": {
            "PK": {
              "S": "POST#post_001",
            },
            "SK": {
              "S": "POST",
            },
          },
          "NewImage": {
            "GSI2PK": {
              "S": "AUTHOR#ada",
            },
            "GSI2SK": {
              "S": "POST#post_001",
            },
            "PK": {
              "S": "POST#post_001",
            },
            "SK": {
              "S": "POST",
            },
            "_docVersion": {
              "N": "0",
            },
            "_tag": {
              "S": "Post",
            },
            "authorId": {
              "S": "ada",
            },
            "id": {
              "S": "post_001",
            },
            "publishedAt": {
              "NULL": true,
            },
            "title": {
              "S": "Post 1",
            },
            "views": {
              "N": "0",
            },
          },
          "eventName": "INSERT",
        }
      `)
      expect(modify.dynamodb.OldImage?.title).toEqual({ S: "Post 1" })
      expect(modify.dynamodb.NewImage?.title).toEqual({ S: "Renamed" })
      expect(records[2].dynamodb.NewImage).toBeUndefined()
      expect(records[2].dynamodb.OldImage?.title).toEqual({ S: "Renamed" })

      const sequence = records.map(({ dynamodb }) =>
        BigInt(dynamodb.SequenceNumber),
      )
      expect(sequence).toEqual(sequence.toSorted((a, b) => (a < b ? -1 : 1)))
    }),
  )

  it.effect("leaves out writes that change nothing", () =>
    Effect.gen(function* () {
      yield* makePost(1).put()
      const changes = yield* (yield* Sandbox).changes

      yield* makePost(1).put({ ignoreExistence: true })
      yield* makePost(2).put()

      const [record] = yield* take(changes, 1)
      expect(record.dynamodb.Keys.PK).toEqual({ S: "POST#post_002" })
    }),
  )

  it.effect(
    "emits a committed transaction and nothing for a cancelled one",
    () =>
      Effect.gen(function* () {
        yield* makePost(1).put()
        const changes = yield* (yield* Sandbox).changes

        yield* DynamoDB.bulk([
          makePost(2).operation("put"),
          makePost(1).operation("put"),
        ]).pipe(Effect.flip)
        yield* DynamoDB.bulk([
          makePost(3).operation("put"),
          Post.operation("delete", makePost(1)),
        ])

        const records = yield* take(changes, 2)
        expect(
          records.map(({ eventName, dynamodb }) => [
            eventName,
            dynamodb.Keys.PK,
          ]),
        ).toEqual(
          expect.arrayContaining([
            ["INSERT", { S: "POST#post_003" }],
            ["REMOVE", { S: "POST#post_001" }],
          ]),
        )
      }),
  )

  it.effect("carries binary values and sets in wire form", () =>
    Effect.gen(function* () {
      const db = yield* DynamoDB
      const changes = yield* (yield* Sandbox).changes

      yield* db.send(
        "PutItem",
        new PutCommand({
          TableName: db.tableName,
          Item: {
            PK: "TYPES",
            SK: "1",
            bytes: new Uint8Array([1, 2, 3]),
            strings: new Set(["a"]),
            nested: { list: [new Uint8Array([6])] },
          },
        }),
      )

      const [record] = yield* take(changes, 1)
      expect(record.dynamodb.NewImage).toEqual({
        PK: { S: "TYPES" },
        SK: { S: "1" },
        bytes: { B: "AQID" },
        strings: { SS: ["a"] },
        nested: { M: { list: { L: [{ B: "Bg==" }] } } },
      })
    }),
  )

  it.effect("only carries writes made after the subscription", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox
      yield* sandbox.seed(makePost(1))
      const changes = yield* sandbox.changes

      yield* sandbox.seed(makePost(2))

      const [record] = yield* take(changes, 1)
      expect(record.eventName).toBe("INSERT")
      expect(record.dynamodb.Keys.PK).toEqual({ S: "POST#post_002" })
    }),
  )
})
