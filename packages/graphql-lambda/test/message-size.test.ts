import { expect, it } from "@effect/vitest"
import { Effect } from "effect"
import {
  Connection,
  ConnectionId,
  GatewayMessages,
  send,
} from "../src/index.ts"
import { testLayer } from "./fixtures.ts"

it.effect(
  "accepts full messages through 32 KiB and rejects the next UTF-8 byte before sending",
  () =>
    Effect.gen(function* () {
      const connection = new Connection({
        id: ConnectionId.make("large-message"),
        endpoint: "https://example.execute-api.local/ws",
        context: {},
        initialized: true,
        initReceived: true,
        createdAt: 0,
        ttl: 7200,
      })
      const gateway = yield* GatewayMessages
      const overhead = Buffer.byteLength(
        JSON.stringify({ type: "ping", payload: { value: "" } }),
      )
      const frame = (bytes: number) => ({
        type: "ping" as const,
        payload: {
          value:
            "é".repeat(Math.floor((bytes - overhead) / 2)) +
            "x".repeat((bytes - overhead) % 2),
        },
      })
      for (const bytes of [32 * 1024 - 1, 32 * 1024]) {
        const message = frame(bytes)
        expect(Buffer.byteLength(JSON.stringify(message))).toBe(bytes)
        expect(yield* send(connection, message)).toBe(true)
      }
      const tooLarge = frame(32 * 1024 + 1)
      // Character count is below the limit: enforcing bytes is essential for non-ASCII payloads.
      expect(JSON.stringify(tooLarge).length).toBeLessThan(32 * 1024)
      expect(yield* send(connection, tooLarge).pipe(Effect.flip)).toMatchObject(
        {
          _tag: "MessageTooLarge",
          message: "WebSocket result exceeds the 32 KiB single-frame limit",
        },
      )
      expect(
        (yield* gateway.all).map((message) => Buffer.byteLength(message.data)),
      ).toEqual([32 * 1024 - 1, 32 * 1024])
      expect(yield* gateway.closed).toEqual([])
    }).pipe(Effect.provide(testLayer())),
)
