import { Context, Effect, Layer } from "effect"
/**
 * An EventBridge endpoint inside the process, plugged into the real AWS SDK
 * client as its request handler. It records every PutEvents request as the
 * SDK serialized it and accepts every entry unless told otherwise.
 */
import { EventBridge } from "../src/index.ts"

interface PutEventsBody {
  readonly Entries: ReadonlyArray<{
    readonly EventBusName: string
    readonly Source: string
    readonly DetailType: string
    readonly Detail: string
  }>
}

interface HttpRequest {
  readonly headers: Readonly<Record<string, string | undefined>>
  readonly body?: string | Uint8Array
}

export class FakeEventBridge {
  readonly requests: Array<PutEventsBody> = []
  /** Entries whose detail type is listed here come back as failed. */
  readonly rejectDetailTypes = new Set<string>()
  /** When set, every request fails with this HTTP status. */
  failWithStatus: number | undefined
  private nextId = 0

  readonly requestHandler = {
    metadata: { handlerProtocol: "fake/1.0" },
    handle: async (request: HttpRequest) => {
      if (this.failWithStatus !== undefined) {
        return respond(this.failWithStatus, {
          __type: "AccessDeniedException",
          message: "fake access denied",
        })
      }
      const raw = request.body ?? "{}"
      const body: PutEventsBody = JSON.parse(
        typeof raw === "string" ? raw : new TextDecoder().decode(raw),
      )
      this.requests.push(body)
      const entries = body.Entries.map((entry) =>
        this.rejectDetailTypes.has(entry.DetailType)
          ? { ErrorCode: "InternalFailure", ErrorMessage: "rejected by fake" }
          : { EventId: `evt-${++this.nextId}` },
      )
      return respond(200, {
        FailedEntryCount: entries.filter((entry) => "ErrorCode" in entry)
          .length,
        Entries: entries,
      })
    },
  }
}

const respond = (statusCode: number, body: object) => ({
  response: {
    statusCode,
    reason: statusCode === 200 ? "OK" : "Error",
    headers: { "content-type": "application/x-amz-json-1.1" },
    body: new TextEncoder().encode(JSON.stringify(body)),
  },
})

/** The fake behind `FakeBus.layer`, so tests can inspect and steer it. */
export class FakeBus extends Context.Service<FakeBus, FakeEventBridge>()(
  "test/FakeBus",
) {
  /** A real `EventBridge.layer` talking to a fresh fake. */
  static readonly layer: Layer.Layer<FakeBus | EventBridge> = Layer.unwrap(
    Effect.sync(() => {
      const fake = new FakeEventBridge()
      return Layer.merge(
        Layer.succeed(FakeBus, fake),
        EventBridge.layer({
          eventBusName: "test-bus",
          client: {
            region: "eu-central-1",
            credentials: { accessKeyId: "fake", secretAccessKey: "fake" },
            endpoint: "http://eventbridge.fake",
            maxAttempts: 1,
            requestHandler: fake.requestHandler,
          },
        }),
      )
    }),
  )
}
