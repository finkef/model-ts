import { ModelInstance } from "@model-ts/core"
import {
  EventBridgeClient,
  EventBridgeClientConfig,
  PutEventsCommand,
  PutEventsCommandOutput,
} from "@aws-sdk/client-eventbridge"
import { PublishError } from "./errors"

export interface ClientProps extends EventBridgeClientConfig {
  eventBusName: string
}

export class Client {
  eventBusName: string
  eventBridgeClient: EventBridgeClient

  constructor(options: ClientProps) {
    const { eventBusName, ...clientOptions } = options
    this.eventBridgeClient = new EventBridgeClient(clientOptions)
    this.eventBusName = eventBusName
  }

  async publish(
    ...events: Array<
      ModelInstance<string, any> & {
        source: string
        detailType: string
      }
    >
  ) {
    if (!events.length) return []

    const chunks = chunk(events, 10)

    const entries: NonNullable<PutEventsCommandOutput["Entries"]> = []
    let failedCount = 0

    for (const chunk of chunks) {
      const { Entries, FailedEntryCount } = await this.eventBridgeClient.send(
        new PutEventsCommand({
          Entries: chunk.map((event) => ({
            EventBusName: this.eventBusName,
            Source: event.source,
            DetailType: event.detailType,
            Detail: JSON.stringify(event.encode()),
          })),
        })
      )

      failedCount += FailedEntryCount ?? 0
      entries.push(...(Entries ?? []))
    }

    if (failedCount > 0) {
      throw new PublishError()
    }

    return entries
  }
}

const chunk = <T>(array: T[], size: number) =>
  Array.from({ length: Math.ceil(array.length / size) }, (_, i) =>
    array.slice(i * size, i * size + size)
  )
