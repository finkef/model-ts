import { ModelInstance, Provider } from "@model-ts/core"
import { PutEventsCommandOutput } from "@aws-sdk/client-eventbridge"
import { Client } from "./client"

export interface EventBridgeProvider extends Provider {
  instanceProps: {
    publish: <
      T extends ModelInstance<string, any> & {
        source: string
        detailType: string
      }
    >(
      this: T
    ) => Promise<PutEventsCommandOutput["Entries"]>
  }
}

export const getProvider = (client: Client) => {
  const provider: EventBridgeProvider = {
    instanceProps: {
      async publish(this) {
        return client.publish(this)
      }
    }
  }
  return provider
}
