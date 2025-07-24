import { ModelInstance, ModelConstructor, Union } from "@model-ts/core"
import { GSIPK, GSISK } from "./gsi"

export interface DynamoDBModelInstance extends ModelInstance<string, any> {
  /**
   * Returns the item's DynamoDB keys.
   */
  keys(): {
    PK: string
    SK: string
  } & { [key in GSIPK]?: string } &
    { [key in GSISK]?: string }

  PK: string
  SK: string
  GSI2PK?: string
  GSI2SK?: string
  GSI3PK?: string
  GSI3SK?: string
  GSI4PK?: string
  GSI4SK?: string
  GSI5PK?: string
  GSI5SK?: string
  GSI6PK?: string
  GSI6SK?: string
  GSI7PK?: string
  GSI7SK?: string
  GSI8PK?: string
  GSI8SK?: string
  GSI9PK?: string
  GSI9SK?: string
  GSI10PK?: string
  GSI10SK?: string
  GSI11PK?: string
  GSI11SK?: string
  GSI12PK?: string
  GSI12SK?: string
  GSI13PK?: string
  GSI13SK?: string
  GSI14PK?: string
  GSI14SK?: string
  GSI15PK?: string
  GSI15SK?: string
  GSI16PK?: string
  GSI16SK?: string
  GSI17PK?: string
  GSI17SK?: string
  GSI18PK?: string
  GSI18SK?: string
  GSI19PK?: string
  GSI19SK?: string
}

// export interface DynamoDBModel {}

export type DynamoDBModelConstructor<T extends DynamoDBModelInstance> =
  ModelConstructor<T>

export type DynamoDBUnion = Union<
  [
    DynamoDBModelConstructor<any>,
    DynamoDBModelConstructor<any>,
    ...DynamoDBModelConstructor<any>[]
  ]
>

export type Decodable = DynamoDBModelConstructor<any> | DynamoDBUnion

export type DecodableInstance<M extends Decodable> =
  M extends DynamoDBModelConstructor<any>
    ? InstanceType<M>
    : M extends DynamoDBUnion
    ? InstanceType<M["_models"][number]>
    : never
