import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient,
  DynamoDBClientConfig,
  waitUntilTableExists,
  waitUntilTableNotExists,
  CreateTableCommandInput,
  CreateTableCommandOutput,
  DeleteTableCommandInput,
  DeleteTableCommandOutput,
} from "@aws-sdk/client-dynamodb"
import {
  BatchGetCommand,
  BatchGetCommandInput,
  BatchGetCommandOutput,
  BatchWriteCommand,
  BatchWriteCommandInput,
  BatchWriteCommandOutput,
  DeleteCommand,
  DeleteCommandInput,
  DeleteCommandOutput,
  DynamoDBDocumentClient,
  GetCommand,
  GetCommandInput,
  GetCommandOutput,
  PutCommand,
  PutCommandInput,
  PutCommandOutput,
  QueryCommand,
  QueryCommandInput,
  QueryCommandOutput,
  ScanCommand,
  ScanCommandInput,
  ScanCommandOutput,
  TransactWriteCommand,
  TransactWriteCommandInput,
  TransactWriteCommandOutput,
  UpdateCommand,
  UpdateCommandInput,
  UpdateCommandOutput,
} from "@aws-sdk/lib-dynamodb"

interface PromiseRequest<T> {
  promise: () => Promise<T>
}

export interface AWSError extends Error {
  code: string
  [key: string]: any
}

export interface ServiceConfigurationOptions extends DynamoDBClientConfig {
  accessKeyId?: string
  secretAccessKey?: string
  sessionToken?: string
}

export interface ClientApiVersions {}

interface DocumentTranslationOptions {
  convertEmptyValues?: boolean
  wrapNumbers?: boolean
  marshallOptions?: { [key: string]: unknown }
  unmarshallOptions?: { [key: string]: unknown }
}

const withoutUndefined = <T extends { [key: string]: any }>(value: T): T =>
  Object.fromEntries(
    Object.entries(value).filter(([, entry]) => typeof entry !== "undefined")
  ) as T

const normalizeClientConfig = (
  config: (ServiceConfigurationOptions & DocumentTranslationOptions) | undefined
) => {
  const {
    accessKeyId,
    secretAccessKey,
    sessionToken,
    convertEmptyValues,
    wrapNumbers,
    marshallOptions,
    unmarshallOptions,
    ...clientConfig
  } = config ?? {}

  const normalizedClientConfig = withoutUndefined({
    ...clientConfig,
  }) as DynamoDBClientConfig

  if (!normalizedClientConfig.credentials && accessKeyId && secretAccessKey) {
    normalizedClientConfig.credentials = withoutUndefined({
      accessKeyId,
      secretAccessKey,
      sessionToken,
    })
  }

  return {
    clientConfig: normalizedClientConfig,
    translateConfig: {
      marshallOptions: withoutUndefined({
        convertEmptyValues,
        removeUndefinedValues: true,
        ...(marshallOptions ?? {}),
      }),
      unmarshallOptions: withoutUndefined({
        wrapNumbers,
        ...(unmarshallOptions ?? {}),
      }),
    },
  }
}

const normalizeOutput = <T>(output: T): T => {
  if (!output || typeof output !== "object") return output

  const { $metadata, ...rest } = output as any
  return rest as T
}

const normalizeError = (error: unknown): AWSError => {
  if (error && typeof error === "object") {
    const candidate = error as AWSError
    if (!candidate.code && typeof candidate.name === "string") {
      candidate.code = candidate.name
    }
    return candidate
  }

  const wrapped = new Error(String(error)) as AWSError
  wrapped.code = "UnknownError"
  return wrapped
}

const request = <T>(fn: () => Promise<T>): PromiseRequest<T> => ({
  promise: async () => {
    try {
      return normalizeOutput(await fn())
    } catch (error) {
      throw normalizeError(error)
    }
  },
})

export class DocumentClient {
  private readonly client: DynamoDBDocumentClient

  constructor(config?: DocumentClient.DocumentClientOptions) {
    const { clientConfig, translateConfig } = normalizeClientConfig(config)
    this.client = DynamoDBDocumentClient.from(
      new DynamoDBClient(clientConfig),
      translateConfig
    )
  }

  get(params: DocumentClient.GetItemInput) {
    return request<DocumentClient.GetItemOutput>(() =>
      this.client.send(new GetCommand(params))
    )
  }

  put(params: DocumentClient.PutItemInput) {
    return request<DocumentClient.PutItemOutput>(() =>
      this.client.send(new PutCommand(params))
    )
  }

  update(params: DocumentClient.UpdateItemInput) {
    return request<DocumentClient.UpdateItemOutput>(() =>
      this.client.send(new UpdateCommand(params))
    )
  }

  delete(params: DocumentClient.DeleteItemInput) {
    return request<DocumentClient.DeleteItemOutput>(() =>
      this.client.send(new DeleteCommand(params))
    )
  }

  query(params: DocumentClient.QueryInput) {
    return request<DocumentClient.QueryOutput>(() =>
      this.client.send(new QueryCommand(params))
    )
  }

  scan(params: DocumentClient.ScanInput) {
    return request<DocumentClient.ScanOutput>(() =>
      this.client.send(new ScanCommand(params))
    )
  }

  batchGet(params: DocumentClient.BatchGetItemInput) {
    return request<DocumentClient.BatchGetItemOutput>(() =>
      this.client.send(new BatchGetCommand(params))
    )
  }

  batchWrite(params: DocumentClient.BatchWriteItemInput) {
    return request<DocumentClient.BatchWriteItemOutput>(async () => ({
      UnprocessedItems: {},
      ...(await this.client.send(new BatchWriteCommand(params))),
    }))
  }

  transactWrite(params: DocumentClient.TransactWriteItemsInput) {
    return request<DocumentClient.TransactWriteItemsOutput>(() =>
      this.client.send(new TransactWriteCommand(params))
    )
  }
}

export namespace DocumentClient {
  export interface DocumentClientOptions
    extends ServiceConfigurationOptions,
      DocumentTranslationOptions {}

  export type Key = { [key: string]: any }
  export type GetItemInput = GetCommandInput
  export type GetItemOutput = GetCommandOutput
  export type PutItemInput = PutCommandInput
  export type PutItemOutput = PutCommandOutput
  export type UpdateItemInput = UpdateCommandInput
  export type UpdateItemOutput = UpdateCommandOutput
  export type DeleteItemInput = DeleteCommandInput
  export type DeleteItemOutput = DeleteCommandOutput
  export type QueryInput = QueryCommandInput
  export type QueryOutput = QueryCommandOutput
  export type ScanInput = ScanCommandInput
  export type ScanOutput = ScanCommandOutput
  export type BatchGetItemInput = BatchGetCommandInput
  export type BatchGetItemOutput = BatchGetCommandOutput
  export type BatchWriteItemInput = BatchWriteCommandInput
  export type BatchWriteItemOutput = BatchWriteCommandOutput
  export type TransactWriteItemsInput = TransactWriteCommandInput
  export type TransactWriteItemsOutput = TransactWriteCommandOutput
  export type TransactWriteItem = NonNullable<
    TransactWriteCommandInput["TransactItems"]
  >[number]
  export type ConditionCheck = NonNullable<
    TransactWriteItem["ConditionCheck"]
  >
}

export class DynamoDB {
  private readonly client: DynamoDBClient

  constructor(config?: ServiceConfigurationOptions) {
    const { clientConfig } = normalizeClientConfig(config)
    this.client = new DynamoDBClient(clientConfig)
  }

  createTable(params: DynamoDB.CreateTableInput) {
    return request<DynamoDB.CreateTableOutput>(() =>
      this.client.send(new CreateTableCommand(params))
    )
  }

  deleteTable(params: DynamoDB.DeleteTableInput) {
    return request<DynamoDB.DeleteTableOutput>(() =>
      this.client.send(new DeleteTableCommand(params))
    )
  }

  waitFor(state: DynamoDB.WaiterState, params: DynamoDB.WaiterInput) {
    return request(async () => {
      switch (state) {
        case "tableExists":
          await waitUntilTableExists(
            { client: this.client, maxWaitTime: 60, minDelay: 1 },
            params
          )
          break
        case "tableNotExists":
          await waitUntilTableNotExists(
            { client: this.client, maxWaitTime: 60, minDelay: 1 },
            params
          )
          break
      }

      return {}
    })
  }
}

export namespace DynamoDB {
  export type CreateTableInput = CreateTableCommandInput
  export type CreateTableOutput = CreateTableCommandOutput
  export type DeleteTableInput = DeleteTableCommandInput
  export type DeleteTableOutput = DeleteTableCommandOutput
  export type WaiterInput = { TableName: string }
  export type WaiterState = "tableExists" | "tableNotExists"
}

export default DynamoDB
