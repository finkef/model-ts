/**
 * An AWS SDK request handler that answers DynamoDB calls from the in-memory
 * engine. The real client still builds, signs and serializes every request,
 * so code under test sends exactly what it would send to AWS.
 */
import type { DynamoDBClientConfig } from "@aws-sdk/client-dynamodb"
import { type Doc, toDoc, toWire } from "../wire.ts"
import type { MemoryEngine } from "./engine.ts"

/** Converts the attribute-value parts of a request to plain documents. */
const docParams = (params: Doc): Doc => ({
  ...params,
  ...(params.Item && { Item: toDoc(params.Item) }),
  ...(params.Key && { Key: toDoc(params.Key) }),
  ...(params.ExclusiveStartKey && {
    ExclusiveStartKey: toDoc(params.ExclusiveStartKey),
  }),
  ...(params.ExpressionAttributeValues && {
    ExpressionAttributeValues: toDoc(params.ExpressionAttributeValues),
  }),
})

const page = (output: {
  Items?: Array<Doc> | undefined
  Count: number
  ScannedCount: number
  LastEvaluatedKey?: Doc | undefined
}) => ({
  Items: (output.Items ?? []).map(toWire),
  Count: output.Count,
  ScannedCount: output.ScannedCount,
  ...(output.LastEvaluatedKey && {
    LastEvaluatedKey: toWire(output.LastEvaluatedKey),
  }),
})

const dispatch = async (
  engine: MemoryEngine,
  operation: string,
  body: Doc,
): Promise<Doc> => {
  switch (operation) {
    case "GetItem": {
      const { Item } = await engine.get(docParams(body))
      return Item ? { Item: toWire(Item) } : {}
    }
    case "PutItem":
      await engine.put(docParams(body))
      return {}
    case "DeleteItem":
      await engine.delete(docParams(body))
      return {}
    case "UpdateItem": {
      const { Attributes } = await engine.update(docParams(body))
      return Attributes ? { Attributes: toWire(Attributes) } : {}
    }
    case "Query":
      return page(await engine.query(docParams(body)))
    case "Scan":
      return page(await engine.scan(docParams(body)))
    case "BatchGetItem": {
      const RequestItems = Object.fromEntries(
        Object.entries(body.RequestItems as Doc).map(([table, request]) => [
          table,
          { ...request, Keys: request.Keys.map(toDoc) },
        ]),
      )
      const { Responses = {} } = await engine.batchGet({
        ...body,
        RequestItems,
      })
      return {
        Responses: Object.fromEntries(
          Object.entries(Responses).map(([table, items]) => [
            table,
            items.map(toWire),
          ]),
        ),
        UnprocessedKeys: {},
      }
    }
    case "BatchWriteItem": {
      const RequestItems = Object.fromEntries(
        Object.entries(body.RequestItems as Doc).map(([table, requests]) => [
          table,
          (requests as Array<Doc>).map((request) =>
            request.PutRequest
              ? { PutRequest: { Item: toDoc(request.PutRequest.Item) } }
              : request.DeleteRequest
                ? { DeleteRequest: { Key: toDoc(request.DeleteRequest.Key) } }
                : request,
          ),
        ]),
      )
      await engine.batchWrite({ ...body, RequestItems })
      return { UnprocessedItems: {} }
    }
    case "TransactWriteItems": {
      const TransactItems = (body.TransactItems as Array<Doc>).map((item) =>
        Object.fromEntries(
          Object.entries(item).map(([kind, params]) => [
            kind,
            docParams(params),
          ]),
        ),
      )
      await engine.transactWrite({ ...body, TransactItems })
      return {}
    }
    default:
      throw Object.assign(
        new Error(`${operation} is not supported in in-memory mode.`),
        {
          code: "NotSupportedError",
          method: operation,
          featurePath: operation,
        },
      )
  }
}

/** The error body DynamoDB would send; extra fields reach the SDK exception. */
const errorBody = (error: unknown) => {
  const {
    code = "InternalServerError",
    message = String(error),
    ...rest
  } = error as { code?: string; message?: string; [key: string]: unknown }
  const { name: _name, stack: _stack, ...fields } = rest
  return {
    __type: `com.amazonaws.dynamodb.v20120810#${code}`,
    message,
    ...fields,
  }
}

const response = (statusCode: number, body: unknown) => ({
  statusCode,
  reason: statusCode === 200 ? "OK" : "Bad Request",
  headers: { "content-type": "application/x-amz-json-1.0" },
  body: new TextEncoder().encode(JSON.stringify(body)),
})

/** Client options that route every request to `engine`. */
export const memoryClientConfig = (
  engine: MemoryEngine,
): DynamoDBClientConfig => ({
  region: "local",
  credentials: { accessKeyId: "memory", secretAccessKey: "memory" },
  endpoint: "http://dynamodb.memory",
  maxAttempts: 1,
  requestHandler: {
    metadata: { handlerProtocol: "memory" },
    destroy: () => {},
    handle: async (request: {
      readonly headers: Record<string, string>
      readonly body?: unknown
    }) => {
      const target = request.headers["x-amz-target"] ?? ""
      const operation = target.slice(target.indexOf(".") + 1)
      const body = JSON.parse(bodyText(request.body))
      try {
        return {
          response: response(200, await dispatch(engine, operation, body)),
        }
      } catch (error) {
        return { response: response(400, errorBody(error)) }
      }
    },
  } as unknown as DynamoDBClientConfig["requestHandler"],
})

const bodyText = (body: unknown) =>
  body instanceof Uint8Array
    ? new TextDecoder().decode(body)
    : typeof body === "string"
      ? body
      : "{}"
