import type { HttpEvent, StreamEvent, WebSocketEvent } from "../../Request.ts"

/** Invocation metadata supplied to a custom local Lambda context factory. */
export interface Invocation {
  readonly event: HttpEvent | WebSocketEvent | StreamEvent
  readonly context: LambdaContext
}

/** The useful, non-deprecated fields of an AWS Lambda Context. */
export interface LambdaContext {
  readonly awsRequestId: string
  readonly functionName: string
  readonly functionVersion: string
  readonly invokedFunctionArn: string
  readonly memoryLimitInMB: string
  readonly logGroupName: string
  readonly logStreamName: string
  readonly callbackWaitsForEmptyEventLoop: boolean
  readonly getRemainingTimeInMillis: () => number
}

export const invocation = (event: Invocation["event"]): Invocation => {
  const started = Date.now()
  return {
    event,
    context: {
      awsRequestId:
        "requestContext" in event
          ? (event.requestContext.requestId ?? crypto.randomUUID())
          : crypto.randomUUID(),
      functionName: "graphql-local",
      functionVersion: "$LATEST",
      invokedFunctionArn:
        "arn:aws:lambda:local:000000000000:function:graphql-local",
      memoryLimitInMB: "0",
      logGroupName: "/aws/lambda/graphql-local",
      logStreamName: "local",
      callbackWaitsForEmptyEventLoop: false,
      getRemainingTimeInMillis: () =>
        Math.max(0, 900_000 - (Date.now() - started)),
    },
  }
}

export type ContextOptions<L> = [LambdaContext] extends [L]
  ? { readonly lambdaContext?: (invocation: Invocation) => L }
  : { readonly lambdaContext: (invocation: Invocation) => L }

// The conditional options type permits omission only when LambdaContext is an L.
// This guard carries that relationship into the implementation without a cast.
function assertDefaultContext<L>(
  options: ContextOptions<L> | undefined,
  _context: LambdaContext,
): asserts _context is LambdaContext & L {
  if (options?.lambdaContext !== undefined)
    throw new Error("A custom Lambda context factory must be invoked")
}

export const invocationContext = <L>(
  options: ContextOptions<L> | undefined,
  call: Invocation,
): L => {
  if (options?.lambdaContext) return options.lambdaContext(call)
  assertDefaultContext<L>(options, call.context)
  return call.context
}
