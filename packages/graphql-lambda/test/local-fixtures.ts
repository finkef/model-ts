import { createServer, request } from "node:http"
import { NodeHttpServer } from "@effect/platform-node"
import { Deferred, Effect, Queue, Schema } from "effect"
import { HttpServer } from "effect/http"

export const server = NodeHttpServer.layer(createServer, {
  port: 0,
  host: "127.0.0.1",
  websocket: {
    handleProtocols: (protocols) =>
      protocols.has("graphql-transport-ws") ? "graphql-transport-ws" : false,
  },
})

export const origin = Effect.gen(function* () {
  const { address } = yield* HttpServer.HttpServer
  if (address._tag === "UnixPathAddress")
    return yield* Effect.die("Expected a TCP server")
  return `http://127.0.0.1:${address.port}`
})

export const openSocket = Effect.fn(function* (path = "/graphql") {
  const url = `${yield* origin}${path}`.replace("http:", "ws:")
  const messages = yield* Queue.unbounded<string>()
  const closed = yield* Deferred.make<number>()
  const opened = yield* Deferred.make<void, Error>()
  const socket = yield* Effect.acquireRelease(
    Effect.sync(() => {
      const socket = new WebSocket(url, ["other", "graphql-transport-ws"])
      socket.addEventListener("open", () =>
        Deferred.doneUnsafe(opened, Effect.void),
      )
      socket.addEventListener("error", () =>
        Deferred.doneUnsafe(
          opened,
          Effect.fail(new Error("WebSocket handshake failed")),
        ),
      )
      socket.addEventListener("message", (event) =>
        Queue.offerUnsafe(
          messages,
          Schema.decodeUnknownSync(Schema.String)(event.data),
        ),
      )
      socket.addEventListener("close", (event) =>
        Deferred.doneUnsafe(closed, Effect.succeed(event.code)),
      )
      return socket
    }),
    (socket) =>
      Effect.sync(() => {
        socket.close()
      }),
  )
  yield* Deferred.await(opened)
  return {
    protocol: socket.protocol,
    close: () => socket.close(),
    next: Queue.take(messages).pipe(
      Effect.map((data): Schema.Json =>
        Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Json))(data),
      ),
    ),
    closed: Deferred.await(closed),
    send: (message: Schema.Json) =>
      Effect.sync(() => socket.send(JSON.stringify(message))),
  }
})

/** A real upgrade request, retaining the HTTP rejection status. */
export const rejectedHandshake = Effect.fn(function* () {
  const url = `${yield* origin}/graphql`
  return yield* Effect.callback<
    { status: number | undefined; upgraded: boolean },
    Error
  >((resume) => {
    const req = request(url, {
      headers: {
        connection: "Upgrade",
        upgrade: "websocket",
        "sec-websocket-version": "13",
        "sec-websocket-key": "dGhlIHNhbXBsZSBub25jZQ==",
        "sec-websocket-protocol": "graphql-transport-ws",
        cookie: "session=handshake",
      },
    })
    req.on("response", (response) => {
      response.resume()
      resume(Effect.succeed({ status: response.statusCode, upgraded: false }))
    })
    req.on("upgrade", (response, socket) => {
      socket.destroy()
      resume(Effect.succeed({ status: response.statusCode, upgraded: true }))
    })
    req.on("error", (error) => resume(Effect.fail(error)))
    req.end()
    return Effect.sync(() => {
      req.destroy()
    })
  })
})
