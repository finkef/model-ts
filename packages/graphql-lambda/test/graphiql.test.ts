import { runInNewContext } from "node:vm"
import { expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { vi } from "vitest"
import { createHttpHandler, type GraphiQLOptions } from "../src/index.ts"
import { httpEvent, options } from "./fixtures.ts"

const pageEvent = (accept = "text/html") =>
  httpEvent(null, {
    requestContext: { http: { method: "GET" } },
    headers: { accept },
  })

it.effect(
  "serves opt-in GraphiQL with CORS without context or GraphQL execution",
  () =>
    Effect.gen(function* () {
      const handler = createHttpHandler({
        ...options,
        graphiql: true,
        introspection: false,
        context: () => Effect.die("must not run"),
        cors: { origin: "https://app.example", credentials: true },
      })
      const response = yield* handler(
        {
          ...pageEvent("application/json, Text/HTML; charset=utf-8"),
          headers: {
            accept: "application/json, Text/HTML; charset=utf-8",
            origin: "https://app.example",
          },
        },
        {},
      )
      expect(response.statusCode).toBe(200)
      expect(response.isBase64Encoded).toBe(false)
      expect(response.headers?.["content-type"]).toBe(
        "text/html; charset=utf-8",
      )
      expect(response.headers?.["access-control-allow-origin"]).toBe(
        "https://app.example",
      )
      expect(response.body).toContain("https://esm.sh/graphiql@5.4.0")
      expect(response.body).toContain('import "graphiql/setup-workers/esm.sh"')
      expect(response.body).toContain('"endpoint":"/graphql"')
      expect(response.body).toContain('"credentials":"same-origin"')
      expect(response.body).toContain("<title>GraphiQL</title>")
    }),
)

it.effect(
  "leaves GraphQL and its CSRF boundary active when HTML is not requested or GraphiQL is disabled",
  () =>
    Effect.gen(function* () {
      for (const graphiql of [undefined, false]) {
        const handler = createHttpHandler({ ...options, graphiql })
        const response = yield* handler(pageEvent(), {})
        expect(response.statusCode).toBe(400)
        expect(response.headers?.["content-type"]).toBe("application/json")
      }
      const handler = createHttpHandler({ ...options, graphiql: true })
      for (const accept of [
        "application/json",
        "text/html;q=0",
        "text/html;q=0.0, application/json",
        "text/html-not-a-type",
        "*/*",
      ]) {
        expect((yield* handler(pageEvent(accept), {})).statusCode).toBe(400)
      }
      const query = yield* handler(
        {
          ...pageEvent("application/json"),
          headers: {
            accept: "application/json",
            "content-type": "application/json",
          },
          queryStringParameters: { query: "{ me }" },
        },
        {},
      )
      expect(JSON.parse(query.body)).toEqual({ data: { me: "guest" } })
      const post = yield* handler(
        httpEvent(
          { query: "{ me }" },
          {
            headers: {
              accept: "text/html",
              "content-type": "application/json",
            },
          },
        ),
        {},
      )
      expect(JSON.parse(post.body)).toEqual({ data: { me: "guest" } })
      expect(
        (yield* handler(
          httpEvent(null, { headers: { accept: "text/html" } }),
          {},
        )).statusCode,
      ).toBe(400)
    }),
)

it.effect(
  "escapes HTML and script contexts while preserving fetcher and editor options",
  () =>
    Effect.gen(function* () {
      const attack = '</script><script>alert("example")</script>&\u2028\u2029'
      const graphiql: GraphiQLOptions = {
        endpoint: `/graphql?example=${attack}`,
        subscriptionUrl: "wss://api.example.test/graphql",
        credentials: "include",
        headers: { authorization: attack },
        query: `query { echo(text: ${JSON.stringify(attack)}) }`,
        title: `</title><script>${attack}`,
        nonce: '" onload="alert(1)',
      }
      const handler = createHttpHandler({ ...options, graphiql })
      const { body } = yield* handler(pageEvent(), {})
      expect(body).not.toContain(attack)
      expect(body).not.toContain("</title><script>")
      expect(body).toContain("&lt;/title&gt;&lt;script&gt;")
      expect(body.match(/<script\b/g)).toHaveLength(2)
      expect(body.match(/<\/script>/g)).toHaveLength(2)
      expect(
        body.match(/nonce="&quot; onload=&quot;alert\(1\)"/g),
      ).toHaveLength(3)
      const script = body.match(
        /<script type="module"[^>]*>([\s\S]*?)<\/script>/,
      )?.[1]
      expect(script).toBeDefined()
      if (!script) throw new Error("Missing GraphiQL module")
      const createClient = vi.fn<
        (config: {
          url: string
          connectionParams?: Readonly<Record<string, string>>
        }) => string
      >(() => "ws-client")
      const createGraphiQLFetcher = vi.fn<
        (config: {
          url: string
          headers: Readonly<Record<string, string>>
          wsClient: string
          fetch: (url: string, init: { method: string }) => Promise<string>
        }) => string
      >(() => "fetcher")
      const createElement = vi.fn<
        (
          component: string,
          props: {
            fetcher: string
            initialQuery?: string
            initialHeaders?: string
            shouldPersistHeaders: boolean
          },
        ) => void
      >()
      const fetch = vi.fn<
        (
          url: string,
          init: {
            method: string
            credentials: GraphiQLOptions["credentials"]
          },
        ) => Promise<string>
      >(async () => "response")
      const render = vi.fn<(element: void) => void>()
      runInNewContext(script.replace(/^\s*import .+;\n/gm, ""), {
        React: { createElement },
        createRoot: () => ({ render }),
        GraphiQL: "GraphiQL",
        createGraphiQLFetcher,
        createClient,
        fetch,
        document: { getElementById: () => "container" },
      })
      expect(createClient).toHaveBeenCalledWith({
        url: graphiql.subscriptionUrl,
        connectionParams: graphiql.headers,
      })
      const config = createGraphiQLFetcher.mock.calls[0][0]
      expect(config.url).toBe(graphiql.endpoint)
      expect(config.headers).toEqual(graphiql.headers)
      expect(config.wsClient).toBe("ws-client")
      yield* Effect.promise(() => config.fetch("/graphql", { method: "POST" }))
      expect(fetch).toHaveBeenCalledWith("/graphql", {
        method: "POST",
        credentials: "include",
      })
      expect(createElement).toHaveBeenCalledWith("GraphiQL", {
        fetcher: "fetcher",
        initialQuery: graphiql.query,
        initialHeaders: JSON.stringify(graphiql.headers, null, 2),
        shouldPersistHeaders: false,
      })
      expect(render).toHaveBeenCalledOnce()
    }),
)

it.effect(
  "escapes the default request path and accepts an omit credentials policy",
  () =>
    Effect.gen(function* () {
      const handler = createHttpHandler({
        ...options,
        graphiql: { credentials: "omit" },
      })
      const response = yield* handler(
        { ...pageEvent(), rawPath: '/</script><script>alert("path")</script>' },
        {},
      )
      expect(response.body).not.toContain('<script>alert("path")')
      expect(response.body).toContain('"credentials":"omit"')
      expect(response.body).not.toContain(" nonce=")
    }),
)
