import type { GraphiQLOptions } from "../GraphiQL.ts"

const escapeHtml = (value: string) =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;")

export const graphiqlHtml = (
  options: GraphiQLOptions,
  path: string,
): string => {
  // JSON escaping alone does not prevent the HTML parser from ending a script.
  const config = JSON.stringify({
    endpoint: options.endpoint ?? path,
    credentials: options.credentials ?? "same-origin",
    subscriptionUrl: options.subscriptionUrl,
    headers: options.headers,
    query: options.query,
  })
    .replaceAll("<", "\\u003c")
    .replaceAll(">", "\\u003e")
    .replaceAll("&", "\\u0026")
    .replaceAll("\u2028", "\\u2028")
    .replaceAll("\u2029", "\\u2029")
  const nonce =
    options.nonce === undefined ? "" : ` nonce="${escapeHtml(options.nonce)}"`
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${escapeHtml(options.title ?? "GraphiQL")}</title>
  <link rel="stylesheet" href="https://esm.sh/graphiql@5.4.0/dist/style.css">
  <style${nonce}>body { margin: 0; } #graphiql { height: 100dvh; }</style>
  <script type="importmap"${nonce}>
    {"imports": {
      "react": "https://esm.sh/react@19.3.1",
      "react/": "https://esm.sh/react@19.3.1/",
      "react-dom": "https://esm.sh/react-dom@19.3.1",
      "react-dom/": "https://esm.sh/react-dom@19.3.1/",
      "graphiql": "https://esm.sh/graphiql@5.4.0?standalone&external=react,react-dom,@graphiql/react,graphql",
      "graphiql/": "https://esm.sh/graphiql@5.4.0/",
      "@graphiql/react": "https://esm.sh/@graphiql/react@0.39.1?standalone&external=react,react-dom,graphql,@graphiql/toolkit,@emotion/is-prop-valid",
      "@graphiql/toolkit": "https://esm.sh/@graphiql/toolkit@0.12.1?standalone&external=graphql,graphql-ws",
      "graphql": "https://esm.sh/graphql@16.11.0",
      "graphql-ws": "https://esm.sh/graphql-ws@6.4.1/client",
      "@emotion/is-prop-valid": "data:text/javascript,"
    }}
  </script>
</head>
<body>
  <div id="graphiql">Loading GraphiQL…</div>
  <script type="module"${nonce}>
    import React from "react";
    import { createRoot } from "react-dom/client";
    import { GraphiQL } from "graphiql";
    import { createGraphiQLFetcher } from "@graphiql/toolkit";
    import { createClient } from "graphql-ws";
    import "graphiql/setup-workers/esm.sh";

    const config = ${config};
    const wsClient = config.subscriptionUrl ? createClient({
      url: config.subscriptionUrl,
      connectionParams: config.headers,
    }) : undefined;
    const fetcher = createGraphiQLFetcher({
      url: config.endpoint,
      headers: config.headers,
      wsClient,
      fetch: (url, init) => fetch(url, { ...init, credentials: config.credentials }),
    });
    createRoot(document.getElementById("graphiql")).render(
      React.createElement(GraphiQL, {
        fetcher,
        initialQuery: config.query,
        initialHeaders: config.headers ? JSON.stringify(config.headers, null, 2) : undefined,
        shouldPersistHeaders: false,
      }),
    );
  </script>
</body>
</html>`
}
