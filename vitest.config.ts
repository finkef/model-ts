import { fileURLToPath } from "node:url"
import { defineConfig } from "vitest/config"

const env = { TZ: "UTC" }

/**
 * Tests import `@model-ts/core` from its sources, so they never run against
 * a stale or missing build.
 */
const resolve = {
  alias: {
    "@model-ts/core": fileURLToPath(
      new URL("./packages/core/src/index.ts", import.meta.url),
    ),
  },
}

const graphqlResolve = {
  alias: {
    ...resolve.alias,
    "@model-ts/dynamodb/Sandbox": fileURLToPath(
      new URL("./packages/dynamodb/src/Sandbox.ts", import.meta.url),
    ),
    "@model-ts/dynamodb": fileURLToPath(
      new URL("./packages/dynamodb/src/index.ts", import.meta.url),
    ),
  },
}

/**
 * DynamoDB tests run twice: against the in-memory engine (dynamodb:memory)
 * and against DynamoDB Local (dynamodb:local). The project picks the table
 * the tests get; nothing reads an environment variable. GraphQL Sandbox
 * operation tests use the same selection in graphql:memory / graphql:local.
 */
export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: "graphql:types",
          root: "packages/graphql",
          include: [],
          typecheck: {
            enabled: true,
            include: ["test/**/*.test-d.ts"],
            tsconfig: "tsconfig.typecheck.json",
          },
          env,
        },
      },
      {
        resolve: graphqlResolve,
        test: {
          name: "graphql",
          exclude: ["test/testing.test.ts", "test/connections.test.ts"],
          root: "packages/graphql",
          include: ["test/**/*.test.ts"],
          env,
        },
      },
      {
        resolve: graphqlResolve,
        test: {
          name: "graphql:memory",
          root: "packages/graphql",
          include: ["test/testing.test.ts", "test/connections.test.ts"],
          provide: { dynamodb: "memory" },
          env,
        },
      },
      {
        resolve: graphqlResolve,
        test: {
          name: "graphql:local",
          root: "packages/graphql",
          include: ["test/testing.test.ts", "test/connections.test.ts"],
          provide: { dynamodb: "local" },
          testTimeout: 30_000,
          hookTimeout: 30_000,
          env,
        },
      },
      {
        test: {
          name: "core",
          root: "packages/core",
          include: ["test/**/*.test.ts"],
          env,
        },
      },
      {
        resolve,
        test: {
          name: "eventbridge",
          root: "packages/eventbridge",
          include: ["test/**/*.test.ts"],
          env,
        },
      },
      {
        resolve,
        test: {
          name: "dynamodb:memory",
          root: "packages/dynamodb",
          include: ["test/**/*.test.ts"],
          exclude: ["test/conformance.test.ts"],
          provide: { dynamodb: "memory" },
          env,
        },
      },
      {
        resolve,
        test: {
          name: "dynamodb:local",
          root: "packages/dynamodb",
          include: ["test/**/*.test.ts"],
          exclude: ["test/memory-spec.test.ts"],
          provide: { dynamodb: "local" },
          // Creating and deleting tables on the shared DynamoDB Local can be slow.
          testTimeout: 30_000,
          hookTimeout: 30_000,
          env,
        },
      },
    ],
  },
})
