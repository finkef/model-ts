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

/**
 * DynamoDB tests run twice: against the in-memory engine (dynamodb:memory)
 * and against DynamoDB Local (dynamodb:local). The project picks the table
 * the tests get; nothing reads an environment variable.
 */
export default defineConfig({
  test: {
    projects: [
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
          // Every test creates a table on DynamoDB Local, which takes a while.
          testTimeout: 30_000,
          env,
        },
      },
    ],
  },
})
