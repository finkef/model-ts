import { defineConfig } from "vitest/config"

const env = { TZ: "UTC" }

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
        test: {
          name: "eventbridge",
          root: "packages/eventbridge",
          include: ["test/**/*.test.ts"],
          env,
        },
      },
      {
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
        test: {
          name: "dynamodb:local",
          root: "packages/dynamodb",
          include: ["test/**/*.test.ts"],
          exclude: ["test/memory-spec.test.ts"],
          provide: { dynamodb: "local" },
          env,
        },
      },
    ],
  },
})
