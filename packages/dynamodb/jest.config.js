module.exports = {
  preset: "ts-jest",
  testMatch: ["**/*.test.ts"],
  transform: {
    "^.+\\.[tj]s$": [
      "ts-jest",
      { tsconfig: "<rootDir>/tsconfig.test.json" },
    ],
  },
  transformIgnorePatterns: [
    "/node_modules/(?!(@aws-sdk|@smithy|effect)/)",
    "packages/core/dist/",
  ],
  setupFilesAfterEnv: ["./src/test-utils/setup.ts"],
}
