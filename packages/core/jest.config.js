module.exports = {
  testMatch: ["**/*.test.ts"],
  transform: {
    "^.+\\.[tj]s$": [
      "ts-jest",
      { tsconfig: "<rootDir>/tsconfig.test.json" },
    ],
  },
  transformIgnorePatterns: ["/node_modules/(?!(effect)/)"],
}
