module.exports = {
  testMatch: ["**/*.test.ts"],
  transform: {
    "^.+\\.[tj]s$": ["ts-jest", { tsconfig: { allowJs: true } }],
  },
  transformIgnorePatterns: ["/node_modules/(?!(effect)/)"],
}
