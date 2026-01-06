import mockdate from "mockdate"

const isSnapshotDiff = (value: unknown): value is string => {
  if (typeof value !== "string") return false
  if (!value.includes(" / ") || !value.includes("[")) return false
  return /^(?:\+ |- |\[)/m.test(value)
}

expect.addSnapshotSerializer({
  test: isSnapshotDiff,
  print: (value) => String(value),
})

mockdate.set(new Date("2021-05-01T08:00:00.000Z"))
