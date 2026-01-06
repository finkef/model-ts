import { formatSnapshotDiff } from "../diff"

describe("formatSnapshotDiff", () => {
  test("renders added items with nested objects and arrays", () => {
    const before = {}
    const after = {
      "PK#user#1__SK#profile": {
        PK: "PK#user#1",
        SK: "SK#profile",
        name: "Ada",
        stats: {
          flags: ["a", "b"],
          logins: 3
        },
        tags: ["alpha", "beta"],
        connections: [
          {
            user: "1"
          },
          {
            user: "2",
            type: "friend"
          }
        ]
      }
    }

    expect(formatSnapshotDiff(before, after)).toMatchInlineSnapshot(`
+ [PK#user#1 / SK#profile]
+   PK: "PK#user#1"
+   SK: "SK#profile"
+   connections:
+     - user: "1"
+     - type: "friend"
+       user: "2"
+   name: "Ada"
+   stats:
+     flags:
+       - "a"
+       - "b"
+     logins: 3
+   tags:
+     - "alpha"
+     - "beta"
`)
  })

  test("renders removed items", () => {
    const before = {
      "PK#post#9__SK#meta": {
        PK: "PK#post#9",
        SK: "SK#meta",
        title: "Removed",
        views: 10
      }
    }
    const after = {}

    expect(formatSnapshotDiff(before, after)).toMatchInlineSnapshot(`
      - [PK#post#9 / SK#meta]
      -   PK: "PK#post#9"
      -   SK: "SK#meta"
      -   title: "Removed"
      -   views: 10
    `)
  })

  test("renders updated fields with nested diffs", () => {
    const before = {
      "PK#order#1__SK#summary": {
        PK: "PK#order#1",
        SK: "SK#summary",
        status: "pending",
        count: 1,
        meta: {
          flags: ["a", "b", "c"],
          stats: ["a", "b", "c"],
          config: { enabled: true }
        }
      }
    }
    const after = {
      "PK#order#1__SK#summary": {
        PK: "PK#order#1",
        SK: "SK#summary",
        status: "paid",
        count: 2,
        meta: {
          flags: ["a", "c", "d"],
          stats: ["a", "c"],
          config: { enabled: false, mode: "fast" }
        }
      }
    }

    expect(formatSnapshotDiff(before, after)).toMatchInlineSnapshot(`
[PK#order#1 / SK#summary]
    PK: "PK#order#1"
    SK: "SK#summary"
-   count: 1
+   count: 2
    meta:
      config:
-       enabled: true
+       enabled: false
+       mode: "fast"
      flags:
        - "a"
-       - "b"
        - "c"
+       - "d"
      stats:
        - "a"
-       - "b"
        - "c"
-   status: "pending"
+   status: "paid"
`)
  })

  test("skips unchanged items and keeps output compact", () => {
    const before = {
      "PK#steady__SK#1": {
        PK: "PK#steady",
        SK: "SK#1",
        value: "same"
      },
      "PK#beta__SK#1": {
        PK: "PK#beta",
        SK: "SK#1",
        count: 1
      }
    }
    const after = {
      "PK#steady__SK#1": {
        PK: "PK#steady",
        SK: "SK#1",
        value: "same"
      },
      "PK#beta__SK#1": {
        PK: "PK#beta",
        SK: "SK#1",
        count: 2
      },
      "PK#alpha__SK#1": {
        PK: "PK#alpha",
        SK: "SK#1",
        flag: true
      }
    }

    expect(formatSnapshotDiff(before, after)).toMatchInlineSnapshot(`
      + [PK#alpha / SK#1]
      +   PK: "PK#alpha"
      +   SK: "SK#1"
      +   flag: true

      [PK#beta / SK#1]
          PK: "PK#beta"
          SK: "SK#1"
      -   count: 1
      +   count: 2
    `)
  })
})
