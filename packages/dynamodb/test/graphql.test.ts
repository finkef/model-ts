/**
 * GraphQL resolvers each run their own Effect, the way an Apollo resolver
 * calls `runtime.runPromise`. Loads issued on one level of a query must
 * still share one BatchGetItem call, as they did with DataLoader, so a nested
 * query costs one round trip per level.
 */
import { afterAll, beforeAll, describe, expect, it } from "@effect/vitest"
import { Effect, ManagedRuntime, Tracer } from "effect"
import { buildSchema, graphql } from "graphql"
import type { DynamoDB } from "../src/index.ts"
import { Sandbox } from "../src/Sandbox.ts"
import { makePost, Post, PostId, Slug } from "./fixtures.ts"
import { testTable } from "./utils.ts"

const schema = buildSchema(`
  type Query {
    slugs(names: [String!]!): [Slug!]!
  }
  type Slug {
    slug: String!
    post: Post!
  }
  type Post {
    id: String!
    title: String!
    next: Post
  }
`)

const runtime = ManagedRuntime.make(testTable())
const spans: Array<Tracer.NativeSpan> = []
const tracer = Tracer.make({
  span(options) {
    const span = new Tracer.NativeSpan(options)
    spans.push(span)
    return span
  },
})

const run = <A, E>(effect: Effect.Effect<A, E, Sandbox | DynamoDB>) =>
  runtime.runPromise(effect.pipe(Effect.withTracer(tracer)))

const nextKey = (post: Post) =>
  Post.key(
    PostId.make(
      `post_${String(Number(post.id.slice(5)) + 1).padStart(3, "0")}`,
    ),
  )

const postNode = (post: Post) => ({
  id: post.id,
  title: post.title,
  next: () =>
    run(
      Post.load(nextKey(post)).pipe(
        Effect.catchTag("ItemNotFoundError", () => Effect.succeed(null)),
      ),
    ).then((next) => next && postNode(next)),
})

const slugNode = (slug: Slug) => ({
  slug: slug.slug,
  post: () => run(Post.load(Post.key(slug.postId))).then(postNode),
})

const rootValue = {
  slugs: ({ names }: { names: ReadonlyArray<string> }) =>
    Promise.all(
      names.map((name) =>
        run(Slug.load({ PK: `SLUG#${name}`, SK: "SLUG" })).then(slugNode),
      ),
    ),
}

describe("GraphQL", () => {
  beforeAll(() =>
    run(
      Effect.gen(function* () {
        const sandbox = yield* Sandbox
        const posts = [1, 2, 3, 4, 5, 6].map((n) => makePost(n))
        yield* sandbox.seed(
          ...posts,
          ...posts
            .slice(0, 5)
            .map((post, i) => new Slug({ slug: `s${i + 1}`, postId: post.id })),
        )
      }),
    ),
  )
  afterAll(() => runtime.dispose())

  it("loads each level of a nested query with one BatchGetItem", async () => {
    spans.length = 0
    const result = await graphql({
      schema,
      rootValue,
      source: `{
        slugs(names: ["s1", "s2", "s3", "s4", "s5"]) {
          slug
          post { title next { title next { title } } }
        }
      }`,
    })

    // Post 7 does not exist, so the last post's next is null.
    expect(result).toEqual({
      data: {
        slugs: [1, 2, 3, 4, 5].map((n) => ({
          slug: `s${n}`,
          post: {
            title: `Post ${n}`,
            next: {
              title: `Post ${n + 1}`,
              next: n + 2 <= 6 ? { title: `Post ${n + 2}` } : null,
            },
          },
        })),
      },
    })

    const calls = spans.filter(
      (span) => span.name.startsWith("DynamoDB.") && span.kind === "client",
    )
    expect(calls.map((span) => span.name)).toEqual(
      Array(4).fill("DynamoDB.BatchGetItem"),
    )
    expect(
      calls.map((span) => span.attributes.get("aws.dynamodb.keys_count")),
    ).toEqual([5, 5, 5, 5])
  })
})
