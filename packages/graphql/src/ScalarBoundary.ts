import { GraphQLScalarType, specifiedScalarTypes } from "graphql"

/** Retain callback provenance even when graphql-js forwards a thrown GraphQLError unchanged. */
export class ScalarInputFailure extends Error {
  constructor(readonly original: unknown) {
    super("Scalar input failed")
  }
}

const guarded = new WeakSet<GraphQLScalarType>()

/** Clone adopted scalars so validation cannot mistake application exceptions for document diagnostics. */
export function guardScalar(type: GraphQLScalarType): GraphQLScalarType {
  if (specifiedScalarTypes.includes(type) || guarded.has(type)) return type
  const input = <A>(evaluate: () => A): A => {
    try {
      return evaluate()
    } catch (error) {
      throw new ScalarInputFailure(error)
    }
  }
  const result = new GraphQLScalarType({
    ...type.toConfig(),
    parseValue: (value) => input(() => type.parseValue(value)),
    parseLiteral: (node, variables) =>
      input(() => type.parseLiteral(node, variables)),
  })
  guarded.add(result)
  return result
}
