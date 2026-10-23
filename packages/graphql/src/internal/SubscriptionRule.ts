import {
  GraphQLError,
  Kind,
  isAbstractType,
  typeFromAST,
  type FieldNode,
  type FragmentDefinitionNode,
  type InlineFragmentNode,
  type DirectiveNode,
  type SelectionSetNode,
  type ValidationRule,
} from "graphql"

/**
 * Replaces graphql-js's SingleFieldSubscriptionsRule, which evaluates root
 * skip/include without variables and throws for a variable condition. Collect
 * root response keys through fragments and reject root conditionals instead.
 */
export const singleRootFieldSubscriptionsRule: ValidationRule = (context) => ({
  OperationDefinition(operation) {
    if (operation.operation !== "subscription") return
    const schema = context.getSchema()
    const root = schema.getSubscriptionType()
    if (!root) return
    const fields = new Map<string, FieldNode[]>()
    const visited = new Set<string>()
    const forbidden: DirectiveNode[] = []
    const matches = (fragment: FragmentDefinitionNode | InlineFragmentNode) => {
      if (!fragment.typeCondition) return true
      const type = typeFromAST(schema, fragment.typeCondition)
      return (
        type === root || (isAbstractType(type) && schema.isSubType(type, root))
      )
    }
    const collect = (selectionSet: SelectionSetNode): void => {
      for (const selection of selectionSet.selections) {
        // Validation cannot evaluate variables. Like GraphQL 17, root selections are
        // unconditional and skip/include are forbidden; nested fields are not visited.
        for (const directive of selection.directives ?? [])
          if (
            directive.name.value === "skip" ||
            directive.name.value === "include"
          )
            forbidden.push(directive)
        if (selection.kind === Kind.FIELD) {
          const key = selection.alias?.value ?? selection.name.value
          const group = fields.get(key)
          if (group) group.push(selection)
          else fields.set(key, [selection])
        } else if (selection.kind === Kind.INLINE_FRAGMENT) {
          if (matches(selection)) collect(selection.selectionSet)
        } else {
          const name = selection.name.value
          if (visited.has(name)) continue
          visited.add(name)
          const fragment = context.getFragment(name)
          // Standard validation owns missing/cyclic fragments; this traversal must terminate.
          if (fragment && matches(fragment)) collect(fragment.selectionSet)
        }
      }
    }
    collect(operation.selectionSet)
    const name = operation.name
      ? `Subscription "${operation.name.value}"`
      : "Anonymous Subscription"
    if (forbidden.length > 0) {
      context.reportError(
        new GraphQLError(
          `${name} must not use \`@skip\` or \`@include\` directives in the top level selection.`,
          { nodes: forbidden },
        ),
      )
      return
    }
    if (fields.size > 1)
      context.reportError(
        new GraphQLError(`${name} must select only one top level field.`, {
          nodes: [...fields.values()].slice(1).flat(),
        }),
      )
    for (const nodes of fields.values())
      if (nodes[0]!.name.value.startsWith("__"))
        context.reportError(
          new GraphQLError(
            `${name} must not select an introspection top level field.`,
            { nodes },
          ),
        )
  },
})
