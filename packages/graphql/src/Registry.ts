import type { Schema } from "effect"
import { type PageInfo, type Edge, type Connection } from "./Connections.ts"
import { type FieldMap, type Contextual } from "./FieldTypes.ts"

export interface Scalars {
  ID: string
  String: string
  Boolean: boolean
  Int: number
  Float: number
  DateTime: Date | string
  JSON: unknown
}

export type DefinitionKind =
  | "object"
  | "interface"
  | "union"
  | "scalar"
  | "enum"

export interface Registration<K extends DefinitionKind, A> {
  readonly kind: K
  readonly model: A
}

export type Model = { readonly Type: unknown }

export type Tag<M extends Model> = M["Type"] extends {
  readonly _tag: infer N extends string
}
  ? N
  : never

export type InferKind<M extends Model> =
  M extends Schema.Literals<readonly string[]>
    ? "enum"
    : M extends Schema.Union<readonly Schema.Top[]>
      ? "union"
      : M["Type"] extends string | number | boolean | Date
        ? "scalar"
        : "object"
/** Tagged models need no name; non-object abstract types specify their kind. */

export type Register<
  M extends Model,
  N extends string = Tag<M>,
  K extends DefinitionKind = InferKind<M>,
> = {
  readonly [Name in N]: Registration<K, M["Type"]>
}

export type NamesOfKind<Types, Kind extends DefinitionKind> = Extract<
  {
    [N in keyof Types]: Types[N] extends Registration<Kind, unknown> ? N : never
  }[keyof Types],
  string
>

export type ModelAt<Types, N> = N extends keyof Types
  ? Types[N] extends Registration<DefinitionKind, infer A>
    ? A
    : never
  : never

export type Values<Types> = {
  readonly [N in keyof Types]: ModelAt<Types, N>
} & {
  readonly [
    N in NamesOfKind<
      Types,
      "object" | "interface" | "union"
    > as `${N}Edge` extends keyof Types ? never : `${N}Edge`
  ]: Edge<ModelAt<Types, N>>
} & {
  readonly [
    N in NamesOfKind<
      Types,
      "object" | "interface" | "union"
    > as `${N}Connection` extends keyof Types ? never : `${N}Connection`
  ]: Connection<ModelAt<Types, N>>
} & ("PageInfo" extends keyof Types ? {} : { readonly PageInfo: PageInfo })

export type NameOf<T> = T extends {
  readonly "~kind": DefinitionKind
  readonly "~name": infer N extends string
}
  ? N
  : never

export type RegistrationIssue<
  Types,
  N extends string,
  A,
  K extends DefinitionKind,
> = N extends keyof Types
  ? Types[N] extends Registration<K, infer Expected>
    ? [A] extends [Expected]
      ? [Expected] extends [A]
        ? unknown
        : `Registration model mismatch for "${N}"; register the definition's model`
      : `Registration model mismatch for "${N}"; register the definition's model`
    : `Registration kind mismatch for "${N}"; expected ${K}`
  : `Register "${N}": add declare module "../graphql" { interface Types extends Register<typeof ${N}${K extends "object" ? "" : `, "${N}"${K extends "enum" | "scalar" ? "" : `, "${K}"`}`}> {} }`

export type RegistrationCheck<
  Types,
  N extends string,
  A,
  K extends DefinitionKind,
> =
  RegistrationIssue<Types, N, A, K> extends infer Issue
    ? Issue extends string
      ? { readonly error: Issue }
      : unknown
    : never

export type TaggedModel = Schema.Top & {
  readonly Type: { readonly _tag: string }
  readonly fields: { readonly _tag: Schema.Constraint }
}

export type MemberIssue<Types, M extends Schema.Top> = M extends TaggedModel
  ? RegistrationIssue<Types, Tag<M>, M["Type"], "object"> extends infer Issue
    ? Issue extends string
      ? Issue
      : never
    : never
  : "Union members must be tagged object models; use registered objects and pass { members: [...] } for untagged models"

export type CheckedMembers<Types, Members extends readonly Schema.Top[]> =
  MemberIssue<Types, Members[number]> extends infer Issue
    ? [Issue] extends [never]
      ? unknown
      : { readonly error: Issue }
    : never

export type Complete<Types, M> =
  Exclude<keyof Types, NameOf<M[keyof M]>> extends infer Missing
    ? [Missing] extends [never]
      ? unknown
      : `Missing definition: ${Missing & string}; export it through the schema barrel`
    : never

/** Check each definition separately so conflicting contexts cannot widen into a union. */
export type CompatibleContexts<M, C> = {
  readonly [K in keyof M]: M[K] extends {
    readonly "~kind": string
    readonly fields: infer F
  }
    ? M[K] extends Contextual<C>
      ? F extends FieldMap<never, C>
        ? unknown
        : "Fields require a different request context; build them with the createBuilder<Types, AppContext>() builder that builds the schema"
      : "Definition requires a different request context; use definitions from createBuilder<Types, AppContext>() with the schema context"
    : unknown
}
