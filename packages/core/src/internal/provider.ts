import type { Input, NoMembers, Provider } from "../Provider.ts"

export const noProvider: Provider<NoMembers> = {
  instanceProps: {},
  classProps: {},
  unionProps: {},
}

const mergeDescriptors = (sources: ReadonlyArray<object>): object => {
  const target = {}
  for (const source of sources) {
    Object.defineProperties(target, Object.getOwnPropertyDescriptors(source))
  }
  return target
}

/** Merges providers left to right; a later member replaces an earlier one. */
export const mergeAll = (
  providers: ReadonlyArray<Provider>,
): Pick<Provider, "instanceProps" | "classProps" | "unionProps"> => ({
  instanceProps: mergeDescriptors(providers.map((p) => p.instanceProps)),
  classProps: mergeDescriptors(providers.map((p) => p.classProps)),
  unionProps: mergeDescriptors(providers.map((p) => p.unionProps)),
})

const isProviderList = (input: Input): input is ReadonlyArray<Provider> =>
  Array.isArray(input)

export const toProvider = (input: Input): Provider =>
  isProviderList(input) ? mergeAll(input) : input

/** Copies provider members onto a target and its prototype. */
export const defineMembers = (target: object, members: object) =>
  Object.defineProperties(target, Object.getOwnPropertyDescriptors(members))
