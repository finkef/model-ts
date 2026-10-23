import { Sandbox, type SandboxOptions } from "@model-ts/dynamodb/Sandbox"
import { inject } from "vitest"

declare module "vitest" {
  export interface ProvidedContext {
    readonly dynamodb: "memory" | "local"
  }
}

/** The Vitest project selects the backend; each layer build owns a fresh table. */
export const testTable = (options?: SandboxOptions) =>
  inject("dynamodb") === "local"
    ? Sandbox.layerLocal(options)
    : Sandbox.layerMemory(options)
