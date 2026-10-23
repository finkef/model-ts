/**
 * The JSON form DynamoDB sends and receives attribute values in, and the
 * plain documents the engine stores. Binary values travel as base64 strings
 * on the wire and are bytes in a document.
 */
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb"

export type Doc = Record<string, any>

const mapValues = (doc: Doc, f: (value: Doc) => Doc): Doc =>
  Object.fromEntries(Object.entries(doc).map(([key, value]) => [key, f(value)]))

const bytesFromWire = (value: Doc): Doc =>
  "B" in value
    ? { B: Buffer.from(value.B, "base64") }
    : "BS" in value
      ? { BS: value.BS.map((b: string) => Buffer.from(b, "base64")) }
      : "L" in value
        ? { L: value.L.map(bytesFromWire) }
        : "M" in value
          ? { M: mapValues(value.M, bytesFromWire) }
          : value

const bytesToWire = (value: Doc): Doc =>
  "B" in value
    ? { B: Buffer.from(value.B).toString("base64") }
    : "BS" in value
      ? {
          BS: value.BS.map((b: Uint8Array) =>
            Buffer.from(b).toString("base64"),
          ),
        }
      : "L" in value
        ? { L: value.L.map(bytesToWire) }
        : "M" in value
          ? { M: mapValues(value.M, bytesToWire) }
          : value

/** A document from attribute values in wire JSON. */
export const toDoc = (value: Doc | undefined) =>
  value === undefined ? undefined : unmarshall(mapValues(value, bytesFromWire))

/** Attribute values in wire JSON from attribute values the SDK decoded. */
export const imageToWire = (image: Doc): Doc => mapValues(image, bytesToWire)

/** Attribute values in wire JSON from a document. */
export const toWire = (value: Doc | undefined) =>
  value === undefined
    ? undefined
    : imageToWire(marshall(value, { removeUndefinedValues: true }))
