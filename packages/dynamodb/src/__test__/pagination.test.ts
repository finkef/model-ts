import crypto from "crypto"
import { encodeDDBCursor, decodeDDBCursor } from "../pagination"
import { PaginationError } from "../errors"

describe("encodeDDBCursor", () => {
  describe("basic encoding without encryption", () => {
    it("should encode basic PK and SK", () => {
      const result = encodeDDBCursor({
        PK: "USER#123",
        SK: "PROFILE#456",
      })

      expect(result).toMatchInlineSnapshot(
        `"eyJQSyI6IlVTRVIjMTIzIiwiU0siOiJQUk9GSUxFIzQ1NiJ9"`
      )
    })

    it("should encode with GSI2 values", () => {
      const result = encodeDDBCursor({
        PK: "USER#123",
        SK: "PROFILE#456",
        GSI2PK: "GSI2PK#user123",
        GSI2SK: "GSI2SK#profile456",
      })

      expect(result).toMatchInlineSnapshot(
        `"eyJQSyI6IlVTRVIjMTIzIiwiU0siOiJQUk9GSUxFIzQ1NiIsIkdTSTJQSyI6IkdTSTJQSyN1c2VyMTIzIiwiR1NJMlNLIjoiR1NJMlNLI3Byb2ZpbGU0NTYifQ=="`
      )
    })

    it("should encode with multiple GSI values", () => {
      const result = encodeDDBCursor({
        PK: "USER#123",
        SK: "PROFILE#456",
        GSI2PK: "GSI2PK#user123",
        GSI2SK: "GSI2SK#profile456",
        GSI3PK: "GSI3PK#fixed",
        GSI3SK: "GSI3SK#value",
      })

      expect(result).toMatchInlineSnapshot(
        `"eyJQSyI6IlVTRVIjMTIzIiwiU0siOiJQUk9GSUxFIzQ1NiIsIkdTSTJQSyI6IkdTSTJQSyN1c2VyMTIzIiwiR1NJMlNLIjoiR1NJMlNLI3Byb2ZpbGU0NTYiLCJHU0kzUEsiOiJHU0kzUEsjZml4ZWQiLCJHU0kzU0siOiJHU0kzU0sjdmFsdWUifQ=="`
      )
    })
  })

  describe("encoding with encryption", () => {
    const encryptionKey = crypto.randomBytes(32)

    it("should encrypt the cursor when encryption key is provided", () => {
      const result = encodeDDBCursor(
        {
          PK: "USER#123",
          SK: "PROFILE#456",
        },
        encryptionKey
      )

      // The result should be encrypted and different from the unencrypted version
      expect(result).not.toBe(
        "eyJQSyI6IlVTRVIjMTIzIiwiU0siOiJQUk9GSUxFIzQ1NiJ9"
      )
      expect(result).toMatch(/^[A-Za-z0-9+/=]+$/) // Base64 format
    })

    it("should produce consistent encrypted results for same input", () => {
      const input = {
        PK: "USER#123",
        SK: "PROFILE#456",
      }

      const result1 = encodeDDBCursor(input, encryptionKey)
      const result2 = encodeDDBCursor(input, encryptionKey)

      expect(result1).toBe(result2)
    })

    it("should encrypt with GSI values", () => {
      const result = encodeDDBCursor(
        {
          PK: "USER#123",
          SK: "PROFILE#456",
          GSI2PK: "GSI2PK#user123",
          GSI2SK: "GSI2SK#profile456",
        },
        encryptionKey
      )

      expect(result).toMatch(/^[A-Za-z0-9+/=]+$/)
    })
  })
})

describe("decodeDDBCursor", () => {
  describe("basic decoding without encryption", () => {
    it("should decode basic PK and SK", () => {
      const encoded = "eyJQSyI6IlVTRVIjMTIzIiwiU0siOiJQUk9GSUxFIzQ1NiJ9"
      const result = decodeDDBCursor(encoded)

      expect(result).toMatchInlineSnapshot(`
        Object {
          "PK": "USER#123",
          "SK": "PROFILE#456",
        }
      `)
    })

    it("should decode with GSI2 values", () => {
      const encoded =
        "eyJQSyI6IlVTRVIjMTIzIiwiU0siOiJQUk9GSUxFIzQ1NiIsIkdTSTJQSyI6IkdTSTJQSyN1c2VyMTIzIiwiR1NJMlNLIjoiR1NJMlNLI3Byb2ZpbGU0NTYifQ=="
      const result = decodeDDBCursor(encoded, "GSI2")

      expect(result).toMatchInlineSnapshot(`
        Object {
          "GSI2PK": "GSI2PK#user123",
          "GSI2SK": "GSI2SK#profile456",
          "PK": "USER#123",
          "SK": "PROFILE#456",
        }
      `)
    })

    it("should decode with GSI3 values", () => {
      const encoded =
        "eyJQSyI6IlVTRVIjMTIzIiwiU0siOiJQUk9GSUxFIzQ1NiIsIkdTSTJQSyI6IkdTSTJQSyN1c2VyMTIzIiwiR1NJMlNLIjoiR1NJMlNLI3Byb2ZpbGU0NTYiLCJHU0kzUEsiOiJHU0kzUEsjZml4ZWQiLCJHU0kzU0siOiJHU0kzU0sjdmFsdWUifQ=="
      const result = decodeDDBCursor(encoded, "GSI3")

      expect(result).toMatchInlineSnapshot(`
        Object {
          "GSI3PK": "GSI3PK#fixed",
          "GSI3SK": "GSI3SK#value",
          "PK": "USER#123",
          "SK": "PROFILE#456",
        }
      `)
    })
  })

  describe("decoding with encryption", () => {
    const encryptionKey = crypto.randomBytes(32)

    it("should decode encrypted cursor", () => {
      const input = {
        PK: "USER#123",
        SK: "PROFILE#456",
      }
      const encoded = encodeDDBCursor(input, encryptionKey)
      const result = decodeDDBCursor(encoded, undefined, encryptionKey)

      expect(result).toMatchInlineSnapshot(`
        Object {
          "PK": "USER#123",
          "SK": "PROFILE#456",
        }
      `)
    })

    it("should decode encrypted cursor with GSI values", () => {
      const input = {
        PK: "USER#123",
        SK: "PROFILE#456",
        GSI2PK: "GSI2PK#user123",
        GSI2SK: "GSI2SK#profile456",
      }
      const encoded = encodeDDBCursor(input, encryptionKey)
      const result = decodeDDBCursor(encoded, "GSI2", encryptionKey)

      expect(result).toMatchInlineSnapshot(`
        Object {
          "GSI2PK": "GSI2PK#user123",
          "GSI2SK": "GSI2SK#profile456",
          "PK": "USER#123",
          "SK": "PROFILE#456",
        }
      `)
    })

    it("should throw PaginationError when decryption fails", () => {
      const wrongKey = crypto.randomBytes(32)
      const input = {
        PK: "USER#123",
        SK: "PROFILE#456",
      }
      const encoded = encodeDDBCursor(input, encryptionKey)

      expect(() => {
        decodeDDBCursor(encoded, undefined, wrongKey)
      }).toThrow(PaginationError)
      expect(() => {
        decodeDDBCursor(encoded, undefined, wrongKey)
      }).toThrow("Couldn't decode cursor")
    })
  })

  describe("error handling", () => {
    it("should throw PaginationError for invalid base64", () => {
      expect(() => {
        decodeDDBCursor("invalid-base64")
      }).toThrow(PaginationError)
      expect(() => {
        decodeDDBCursor("invalid-base64")
      }).toThrow("Couldn't decode cursor")
    })

    it("should throw PaginationError for invalid JSON", () => {
      const invalidJson = Buffer.from("invalid json").toString("base64")
      expect(() => {
        decodeDDBCursor(invalidJson)
      }).toThrow(PaginationError)
    })

    it("should throw PaginationError when PK is missing", () => {
      const invalidData = Buffer.from(
        JSON.stringify({ SK: "PROFILE#456" })
      ).toString("base64")
      expect(() => {
        decodeDDBCursor(invalidData)
      }).toThrow(PaginationError)
    })

    it("should throw PaginationError when SK is missing", () => {
      const invalidData = Buffer.from(
        JSON.stringify({ PK: "USER#123" })
      ).toString("base64")
      expect(() => {
        decodeDDBCursor(invalidData)
      }).toThrow(PaginationError)
    })

    it("should throw PaginationError when PK is not a string", () => {
      const invalidData = Buffer.from(
        JSON.stringify({ PK: 123, SK: "PROFILE#456" })
      ).toString("base64")
      expect(() => {
        decodeDDBCursor(invalidData)
      }).toThrow(PaginationError)
    })

    it("should throw PaginationError when SK is not a string", () => {
      const invalidData = Buffer.from(
        JSON.stringify({ PK: "USER#123", SK: 456 })
      ).toString("base64")
      expect(() => {
        decodeDDBCursor(invalidData)
      }).toThrow(PaginationError)
    })
  })

  describe("round-trip encoding and decoding", () => {
    it("should round-trip basic values", () => {
      const input = {
        PK: "USER#123",
        SK: "PROFILE#456",
      }
      const encoded = encodeDDBCursor(input)
      const decoded = decodeDDBCursor(encoded)

      expect(decoded).toEqual(input)
    })

    it("should round-trip with GSI values", () => {
      const input = {
        PK: "USER#123",
        SK: "PROFILE#456",
        GSI2PK: "GSI2PK#user123",
        GSI2SK: "GSI2SK#profile456",
        GSI3PK: "GSI3PK#fixed",
        GSI3SK: "GSI3SK#value",
      }
      const encoded = encodeDDBCursor(input)
      const decoded = decodeDDBCursor(encoded, "GSI2")

      expect(decoded).toEqual({
        PK: input.PK,
        SK: input.SK,
        GSI2PK: input.GSI2PK,
        GSI2SK: input.GSI2SK,
      })
    })

    it("should round-trip with encryption", () => {
      const encryptionKey = crypto.randomBytes(32)
      const input = {
        PK: "USER#123",
        SK: "PROFILE#456",
        GSI2PK: "GSI2PK#user123",
        GSI2SK: "GSI2SK#profile456",
      }
      const encoded = encodeDDBCursor(input, encryptionKey)
      const decoded = decodeDDBCursor(encoded, "GSI2", encryptionKey)

      expect(decoded).toEqual({
        PK: input.PK,
        SK: input.SK,
        GSI2PK: input.GSI2PK,
        GSI2SK: input.GSI2SK,
      })
    })
  })
})
