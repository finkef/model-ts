import { CreateTableCommand } from "@aws-sdk/client-dynamodb"
import type { marshallOptions as MarshallOptions } from "@aws-sdk/util-dynamodb"
import { GSI_NAMES } from "./keys.ts"

/**
 * How the document client turns items into attribute values. The request
 * cache stores written items with the same options, so they read back as the
 * table returns them. A function, because the client adds its own settings
 * to the object it is given.
 */
export const marshallOptions = (): MarshallOptions => ({
  removeUndefinedValues: true,
})

/** The table layout this library expects: PK/SK and GSI1 to GSI19. */
export const createTableCommand = (TableName: string) =>
  new CreateTableCommand({
    TableName,
    BillingMode: "PAY_PER_REQUEST",
    AttributeDefinitions: [
      { AttributeName: "PK", AttributeType: "S" },
      { AttributeName: "SK", AttributeType: "S" },
      ...GSI_NAMES.flatMap((gsi) => [
        { AttributeName: `${gsi}PK`, AttributeType: "S" as const },
        { AttributeName: `${gsi}SK`, AttributeType: "S" as const },
      ]),
    ],
    KeySchema: [
      { AttributeName: "PK", KeyType: "HASH" },
      { AttributeName: "SK", KeyType: "RANGE" },
    ],
    GlobalSecondaryIndexes: GSI_NAMES.map((gsi) => ({
      IndexName: gsi,
      KeySchema: [
        { AttributeName: `${gsi}PK`, KeyType: "HASH" as const },
        { AttributeName: `${gsi}SK`, KeyType: "RANGE" as const },
      ],
      Projection: { ProjectionType: "ALL" as const },
    })),
  })
