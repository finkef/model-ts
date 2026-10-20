import { CreateTableCommand } from "@aws-sdk/client-dynamodb"
import { GSI_NAMES } from "../Keys.ts"

/** The table layout this library expects: PK/SK, GSI1 (SK/PK) and GSI2 to GSI19. */
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
    GlobalSecondaryIndexes: [
      {
        IndexName: "GSI1",
        KeySchema: [
          { AttributeName: "SK", KeyType: "HASH" },
          { AttributeName: "PK", KeyType: "RANGE" },
        ],
        Projection: { ProjectionType: "ALL" },
      },
      ...GSI_NAMES.map((gsi) => ({
        IndexName: gsi,
        KeySchema: [
          { AttributeName: `${gsi}PK`, KeyType: "HASH" as const },
          { AttributeName: `${gsi}SK`, KeyType: "RANGE" as const },
        ],
        Projection: { ProjectionType: "ALL" as const },
      })),
    ],
  })
