export const GSI_NAMES = [
  "GSI2",
  "GSI3",
  "GSI4",
  "GSI5",
  "GSI6",
  "GSI7",
  "GSI8",
  "GSI9",
  "GSI10",
  "GSI11",
  "GSI12",
  "GSI13",
  "GSI14",
  "GSI15",
  "GSI16",
  "GSI17",
  "GSI18",
  "GSI19",
] as const

export type GSI = typeof GSI_NAMES[number]

export type GSIPK = `${GSI}PK`
export type GSISK = `${GSI}SK`
