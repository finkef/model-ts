/** Crockford's base32, as used by ULID. */
const ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
const TIME_LENGTH = 10
const RANDOM_LENGTH = 16

/**
 * A ULID: 48 bits of time and 80 bits of randomness, encoded in 26
 * characters that sort by time. `draws` are numbers in [0, 1); without them
 * the random part comes from `crypto.getRandomValues`.
 */
export const ulid = (time: number, draws?: ReadonlyArray<number>): string => {
  let out = ""
  let rest = Math.floor(time)
  for (let i = 0; i < TIME_LENGTH; i++) {
    const digit = rest % 32
    out = ENCODING[digit] + out
    rest = (rest - digit) / 32
  }
  const random = draws ?? randomDraws()
  for (let i = 0; i < RANDOM_LENGTH; i++) {
    out += ENCODING[Math.min(31, Math.floor(random[i % random.length]! * 32))]
  }
  return out
}

const randomDraws = () =>
  Array.from(
    globalThis.crypto.getRandomValues(new Uint8Array(RANDOM_LENGTH)),
    (byte) => byte / 256,
  )
