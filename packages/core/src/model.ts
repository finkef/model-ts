import * as t from "io-ts"
import { either, isLeft } from "fp-ts/lib/Either"
import { Provider, InstanceProps, ClassProps } from "./provider"
import { ModelConstructor, encodeProp, getProps } from "./utils"
import { RuntimeTypeValidationError } from "./runtime-type-validation-error"

export type ModelInstance<
  T extends string,
  C extends t.HasProps,
  P extends Provider = Provider
> = Readonly<t.TypeOf<C>> & {
  _model: Model<T, C, P>
  _tag: T
  _codec: C
  encode(): t.OutputOf<C> & { _tag: T }
  values(): t.TypeOf<C>
} & InstanceProps<P>

export interface BaseModel<
  T extends string,
  C extends t.HasProps,
  P extends Provider = Provider
> {
  _tag: T
  _codec: C

  new (input: t.TypeOf<C>): ModelInstance<T, C, P>

  /**
   * Try to decode the provided value with the model's codec and create a new model instance.
   * Throws `RuntimeTypeValidationError` if value can't be decoded.
   *
   * @param {unknown} value - Value to be decoded
   * @throws {RuntimeTypeValidationError} Type can't be decoded using the model's codec.
   */
  decodeOrThrow<M extends ModelConstructor>(
    this: M,
    value: unknown
  ): InstanceType<M>
  /**
   * Try to decode the provided value with the model's codec and create a new model instance.
   * Throws `RuntimeTypeValidationError` if value can't be decoded.
   *
   * Shorthand for `decodeOrThrow`
   *
   * @param {unknown} value - Value to be decoded
   * @throws {RuntimeTypeValidationError} Type can't be decoded using the model's codec.
   */
  from<M extends ModelConstructor>(this: M, value: unknown): InstanceType<M>

  /**
   * Encodes a value using the codec for the given field.
   *
   * @param key
   * @param value
   */
  encodeProp<K extends keyof t.TypeOf<C>>(
    key: K,
    value: t.TypeOf<C>[K]
  ): t.OutputOf<C>[K]

  // io-ts interop
  _A: ModelInstance<T, C, P>
  _O: t.TypeOf<C> & { _tag: T }
  _I: unknown
  is<M extends ModelConstructor>(
    this: M,
    value: unknown
  ): value is InstanceType<M>
  decode<T>(this: ModelConstructor<T>, value: unknown): t.Validation<T>
  encode<M extends ModelConstructor>(
    this: M,
    value: InstanceType<M>
  ): OutputOf<M>
  validate<T>(
    this: ModelConstructor<T>,
    value: unknown,
    context: t.Context
  ): t.Validation<T>

  // TODO: figure out pipe typing
  pipe: any
  // pipe<T, M extends ModelConstructor<T>, B, IB, A extends IB, OB extends A>(
  //   this: M,
  //   ab: t.Type<B, OB, IB>,
  //   name?: string
  // ): t.Type<B, OutputOf<M>, InputOf<M>>

  asDecoder<M extends ModelConstructor>(this: M): t.Decoder<unknown, TypeOf<M>>
  asEncoder<M extends ModelConstructor>(
    this: M
  ): t.Encoder<TypeOf<M>, OutputOf<M>>
}

export type Model<
  T extends string,
  C extends t.HasProps,
  P extends Provider = Provider
> = BaseModel<T, C, P> & ClassProps<P>

export type AnyModel = Model<string, any>
export type AnyModelInstance = InstanceType<AnyModel>

export type ModelOf<T extends AnyModelInstance> = T["_model"]
export type TypeOf<T extends ModelConstructor> = InstanceType<T>
export type InputOf<T extends ModelConstructor> = t.InputOf<T["_codec"]>
export type OutputOf<T extends ModelConstructor> = t.OutputOf<T["_codec"]> & {
  _tag: T["_tag"]
}

/**
 * Creates a new model class with the given tag and codec.
 *
 * Note: It is intended to extend the returned anonymous class by another user-defined class in order
 * to not generate redundant TS declarations that can ultimately slow down the compiler significantly.
 *
 * ### Example
 *
 * ```
 * class MyClass extends model("MyClass", t.type({foo: t.number})) {}
 * ```
 *
 * @param tag - Unique identifier/name for the model.
 * @param codec - io-ts type used for encoding and decoding.
 */
export function model<T extends string, C extends t.HasProps>(
  tag: T,
  codec: C
): Model<T, C>
/**
 * Creates a new model class with the given tag and codec and inject properties and methods from the
 * given provider.
 *
 * Note: It is intended to extend the returned anonymous class by another user-defined class in order
 * to not generate redundant TS declarations that can ultimately slow down the compiler significantly.
 *
 * ### Example
 *
 * ```
 * class MyClass extends model("MyClass", t.type({foo: t.number}), provider) {}
 * ```
 *
 * @param tag - Unique identifier/name for the model.
 * @param codec - io-ts type used for encoding and decoding.
 * @param provider - io-ts type used for encoding and decoding.
 */
export function model<
  T extends string,
  C extends t.HasProps,
  P extends Provider
>(tag: T, codec: C, provider: P): Model<T, C, P>
export function model<
  T extends string,
  C extends t.HasProps,
  P extends Provider
>(tag: T, codec: C, provider?: P): Model<T, C, P> {
  class Model {
    static _tag = tag
    static _codec = t.exact(codec)

    constructor(input: t.TypeOf<C>) {
      Object.assign(this, input)
      Object.assign(this, {
        _tag: Model._tag,
        _codec: Model._codec,
        encode() {
          return (Model as any).encode(this)
        },
        values(): t.TypeOf<C> {
          const keys = new Set(Object.keys(getProps(codec)))
          return Object.fromEntries(
            Object.entries(this).filter(([key]) => keys.has(key))
          )
        },
        toJSON(): t.TypeOf<C> {
          return { _tag: this._tag, ...this.values() }
        },
        [Symbol.for("nodejs.util.inspect.custom")](): string {
          return `${Model._tag} ${JSON.stringify(this.values(), null, 2)}`
        }
      })
      Object.assign(this, provider?.instanceProps ?? {})
    }

    get _model() {
      return this.constructor
    }

    // TODO: use?
    // toJSON<T extends ModelInstance<string, any>>(this: T) {
    //   const { _model, _codec, _tag, ...rest } = this

    //   return rest
    // }

    static decodeOrThrow<M extends ModelConstructor<any>>(
      this: M,
      value: unknown
    ): InstanceType<M> {
      const decoded = this._codec.decode(value)
      if (isLeft(decoded)) throw new RuntimeTypeValidationError(decoded.left)
      return new this(decoded.right)
    }
    // Shorthand for decodeOrThrow
    static from<M extends ModelConstructor<any>>(
      this: M,
      value: unknown
    ): InstanceType<M> {
      return this.decodeOrThrow<M>(value)
    }

    static encodeProp<K extends keyof t.TypeOf<C>>(
      this: T,
      key: K,
      value: T[K]
    ) {
      try {
        const encoded = encodeProp(Model._codec, key, value)
        return encoded
      } catch (error) {
        // In case that nothing matched, return the value itself.
        return value
      }
    }

    // io-ts interop
    static is<M extends ModelConstructor>(
      this: M,
      value: unknown
    ): value is InstanceType<M> {
      return value instanceof Model
    }
    static decode<T>(
      this: ModelConstructor<T>,
      value: unknown
    ): t.Validation<T> {
      return either.map(
        Model._codec.decode(value),
        decoded => new this(decoded)
      )
    }
    static encode<M extends ModelConstructor>(
      this: M,
      value: InstanceType<M>
    ): OutputOf<M> {
      return Object.assign(Model._codec.encode(value), { _tag: Model._tag })
    }
    static validate<T>(
      this: ModelConstructor<T>,
      value: unknown,
      context: t.Context
    ): t.Validation<T> {
      return either.map(
        Model._codec.validate(value, context),
        decoded => new this(decoded)
      )
    }
    static pipe<
      B,
      IB extends {},
      T extends IB,
      M extends ModelConstructor<T>,
      OB extends T
    >(
      this: M,
      ab: t.Type<B, OB, IB>,
      name: string = `pipe(${this._tag}, ${ab.name})`
    ): t.Type<B, OutputOf<M>, InputOf<M>> {
      return new t.Type(
        name,
        ab.is,
        (i, c) => {
          const e = this.validate(i, c)
          if (isLeft(e)) {
            return e
          }
          return ab.validate(e.right, c)
        },
        b => this.encode(ab.encode(b) as any)
      )
    }
    static asDecoder<T extends {}, M extends ModelConstructor<T>>(
      this: M
    ): t.Decoder<unknown, TypeOf<M>> {
      return this
    }
    static asEncoder<M extends ModelConstructor>(
      this: M
    ): t.Encoder<InstanceType<M>, OutputOf<M>> {
      return this
    }
  }

  Object.assign(Model, provider?.classProps ?? {})

  return Model as any
}
