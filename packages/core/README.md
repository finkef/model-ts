# @model-ts/core

Effect Schema classes with your own methods. `model()` turns Schema fields into
a class that decodes, encodes and gains members from providers. This page is
the API reference; the [main README](../../README.md) explains the concepts.

> [!NOTE]
> Version 7 is built on Effect v4. The io-ts version is **@model-ts/core 0.4.x**.

```sh
npm install @model-ts/core effect
```

- [`model`](#model)
- [`Field`](#field)
- [`Id` and `generated`](#id-and-generated)
- [`union`](#union)
- [Providers](#providers)
- [Types](#types)

## `model`

```ts
model<Self>()(tag, fields)
model<Self>()(tag, provider, fields)
model<Self>()(tag, [providerA, providerB], fields)
```

Returns a class to extend. `Self` is the class you are declaring; `tag` becomes
its `_tag` and its Schema identifier.

```ts
import { Field, model } from "@model-ts/core"
import { Schema } from "effect"

class Address extends model<Address>()("Address", {
  street: Schema.String,
  city: Schema.String,
  zip: Field.optional(Schema.String),
}) {
  get oneLine() {
    return `${this.street}, ${this.city}`
  }
}
```

The class is a `Schema.Class`: `Schema.decodeUnknownEffect(Address)`,
`Schema.Array(Address)` and nesting in other models all work.

Fields are any Schemas that decode and encode without Effect services.
Construction, `from` and `encode` run synchronously, so a field whose
transformation or default needs a service is a type error.

### Instances

| Member                   | Returns                                                                             |
| ------------------------ | ----------------------------------------------------------------------------------- |
| `new Address(values)`    | An instance. Applies defaults and does not validate (see below).                    |
| `address.encode()`       | The encoded form, with `_tag`. Throws a `SchemaError` if a value cannot be encoded. |
| `address.values()`       | The field values as a plain object, without `_tag`.                                 |
| fields, getters, methods | As declared.                                                                        |
| provider members         | Whatever the model's providers add.                                                 |

`new` takes the decoded (`Type`) side of each field, brands included, and
leaves out `_tag` and fields that have defaults. It does not run validation:
the types already guarantee the shape, and checks belong where untrusted data
enters. Use `Address.make(values)` when you want a constructor that validates.

### Statics

| Member                  | Returns                                                                    |
| ----------------------- | -------------------------------------------------------------------------- |
| `Address.from(input)`   | An instance; throws a `SchemaError` on invalid input.                      |
| `Address.decode(input)` | `Effect<Address, SchemaError>`.                                            |
| `Address.make(values)`  | An instance; validates and throws on invalid values.                       |
| Schema members          | `fields`, `identifier`, `annotate`, `pipe` and the rest of `Schema.Class`. |
| provider members        | Whatever the model's providers add.                                        |

### `_tag`

`_tag` is optional on input, at any depth: `Address.from({ street, city })`
works, and so does a stored row written before the model had a tag. Encoding
always writes it.

## `Field`

Short names for common field shapes. Each returns an ordinary Schema.

| Helper                         | Field type        | Missing key       | `null`  |
| ------------------------------ | ----------------- | ----------------- | ------- |
| `Field.optional(s)`            | `key?: T`         | allowed           | no      |
| `Field.nullable(s)`            | `key: T \| null`  | no                | allowed |
| `Field.nullish(s)`             | `key?: T \| null` | allowed           | allowed |
| `s.pipe(Field.withDefault(f))` | `key: T`          | filled with `f()` | no      |

`Field.withDefault(f)` calls `f` each time a value is needed: when the
constructor leaves the field out and when decoding input that lacks it. That
makes it the tool for timestamps, counters and fields added to existing data:

```ts
class Settings extends model<Settings>()("Settings", {
  theme: Schema.Literals(["light", "dark"]).pipe(
    Field.withDefault(() => "light" as const),
  ),
  createdAt: Schema.DateFromString.pipe(Field.withDefault(() => new Date())),
}) {}

new Settings({}).theme // "light"
Settings.from({ createdAt: "2026-01-01T00:00:00.000Z" }).theme // "light"
```

Schema's own `Schema.withConstructorDefault` and `Schema.withDecodingDefaultKey`
work too, if a default should apply in only one of the two places.

## `Id` and `generated`

```ts
Id(prefix, brand)
```

An ID schema: a string that starts with `prefix`, branded as `brand`. Every
ID gets its own nominal type, so IDs of different entities cannot be mixed up.

```ts
const UserId = Id("user_", "UserId")
type UserId = typeof UserId.Type
```

| Member              | Does                                                                         |
| ------------------- | ---------------------------------------------------------------------------- |
| `UserId.make(s)`    | Brands `s`; the prefix is checked at compile time and at runtime.            |
| `UserId.generate()` | A new ID: the prefix plus a ULID from the system clock and `crypto`.         |
| `UserId.create`     | An `Effect<UserId>` that builds the ULID from Effect's `Clock` and `Random`. |
| `UserId.prefix`     | `"user_"`.                                                                   |

ULIDs sort by creation time, which keeps IDs in sort keys in creation order.
In tests, `UserId.create` with `TestClock` and `Random.withSeed` returns
repeatable IDs.

`generated(UserId)` is the field form: it fills in `UserId.generate()` when
the constructor or the decoded input leaves the field out.

```ts
class User extends model<User>()("User", {
  id: generated(UserId),
  name: Schema.String,
}) {}

new User({ name: "Ada" }).id // "user_01J..."
```

## `union`

```ts
union(members)
union(members, provider)
```

A `Schema.Union` of models. Decoding picks the member named by `_tag`, or the
first member that matches when the input has no tag.

```ts
const Shape = union([Circle, Square])
type Shape = typeof Shape.Type // Circle | Square

Shape.from(input) // Circle | Square; throws a SchemaError if nothing matches
Shape.decode(input) // Effect<Circle | Square, SchemaError>
Shape.members // [Circle, Square]
```

With a provider, the union also gets the provider's union members, such as
the DynamoDB reads in `union([Post, Comment], dynamodb)`.

## Providers

A provider adds members to every model that uses it. It has three parts, each
optional:

| Part            | Added to                                     |
| --------------- | -------------------------------------------- |
| `instanceProps` | the prototype: methods and getters           |
| `classProps`    | the class: statics                           |
| `unionProps`    | unions built with `union(members, provider)` |

### Declaring members

The member types are a _type lambda_: an interface that extends
`ProviderMembers` and reads `this["Self"]` (the model's instance type) and
`this["Fields"]` (its Schema fields). `model()` fills both in, so a member can
return the exact model type. `this` is only usable at the top level of the
interface, so pass it on to a generic interface:

```ts
import { makeProvider, type ProviderMembers, type Values } from "@model-ts/core"

interface TimestampsInstance<Self> {
  touch(this: Self & { readonly updatedAt: Date }): Self
}

interface TimestampsStatics<Self, Fields> {
  createWith(
    this: new (values: Values<Fields>) => Self,
    values: Values<Fields>,
  ): Self
}

interface TimestampsMembers extends ProviderMembers {
  readonly instance: TimestampsInstance<this["Self"]>
  readonly static: TimestampsStatics<this["Self"], this["Fields"]>
}
```

A `this` parameter on a member says what the model must have for the member to
be callable. `touch` above can only be called on models with an `updatedAt`
field; on any other model it is a type error.

### Implementing them

```ts
export const timestamps = makeProvider<TimestampsMembers>()({
  instanceProps: {
    touch() {
      const Model = this.constructor as new (values: object) => typeof this
      return new Model({ ...this.values(), updatedAt: new Date() })
    },
  },
  classProps: {
    createWith(values) {
      return new this(values)
    },
  },
})
```

`makeProvider` checks the implementation against the declared members, so a
missing member or a wrong signature does not compile. Getters and other
property descriptors are copied as written.

Members that need a client or configuration should return an `Effect` that
asks for a service, the way `@model-ts/dynamodb` asks for `DynamoDB`. The
provider then stays a constant, and the caller chooses the implementation with
a layer.

### Combining providers

Pass an array to `model()`, or merge once and reuse the result:

```ts
const stored = mergeProviders([dynamodb, timestamps])

class Invoice extends model<Invoice>()("Invoice", stored, { ... }) {}
class Receipt extends model<Receipt>()("Receipt", [dynamodb, timestamps], { ... }) {}
```

Providers merge left to right. When two define a member with the same name,
the later one wins, in the types and at runtime. That lets a provider wrap or
replace a member of another one.

## Types

| Type                                               | Is                                                                  |
| -------------------------------------------------- | ------------------------------------------------------------------- |
| `Model<Self, Tag, Fields, P>`                      | The class `model()` returns.                                        |
| `ModelMethods`, `ModelStatics`                     | The members every model and model class has.                        |
| `ModelStruct<Tag, Fields>`                         | The `Schema.Struct` behind a model, `_tag` included.                |
| `ModelFields`                                      | The fields `model()` accepts: Schemas without service requirements. |
| `Values<Fields>`                                   | The field values of a model, without `_tag`.                        |
| `AnyModel`                                         | Any model class; the constraint for union members.                  |
| `Union`, `UnionStatics`                            | The schema `union()` returns and its extra members.                 |
| `Provider<M>`                                      | A provider value with members `M`.                                  |
| `ProviderMembers`                                  | The base interface for declaring members.                           |
| `InstanceMembers`, `StaticMembers`, `UnionMembers` | The members a provider adds for a given model.                      |
| `MergedMembers`, `AsProvider`, `ProviderInput`     | Types behind merging and the provider argument of `model()`.        |
| `NoMembers`                                        | The members of a model without providers: none.                     |
