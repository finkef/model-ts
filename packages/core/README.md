# @model-ts/core

Effect Schema classes with your own methods. `Model.Class` turns Schema fields
into a class that decodes, encodes and gains members from providers. This page is
the API reference; the [main README](../../README.md) explains the concepts.

> [!NOTE]
> Version 7 is built on Effect v4. The io-ts version is **@model-ts/core 0.4.x**.

```sh
npm install @model-ts/core effect
```

- [`Model.Class`](#modelclass)
- [`Model.Union`](#modelunion)
- [Providers](#providers)
- [Types](#types)

## `Model.Class`

```ts
Model.Class<Self>()(tag, fields)
Model.Class<Self>()(tag, provider, fields)
Model.Class<Self>()(tag, [providerA, providerB], fields)
```

Returns a class to extend. `Self` is the class you are declaring; `tag` becomes
its `_tag` and its Schema identifier.

```ts
import { Model } from "@model-ts/core"
import { Schema } from "effect"

class Address extends Model.Class<Address>()("Address", {
  street: Schema.String,
  city: Schema.String,
  zip: Schema.optional(Schema.String),
}) {
  get oneLine() {
    return `${this.street}, ${this.city}`
  }
}
```

The class is a `Schema.Class`: `Schema.decodeUnknownEffect(Address)`,
`Schema.Array(Address)` and nesting in other models all work, and every
decoder returns `Address` instances, getters and methods included.

Fields are any Schemas that decode and encode without Effect services.
Construction, `from` and `encode` run synchronously, so a field whose
transformation or default needs a service is a type error. Optional and
nullable fields and defaults use Schema's own combinators:
`Schema.optional`, `Schema.NullOr`, `Schema.withConstructorDefault` and
`Schema.withDecodingDefaultKey`.

### Instances

| Member                   | Returns                                                                             |
| ------------------------ | ----------------------------------------------------------------------------------- |
| `new Address(values)`    | An instance. Applies defaults and skips filters and brands (see below).             |
| `address.encode()`       | The encoded form, with `_tag`. Throws a `SchemaError` if a value cannot be encoded. |
| `address.values()`       | The field values as a plain object, without `_tag`.                                 |
| fields, getters, methods | As declared.                                                                        |
| provider members         | Whatever the model's providers add.                                                 |

`new` takes the decoded (`Type`) side of each field, brands included, and
leaves out `_tag` and fields that have defaults. It passes Effect's
`disableChecks`, so it checks the shape but skips filters and brands: the
types already guarantee them, and checks belong where untrusted data enters.
`new Address(values, { disableChecks: false })` and `Address.make(values)`
run every check.

### Statics

| Member                  | Returns                                                                                            |
| ----------------------- | -------------------------------------------------------------------------------------------------- |
| `Address.from(input)`   | An instance; throws a `SchemaError` on invalid input. Same as `Schema.decodeUnknownSync(Address)`. |
| `Address.decode(input)` | `Effect<Address, SchemaError>`. Same as `Schema.decodeUnknownEffect(Address)`.                     |
| `Address.make(values)`  | An instance; validates and throws on invalid values.                                               |
| Schema members          | `fields`, `identifier`, `annotate`, `pipe` and the rest of `Schema.Class`.                         |
| provider members        | Whatever the model's providers add.                                                                |

### `_tag`

`_tag` is optional on input, at any depth: `Address.from({ street, city })`
works, and so does a stored row written before the model had a tag. Encoding
always writes it.

## `Model.Union`

```ts
Model.Union(members)
Model.Union(members, provider)
```

A `Schema.Union` of models. Decoding picks the member named by `_tag`, or the
first member that matches when the input has no tag.

```ts
const Shape = Model.Union([Circle, Square])
type Shape = typeof Shape.Type // Circle | Square

Shape.from(input) // Circle | Square; throws a SchemaError if nothing matches
Shape.decode(input) // Effect<Circle | Square, SchemaError>
Shape.members // [Circle, Square]
```

With a provider, the union also gets the provider's union members, such as
the DynamoDB reads in `Model.Union([Post, Comment], DynamoDB.provider)`.

## Providers

A provider adds members to every model that uses it. It has three parts, each
optional:

| Part            | Added to                                           |
| --------------- | -------------------------------------------------- |
| `instanceProps` | the prototype: methods and getters                 |
| `classProps`    | the class: statics                                 |
| `unionProps`    | unions built with `Model.Union(members, provider)` |

### Declaring members

The member types are a _type lambda_: an interface that extends
`Provider.Members` and reads `this["Self"]` (the model's instance type) and
`this["Fields"]` (its Schema fields). `Model.Class` fills both in, so a member can
return the exact model type. `this` is only usable at the top level of the
interface, so pass it on to a generic interface:

```ts
import { type Model, Provider } from "@model-ts/core"

interface TimestampsInstance<Self> {
  touch(this: Self & { readonly updatedAt: Date }): Self
}

interface TimestampsStatics<Self, Fields> {
  createWith(
    this: new (values: Model.Values<Fields>) => Self,
    values: Model.Values<Fields>,
  ): Self
}

interface TimestampsMembers extends Provider.Members {
  readonly instance: TimestampsInstance<this["Self"]>
  readonly static: TimestampsStatics<this["Self"], this["Fields"]>
}
```

A `this` parameter on a member says what the model must have for the member to
be callable. `touch` above can only be called on models with an `updatedAt`
field; on any other model it is a type error.

### Implementing them

```ts
export const timestamps = Provider.make<TimestampsMembers>()({
  instanceProps: {
    touch() {
      const Self = this.constructor as new (values: object) => typeof this
      return new Self({ ...this.values(), updatedAt: new Date() })
    },
  },
  classProps: {
    createWith(values) {
      return new this(values)
    },
  },
})
```

`Provider.make` checks the implementation against the declared members, so a
missing member, a wrong parameter or a `this` that asks for more than the
declaration does not compile. The implementation is checked with `Self` as
`unknown`, so a member declared to return `Self` is trusted to return the
model it was called on. Getters and other property descriptors are copied as
written.

Members that need a client or configuration should return an `Effect` that
asks for a service, the way `@model-ts/dynamodb` asks for `DynamoDB`. The
provider then stays a constant, and the caller chooses the implementation with
a layer.

### Combining providers

Pass an array to `Model.Class`, or merge once and reuse the result:

```ts
const stored = Provider.merge([DynamoDB.provider, timestamps])

class Invoice extends Model.Class<Invoice>()("Invoice", stored, { ... }) {}
class Receipt extends Model.Class<Receipt>()("Receipt", [DynamoDB.provider, timestamps], { ... }) {}
```

Providers merge left to right. When two define a member with the same name,
the later one wins, in the types and at runtime. That lets a provider wrap or
replace a member of another one.

## Types

The `Model` namespace:

| Type                                | Is                                                                      |
| ----------------------------------- | ----------------------------------------------------------------------- |
| `Model.Class<Self, Tag, Fields, P>` | The class `Model.Class` returns.                                        |
| `Model.Methods`, `Model.Statics`    | The members every model and model class has.                            |
| `Model.Struct<Tag, Fields>`         | The `Schema.Struct` behind a model, `_tag` included.                    |
| `Model.Fields`                      | The fields `Model.Class` accepts: Schemas without service requirements. |
| `Model.Values<Fields>`              | The field values of a model, without `_tag`.                            |
| `Model.Any`                         | Any model class; the constraint for union members.                      |
| `Model.Union`, `Model.UnionStatics` | The schema `Model.Union` returns and its extra members.                 |

The `Provider` namespace:

| Type                                                                          | Is                                                               |
| ----------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| `Provider.Provider<M>`                                                        | A provider value with members `M`.                               |
| `Provider.Members`                                                            | The base interface for declaring members.                        |
| `Provider.InstanceMembers`, `Provider.StaticMembers`, `Provider.UnionMembers` | The members a provider adds for a given model.                   |
| `Provider.MergedMembers`, `Provider.AsProvider`, `Provider.Input`             | Types behind merging and the provider argument of `Model.Class`. |
| `Provider.NoMembers`                                                          | The members of a model without providers: none.                  |

Every module is also importable on its own, as in Effect:
`import * as Model from "@model-ts/core/Model"`.
