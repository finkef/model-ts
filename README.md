# model-ts

![npm version](https://img.shields.io/npm/v/@model-ts/core)
![github workflow status](https://img.shields.io/github/workflow/status/finkef/model-ts/CI)

> Extensible model framework for [io-ts](https://github.com/gcanti/io-ts).

- [About](#about)
- [Installation](#installation)
- [Usage](#usage)
  - [Composing models and io-ts codecs](#composing-models-and-io-ts-codecs)
  - [Providers](#providers)
    - [Enforcing Properties on models](#enforcing-properties-on-models)
  - [Union Types](#union-types)
- [License](#license)

## About

When working with en-/decoding or parsing data in TypeScript, [io-ts](https://github.com/gcanti/io-ts) is arguably one of the best solutions available. This library aims at extending io-ts codecs with the power of OOP classes, adding the ability of defining class/instance methods and properties. In addition, it adds more ergonomic ways of instantiating and decoding types for when used in codebases that don't heavily rely on functional programming (and [fp-ts](https://github.com/gcanti/fp-ts)).

Consider this simple io-ts codec:

```ts
const PersonCodec = t.type({ firstName: t.string, lastName: t.string })
```

If we want to have a way of generating the full name of a person, we currently need to do the following:

```ts
const getFullName = (person: t.TypeOf<typeof PersonCodec>) =>
  `${person.firstName} ${person.lastName}`
```

With model-ts, we can transform the `PersonCodec` into a class that we can use as we would any other, keeping all of the TypeScript information available.

```ts
class Person extends model("Person", PersonCodec) {
  get fullName() {
    return `${person.firstName} ${person.lastName}`
  }

  printFullName() {
    console.log(this.fullName)
  }

  async save() {
    return db.someCallThatStoresAPerson(this.encode())
  }

  static async load() {
    const data = await db.someCallThatLoadsAPerson()
    return new Person(data)
  }
}
```

Now, we can easily instantiate a Person and have the `fullName` property and `printFullName()` method neatly available on the instance. In addition, TypeScript now refers to our instances with the `Person` type, rather than unpacking the whole type of the codec:

```ts
const person1 = new Person({ firstName: "John", lastName: "Doe" }) // Person { firstName: "John", lastName: "Doe" }

person1.fullName // "John Doe"

person1.printFullName() // logs "John Doe"
```

Notice that we still preserved the underlying data without adding any additional stuff (except for a `_tag` property used for more efficient decoding of unions):

```ts
person1.encode() // { _tag: "Person", firstName: "John", lastName: "Doe" }
```

Also, now we have an easy way of loading and storing a person from an imaginary database:

```ts
// Loading
const person2 = await Person.load() // Person { firstName: "Jane", lastName: "Doe" }

// Storing
const person3 = new Person({ firstName: "John", lastName: "Doe" })
await person3.save()
```

One of the most powerful tools of model-ts is the concept of providers. Providers allow you to inject properties and methods into classes (as we just did manually for the Person model) while also preserving type safety. This allows to build an ORM for virtually any data storage all while using the same concepts. See [Providers](#providers) for details.

## Installation

```sh
npm install @model-ts/core
# or
yarn add @model-ts/core
```

## Usage

```ts
import { model, t } from "@model-ts/core"

const codec = t.type({
  foo: t.string,
  bar: t.DateFromISOString
})

class MyModel extends model("MyModel", codec) {
  print() {
    console.log("Bar: " + this.bar.toLocaleString())
  }
}

// Create a model instance from complex data structures
const instance = new MyModel({ foo: "Hello World", bar: new Date() })
// -> MyModel { foo: "Hello World", bar: Date }

instance.print()
// logs "Bar: 12/30/2021, 11:38:58 AM"

// Encode as JSON-serializable object
const encoded = instance.encode()
// -> { _tag: "MyModel", foo: "Hello World", bar: "2021-12-30T10:38:58.786Z" }

// Decode from serialized data
const decoded = MyModel.from({ foo: "bar", bar: "2020-12-12T12:12:12.000Z" })
// -> MyModel { foo: "bar", bar: Date }

// Trying to decode invalid data
MyModel.from("this is just a string, not the expected signature")
// -> throws RuntimeTypeValidationError
```

### Composing models and io-ts codecs

Since models themselves are io-ts codecs, they can be used interchangeably and also be nested or composed.

```ts
/* Nesting models in models */
class A extends model("A", t.type({ foo: t.number })) {}
class B extends model("B", t.type({ bar: t.string, baz: A })) {}

const a = new A({ foo: 42 })
const b = new B({ bar: "Hello", baz: a })

b.encode()
// -> { _tag: "B", bar: "Hello", baz: { _tag: "A", foo: 42 }}

B.from({ bar: "bar", baz: { foo: 123 } })
// -> B { bar: "bar", baz: A { foo: 123 } }

/* Nesting models in io-ts codecs */
const C = t.type({ a: A, b: B })
const D = t.union(t.null, B)

C.decode({ a: { foo: 234 }, b: { bar: "123", baz: { foo: 123 } } })
// -> Either<Error, C>
```

### Providers

A provider can inject properties, methods and functionality into both the model class itself through
`classProps` (as static methods) as well as into its instances through `instanceProps`. Additionally,
`unionProps` can be injected into the models created with `union`.

```ts
const myProvider = {
  classProps: {
    foo: 42, // Simple value

    // Function
    printSomething() {
      console.log("something")
    },

    // With a getter
    get bar() {
      return "*".repeat(this.foo)
    },

    getNamespace() {
      // Accessing properties of the model class can't be handled safely in the function
      // implementation, but can be enforced by specifically describing this provider
      // using an interface.
      // Check the description of the `Provider` type for reference.
      return this._namespace
    }
  },
  instanceProps: {
    // Same as for classProps
  },
  unionProps: {
    // Same as for classProps
  }
}
```

#### Enforcing Properties on models

Often, injected functionality relies on certain fields and functions on the model class or instance
themselves. Since we have no access (at least not statically) to the models we're injecting into,
we need to work around that limitation and enforce these constraints through Provider interfaces.

For example, let's assume that we're writing a provider for a KV Store that allows for basic CRUD
functionality (we'll focus on a `save()` function here), but requires our models to generate a
unique primary key. We can achieve this by specifying an interface for our provider and making use
of a generic typing for the `this` parameter to make sure the model actually implements a function
`getKey(): string`. If the condition is not met, the compiler errors when trying to invoke the
injected functions.

```ts
interface KVProvider extends Provider {
  instanceProps: {
    // T is a generic type for a model instance
    save<T extends { getKey: () => string }>(this: T): Promise<void>
  }
  classProps: {
    // C is a generic type for the model class itself and needs to extend `ModelConstructor` for us
    // to be able to derive the instance type via `InstanceType<C>`
    getAll<C extends ModelConstructor & { namespace: string }>(
      this: C
    ): Promise<InstanceType<C>>
  }
}

const provider: KVProvider = {
  instanceProps: {
    save() {
      const key = this.getKey()
      return store.save(key, this)
    }
  },
  classProps: {
    getAll() {
      return store.list(this.namespace)
    }
  }
}

const codec = t.type({ foo: t.number })

class A extends model("A", codec, provider) {
  static namespace = "A"
}
class B extends model("B", codec, provider) {
  getKey() {
    return "mynamespace:" + this.foo
  }
}

new A({ foo: 12 }).save() // TypeScript Error! A does not have a `getKey` function
new B({ foo: 12 }).save() // Promise<void>

A.getAll() // Promise<A>
B.getAll() // TypeScript Error! B does not have a `namespace` property
```

### Union Types

We often encounter use cases that require polymorphic data types. If we want to be able to decode multiple types at the same time, we can make use of unions:

```ts
class A extends model("A", t.type({ a: t.string, shared: t.string })) {}
class B extends model("B", t.type({ b: t.number, shared: t.string })) {}

class MyUnion extends union([A, B]) {}
```

Now, we can decode data with either one of the two (or more) models. Here, model-ts makes use of the automatically added `_tag` property (if present) to more efficiently select the correct type for decoding.

```ts
const aOrB = MyUnion.decode({ b: 42, shared: "hello" }) // A | B

aOrB.shared // string
aOrB.a // TypeScript Error!

if (aOrB instanceof A) {
  aOrB.a // string
}
```

As with regular models, we can nest union types within other models and inject props by a provider using the `unionProps` property. Also, we can add class properties and methods to the union class itself.

## License

MIT
