---
"@model-ts/graphql": patch
---

Stop copying a named input type's description onto every argument and input field of that type. The description now appears only on the `input` definition; describe an argument or field with `annotateKey({ description })`. An input type described differently at two uses (for example `Input.annotate({ description })` at one argument) now fails to assemble instead of taking whichever description was assembled first. Key annotations no longer change a named input type's own description.
