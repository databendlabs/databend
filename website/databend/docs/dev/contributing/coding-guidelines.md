---
title: Coding Guidelines
---

This document describes the coding guidelines for the Databend Rust codebase.

## Code formatting

All code formatting is enforced with [rustfmt](https://github.com/rust-lang/rustfmt) with a project-specific configuration.  Below is an example command:

```shell
$ make fmt
```

## Code analysis

[Clippy](https://github.com/rust-lang/rust-clippy) is used to catch common mistakes and is run as a part of continuous integration.  Before submitting your code for review, you can run lint:

```shell
$ make lint
```

## Code documentation

Any public fields, functions, and methods should be documented with [Rustdoc](https://doc.rust-lang.org/book/ch14-02-publishing-to-crates-io.html#making-useful-documentation-comments).

Please follow the conventions as detailed below for modules, structs, enums, and functions.  The *single line* is used as a preview when navigating Rustdoc.  As an example, see the 'Structs' and 'Enums' sections in the [collections](https://doc.rust-lang.org/std/collections/index.html) Rustdoc.

 ```rust
 /// [Single line] One line summary description
 ///
 /// [Longer description] Multiple lines, inline code
 /// examples, invariants, purpose, usage, etc.
 [Attributes] If attributes exist, add after Rustdoc
 ```

Example below:

```rust
/// Represents (x, y) of a 2-dimensional grid
///
/// A line is defined by 2 instances.
/// A plane is defined by 3 instances.
#[repr(C)]
struct Point {
    x: i32,
    y: i32,
}
```


## Testing

*Unit tests*

```shell
$ make unit-test
```

*Stateless tests*

```shell
$ make stateless-test
```
