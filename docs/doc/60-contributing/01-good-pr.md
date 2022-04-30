---
title: Your First Databend Pull Request
sidebar_label: First Pull Request
description:
  You first good Databend pull request
---

[Databend](https://github.com/datafuselabs/databend) is an open source project, you can help with ideas, code, or documentation, we appreciate any efforts that help us to make the project better!

This document is a short guide for the people who want to contribute to Databend.

Our goal is to make contributing to the Databend project easy and transparent.

Thank you.

:::tip
Once the code been merged, your name will be stoned in the `system.contributors` table forever.

`SELECT * FROM system.contributors`
:::

## Prerequisites

Databend is written in Rust, to build Databend from scratch you will need to install the following tools:
* **Git**
* **Rust** Install with [rustup](https://rustup.rs/)

## Coding Guidelines

### Code Format and Analysis

```shell
$ make lint
```

### Code Documentation

Any public fields, functions, and methods should be documented with [Rustdoc](https://doc.rust-lang.org/book/ch14-02-publishing-to-crates-io.html#making-useful-documentation-comments).

Please follow the conventions as detailed below for `modules`, `structs`, `enums`, and `functions`. The *single line* is used as a preview when navigating Rustdoc.  As an example, see the 'Structs' and 'Enums' sections in the [collections](https://doc.rust-lang.org/std/collections/index.html) Rustdoc.

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


## Pull Requests

### Submit a PR

1. Fork the `databend` repo and create your branch from `main`.
2. Open a regular [issue](https://github.com/datafuselabs/databend/issues/new/choose) for binding the pull request.
3. Submit a [Draft Pull Requests](https://github.blog/2019-02-14-introducing-draft-pull-requests/), tag your work in progress.
4. If you have added code that should be tested, add unit tests.
5. Verify and ensure that the test suites passes, `make test`.
6. Make sure your code passes both linters, `make lint`.
7. Change the status to “Ready for review”.
8. Watch out the replies from the @datafuse-bots, she will be your guide.

### PR Template

Databend has a [Pull Request Template](https://github.com/datafuselabs/databend/blob/main/.github/PULL_REQUEST_TEMPLATE.md):

```shell
I hereby agree to the terms of the CLA available at: https://databend.rs/dev/policies/cla/

## Summary

Summary about this PR

## Changelog

- New Feature
- Bug Fix
- Improvement
- Performance Improvement
- Build/Testing/CI
- Documentation
- Other 
- Not for changelog (changelog entry is not required)

## Related Issues

Fixes #issue
```

You should not change the PR template context, but need to finish:

1. `Summary` - Describes what constitutes the Pull Request and what changes you have made to the code.
2. `Changelog` - Choose one or more, this is used for tagging label, then used to generate the changelog.
3. `Related Issues` - Fixes which issue, the issue will be closed once the commit is merged into the main brach.

### PR Commit Message

Format: `<type>(<scope>): <subject>`

`<scope>` is optional

```
fix(functions): fix group by string bug
^--^  ^------------^
|     |
|     +-> Summary in present tense.
|
+-------> Type: chore, docs, feat, fix, refactor, style, or test.
```

More types:

- `feat`: (new feature for the user)
- `fix`: (bug fix for the user)
- `docs`: (changes to the documentation)
- `style`: (formatting, missing semi colons, etc; no production code change)
- `refactor`: (refactoring production code, eg. renaming a variable)
- `test`: (adding missing tests, refactoring tests; no production code change)
- `chore`: (updating grunt tasks etc; no production code change)

## Testing

*Unit tests*

```shell
$ make unit-test
```

*Stateless tests*

```shell
$ make stateless-test
```

## Issues

Databend uses [GitHub issues](https://github.com/datafuselabs/databend/issues) to track bugs. Please include necessary information and instructions to reproduce your issue.

## Documentation

All developer documentation is published on the Databend developer site, [databend.rs](https://databend.rs). 

## Code of Conduct

Please refer to the [Code of Conduct](/dev/policies/code-of-conduct), which describes the expectations for interactions within the community.

