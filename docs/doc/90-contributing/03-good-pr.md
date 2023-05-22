---
title: Your First PR for Databend
sidebar_label: First Pull Request
description:
  You first good Databend pull request
---

[Databend](https://github.com/datafuselabs/databend) is an open-source project where everyone can contribute their code and make their creative ideas into a reality. This topic helps new contributors understand how to create a pull request for Databend on GitHub and provides useful considerations for code contributions.

:::tip
**Did You Know?** To show our appreciation, your GitHub username will be added to the table `system.contributors` after your code is successfully merged.

Check it out:

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

## Contributing Documentation

We welcome you to contribute to the Databend documentation, alongside your code! The Databend documentation is composed of markdown files, which makes it easy to write and maintain. To ensure high-quality documentation, we recommend using [Visual Studio Code](https://code.visualstudio.com/) as your markdown editor. Additionally, installing the [Code Spell Checker](https://marketplace.visualstudio.com/items?itemName=streetsidesoftware.code-spell-checker) extension will help you catch any spelling mistakes.

When submitting a document for a new command or function, it is crucial to adhere to the provided templates and guidelines to maintain consistency and ensure clarity in the Databend documentation. These templates are designed to include all the necessary information and formatting conventions, making it easier for users to understand and use the commands or functions.

- [command-template](https://github.com/datafuselabs/databend/blob/d3a40d91b8a8ebaf878344e024164f36b6db5615/docs/public/templates/command-template.md?plain=1)
- [function-template](https://github.com/datafuselabs/databend/blob/d3a40d91b8a8ebaf878344e024164f36b6db5615/docs/public/templates/function-template.md?plain=1)

In addition to following the templates, please pay attention to the code included within them. The code serves the purpose of explicitly indicating whether the new command or function is exclusively available for Databend Enterprise and provides information about when the command or function was introduced. This contextual information is crucial for users as it allows them to accurately assess the scope and limitations of the feature. It also enables users to identify the specific versions of Databend that support the feature, enabling them to plan their usage effectively.

## Pull Requests

### Submit a PR

1. Fork the `databend` repo and create your branch from `main`.
2. Open a regular [issue](https://github.com/datafuselabs/databend/issues/new/choose) for binding the pull request.
3. Submit a [Draft Pull Requests](https://github.blog/2019-02-14-introducing-draft-pull-requests/), tag your work in progress.
4. If you have added code that should be tested, add unit tests.
5. Verify and ensure that the test suites passes, `make test`.
6. Make sure your code passes both linters, `make lint`.
7. Change the status to “Ready for review”.
8. Watch out the replies from the `@mergify`, she will be your guide.

### PR Title

Format: `<type>(<scope>): <subject>`

`<scope>` is optional

```
fix(query): fix group by string bug
^--^  ^------------^
|     |
|     +-> Summary in present tense.
|
+-------> Type: rfc, feat, fix, refactor, ci, docs, chore
```

More types:

- `rfc`: this PR proposes a new RFC
- `feat`: this PR introduces a new feature to the codebase
- `fix`: this PR patches a bug in codebase
- `refactor`: this PR changes the code base without new features or bugfix
- `ci`: this PR changes build/ci steps
- `docs`: this PR changes the documents or websites
- `chore`: this PR only has small changes that no need to record, like coding styles.

### PR Template

Databend has a [Pull Request Template](https://github.com/datafuselabs/databend/blob/main/.github/PULL_REQUEST_TEMPLATE.md):

```shell
I hereby agree to the terms of the CLA available at: https://databend.rs/dev/policies/cla/

## Summary

Summary about this PR

Fixes #issue
```

You should not change the PR template context, but need to finish:

- `Summary` - Describes what constitutes the Pull Request and what changes you have made to the code. For example, fixes which issue.

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

