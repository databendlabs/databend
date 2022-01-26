---
title: Contribution Guide
sidebar_position: 1
---

## Contributing to Databend

Databend is an open project, and you can contribute to it in many ways. You can help with ideas, code, or documentation. We appreciate any efforts that help us to make the project better.

Our goal is to make contributing to the Databend project easy and transparent.

Thank you.

:::note Notes
Once the code been merged, your name will be stoned in the `system.contributors` table forever.

`SELECT * FROM system.contributors`
:::

## Coding Guidelines

For detailed guidance on how to contribute to the codebase refer to [Coding Guidelines](coding-guidelines.md).

## Documentation

All developer documentation is published on the Databend developer site, [databend.rs](https://databend.rs). 

## Pull Requests

To submit your pull request:

1. Fork the `databend` repo and create your branch from `main`.
2. Open an regular [issue](https://github.com/datafuselabs/databend/issues/new/choose) for binding the pull request.
3. Submit a [draft pull requests](https://github.blog/2019-02-14-introducing-draft-pull-requests/), tag your work in progress.
4. If you have added code that should be tested, add unit tests.
5. Verify and ensure that the test suites passes, `make test`.
6. Make sure your code passes both linters, `make lint`.
7. Change the status to “Ready for review”.
8. Watch out the replies from the @datafuse-bots, she will be your guide.

## Code of Conduct

Please refer to the [Code of Conduct](/dev/policies/code-of-conduct), which describes the expectations for interactions within the community.

## Issues

Databend uses [GitHub issues](https://github.com/datafuselabs/databend/issues) to track bugs. Please include necessary information and instructions to reproduce your issue. 
