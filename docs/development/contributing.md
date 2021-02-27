---
id: development-contributing
title: Contribution Guide
---

Our goal is to make contributing to the FuseQuery project easy and transparent.

## Contributing to FuseQuery

To contribute to FuseQuery, ensure that you have the latest version of the codebase, run the following:
```
$ git clone https://github.com/datafusedev/fuse-query
$ cd fuse-query
$ make setup
$ source ~/.cargo/env
$ make build
$ make test
```

## Coding Guidelines

For detailed guidance on how to contribute to the codebase refer to [Coding Guidelines](coding-guidelines.md).

## Documentation

All developer documentation is published on the DataFuse developer site. 

## Pull Requests

To submit your pull request:

1. Fork the `fuse-query` repo and create your branch from `master`.
2. If you have added code that should be tested, add unit tests.
3. If you have made changes to APIs, update the relevant documentation, and build and test the developer site.
4. Verify and ensure that the test suite passes.
5. Make sure your code passes both linters.
6. Submit your pull request.

## Code of Conduct
Please refer to the [Code of Conduct](../policies/code-of-conduct.md), which describes the expectations for interactions within the community.

## Issues

FuseQuery uses [GitHub issues](https://github.com/datafusedev/fuse-query/issues) to track bugs. Please include necessary information and instructions to reproduce your issue. 