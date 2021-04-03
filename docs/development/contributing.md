---
id: development-contributing
title: Contribution Guide
---

Our goal is to make contributing to the Datafuse project easy and transparent.

## Contributing to Datafuse

To contribute to Datafuse, ensure that you have the latest version of the codebase, run the following:
```
$ git clone https://github.com/datafuselabs/datafuse
$ cd datafuse
$ make setup
$ make build
$ make test
```

## Coding Guidelines

For detailed guidance on how to contribute to the codebase refer to [Coding Guidelines](coding-guidelines.md).

## Documentation

All developer documentation is published on the DataFuse developer site. 

## Pull Requests

To submit your pull request:

1. Fork the `datafuse` repo and create your branch from `master`.
2. Submit a [draft pull requests](https://github.blog/2019-02-14-introducing-draft-pull-requests/), tag your work in progress.
3. If you have added code that should be tested, add unit tests.
4. If you have made changes to APIs, update the relevant documentation, and build and test the developer site.
5. Verify and ensure that the test suite passes, `make test`.
6. Make sure your code passes both linters, `make lint`.
7. Change the status to “Ready for review”.

## Code of Conduct
Please refer to the [Code of Conduct](../policies/code-of-conduct.md), which describes the expectations for interactions within the community.

## Issues

Datafuse uses [GitHub issues](https://github.com/datafuselabs/datafuse/issues) to track bugs. Please include necessary information and instructions to reproduce your issue. 
