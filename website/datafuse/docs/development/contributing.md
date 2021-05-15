---
id: development-contributing
title: Contribution Guide
---

## Contributing to Datafuse

Datafuse is an open project, and you can contribute to it in many ways. You can help with ideas, code, or documentation. We appreciate any efforts that help us to make the project better.

Our goal is to make contributing to the Datafuse project easy and transparent.

Thank you.

!!! note "Notes"
    Once the code been merged, your name will be stoned in the `system.contributors` table forever.

    `SELECT * FROM system.contributors`


## Contributing

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

All developer documentation is published on the Datafuse developer site, [datafuse.rs](https://datafuse.rs). 

## Pull Requests

To submit your pull request:

1. Fork the `datafuse` repo and create your branch from `master`.
2. Open an regular [issue](https://github.com/datafuselabs/datafuse/issues/new/choose) for binding the pull request.
3. Submit a [draft pull requests](https://github.blog/2019-02-14-introducing-draft-pull-requests/), tag your work in progress.
4. If you have added code that should be tested, add unit tests.
5. Verify and ensure that the unit test suite passes, `make test`.
6. Verify and ensure that stateless test passes:
   `cd tests` and `./fuse-test`

7. Make sure your code passes both linters, `make lint`.
8. Change the status to “Ready for review”.

## Code of Conduct
Please refer to the [Code of Conduct](../policies/code-of-conduct.md), which describes the expectations for interactions within the community.

## Issues

Datafuse uses [GitHub issues](https://github.com/datafuselabs/datafuse/issues) to track bugs. Please include necessary information and instructions to reproduce your issue. 
