# Databend Workspace Hack

This crate is hack to unify 3rd party dependency crate.

- On the one hand, features that can be uniformly enabled to optimize compiler caching.
- On the other hand, versions of dependencies can be managed to avoid potential problems.

The downside is that all code is compiled with the features needed for any invocation.
