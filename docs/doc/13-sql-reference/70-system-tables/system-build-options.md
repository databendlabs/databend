---
title: system.build_options
---

This system table describes the build options of the current Databend release.

- `cargo_features`: The package features have been enabled, which are listed in the `[features]` section of `Cargo.toml` .
- `target_features`: The platform features that have been enabled for the current compilation target. Ref: [Conditional Compilation - `target_feature`](https://doc.rust-lang.org/reference/conditional-compilation.html#target_feature) .

```sql
SELECT * FROM system.build_options;
+----------------+---------------------+
| cargo_features | target_features     |
+----------------+---------------------+
| default        | fxsr                |
|                | llvm14-builtins-abi |
|                | sse                 |
|                | sse2                |
+----------------+---------------------+
4 rows in set (0.031 sec)
```