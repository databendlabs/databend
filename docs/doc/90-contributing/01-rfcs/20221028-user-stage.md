---
title: User Stage
---

- RFC PR: [datafuselabs/databend#8519](https://github.com/datafuselabs/databend/pull/8519)
- Tracking Issue: [datafuselabs/databend#8520](https://github.com/datafuselabs/databend/issues/8520)

## Summary

Supports user internal stage.

## Motivation

Databend only supports named internal stage:

```sql
CREATE STAGE @my_stage;
COPY INTO my_table FROM @my_stage;
```

However, named internal stages are complex to be used in some cases. Especially for users who only use stages to load data. By supporting the user stage, they can copy data more efficiently:

```sql
COPY INTO my_table from @~;
```

## Guide-level explanation

Databend will add support for user stage. Every sql user will have its own stage, which can be referred to by `~`.

Users can use `~` everywhere like a named stage:

```sql
COPY INTO my_table FROM @~;
LIST @~;
PRESIGN @~/data.csv;
REMOVE @~ PATTERN = 'ontime.*';
```

User stage is the internal anonymous stage for sql user, so users can't:

- create
- drop
- alter

And users can't set format options for user stage. They need to specify the format settings during `COPY`.

## Reference-level explanation

Databend now has two different [`StageType`](https://github.com/datafuselabs/databend/blob/c2d4e9d3e0a5bf7d54a2a6ce1db1d41b00cd2cd1/src/meta/types/src/user_stage.rs#L52-L55):

```rust
pub enum StageType {
    Internal,
    External,
}
```

Databend will generate a unique prefix for the internal stage, like `stage/{stage_name}`.

We will add two new stage types:

```rust
pub enum StageType {
    LegacyInternal,
    External,
    Internal,
    User,
}
```

`StageType::Internal` will deprecate `StageType::LegacyInternal`. Since this RFC, we will not create a new stage with `StageType::LegacyInternal` anymore.

The stage prefix rule will be:

- `LegacyInternal` => `stage/{stage_name}`
- `External` => spcified location.
- `Internal` => `stage/internal/{stage_name}`
- `User` => `stage/user/{user_name}`

Notes: `StageType::User` will not be stored in metasrv and will constantly build in memory directly.

## Drawbacks

None.

## Rationale and alternatives

### Preserve stage name prefix

To simplify, we can preserve all stages prefixed by `bend_internal_`. Users can't create and drop stages with this prefix.

By adding this limitation, we can implement the user stage easier. Every time user tries to access their own user stage, we will expand to `bend_internal_user_<user_name>`.

Take user `root` as an example:

```sql
COPY INTO my_table FROM @~;
```

will be transformed into:

```sql
COPY INTO my_table FROM @bend_internal_user_root;
```

Users can only access their user stage by `@~`. Visit `@bend_internal_user_root` will always return an error.

### Create stage with UUID in metasrv

We can create a stage with UUID for first-time users to access their user stage.

## Prior art

None

## Unresolved questions

None

## Future possibilities

### Table Stage

We can introduce the table stage like snowflake does:

```sql
COPY INTO my_table FROM @#my_table;
```

### Cleanup while drop users

Users' stage should be purged while dropping user.

### Garbage Collection for user stage

We can support garbage collection for user stages so that obsoleted files can be removed.
