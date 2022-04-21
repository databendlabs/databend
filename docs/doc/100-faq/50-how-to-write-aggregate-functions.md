---
title: How to Write Aggregate Functions
---

## How to write aggregate functions

Databend allows us to write custom aggregate functions through rust code.
It's not an easy way because you need to be a rustacean first. Databend has a plan to support writing UDAFs in other languages(like js, web assembly) in the future.

In this section we will talk about how to write aggregate functions in Databend.


## AggregateFunction trait introduction

All aggregate functions implement `AggregateFunction` trait, and we register them into a global static `FunctionFactory`, the factory is just an index map and the key is the name of the aggregate function.

:::tip
Function name in Databend is case-insensitive.
:::

``` rust
pub trait AggregateFunction: fmt::Display + Sync + Send {
    ...
}

### Understand the functions

- The function `name` indicates the name of this function, such as `sum`, `min`.
- The function `return_type` indicates the return type of the function, it may vary with different arguments, such as `sum(int8)` -> `int64`, `sum(uint8)` -> `uint64`, `sum(float64)` -> `float64`.
- The function `nullable` indicates whether the `return_type` is nullable or not.

Before we start to introduce the function `init_state`, let's ask a question first:

>  what's aggregate function state?

It indicates the temporary results of an aggregate function. Because an aggregate function accumulates data in columns block by block and there will be some intermediate results after the aggregation. Therefore, the state must be mergeable, serializable.

For example, in the `avg` aggregate function, we can represent the state like:

```rust
struct AggregateAvgState<T: PrimitiveType> {
    #[serde(bound(deserialize = "T: DeserializeOwned"))]
    pub value: T,
    pub count: u64,
}

```

- The function `init_state` initializes the aggregate function state, we ensure the memory is already allocated, and we just need to initial the state with the initial value.
- The function `state_layout` indicates the memory layout of the state.
- The function `accumulate` is used in aggregation with a single batch, which means the whole block can be aggregated in a single state, no other keys. The SQL query, which applies aggregation without group-by columns, will hit this function.

Noted that the argument `_arrays` is the function arguments, we can safely get the array by index without index bound check because we must validate the argument numbers and types in function constructor.

The `_input_rows` is the rows of the current block, and it may be useful when the `_arrays` is empty, e.g., `count()` function.


- The function `accumulate_keys` is similar to accumulate, but we must take into consideration the keys and offsets, for which each key represents a unique memory address named place.
- The function `serialize` serializes state into binary.
- The function `deserialize` deserializes state from binary.
- The function `merge`, can be used to merge other state into current state.
- The function `merge_result`, can be used to represent the aggregate function state into one-row field.


## Example
Let's take an example of aggregate function `sum`.

It's declared as `AggregateSumFunction<T, SumT>`, we can accept varying integer types like `u8`, `i8`. `T` and `SumT` is logic types which implement `DFPrimitiveType`. e.g., `T` is `u8` and `SumT` must be `u64`.

Also, we can dispatch it using macros by matching the types of the arguments. Take a look at the `with_match_primitive_type` to understand the dispatch macros.

The `AggregateSumState` will be

```rust
struct AggregateSumState<T> {
    pub value: T,
}
```

The generic `T` is from `SumT`.

Let's take into the function `accumulate_keys`, because this is the only function that a little hard to understand in this case.

```sql
fn accumulate_keys(
    &self,
    places: &[StateAddr],
    offset: usize,
    columns: &[ColumnRef],
    _input_rows: usize,
) -> Result<()> {
    if columns[0].data_type().data_type_id() == TypeID::Boolean {
        // boolean cast into u8 column
        // ...
    } else {
        let darray: &PrimitiveColumn<T> = unsafe { Series::static_cast(&columns[0]) };
        darray.iter().zip(places.iter()).for_each(|(c, place)| {
            let place = place.next(offset);
            let state = place.get::<AggregateSumState<SumT>>();
            state.add(c.as_());
        });
    }

    Ok(())
}
```

The `places` is the memory address of the first state in this row, so we can get the address of `AggregateSumState<T>` using `places[row] + offset`, then using `place.get::<AggregateSumState<SumT>>()` to get the value of the corresponding state.

Since we already know the array type of this function, we can safely cast it to arrow's `PrimitiveArray<T>`, here we make two branches to reduce the branch prediction of CPU, `null` and `no_null`. In `no_null` case, we just iterate the array and apply the `sum`, this is good for compiler to optimize the codes into vectorized codes.

Ok, this example is pretty easy. If you already read this, you may have the ability to write a new function.


## Testing
To be a good engineer, don't forget to test your codes, please add unit tests and stateless tests after you finish the new aggregate functions.

- [Unit tests](https://github.com/datafuselabs/databend/blob/a0869e8ac3cc70105822e6f0aaddfa534c4270d2/common/functions/tests/it/aggregates/aggregate_function.rs)

- Stateless tests:

```sql
SELECT sum(number), sum(-1), sum(2.3)  from numbers(3);
+-------------+---------+--------------------+
| sum(number) | sum(-1) | sum(2.3)           |
+-------------+---------+--------------------+
|           3 |      -3 | 6.8999999999999995 |
+-------------+---------+--------------------+
```

## Refer to other examples
As you see, adding a new aggregate function in Databend is not as hard as you think.
Before you start to add one, please refer to other aggregate function examples, such as `min`, `count`, `max`, `avg`.

## Summary
We welcome all community users to contribute more powerful functions to Databend. If you find any problems, feel free to [open an issue](https://github.com/datafuselabs/databend/issues) in GitHub, we will use our best efforts to help you.
