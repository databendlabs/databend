---
id: how-to-write-aggregate-functions.md
title: How to write aggregate functions
---

## How to write aggregate functions

Datafuse allows you to write custom aggregate functions through rust codes.
It's not an easy way because you need to be rustacean first. Datafuse has a plan to support writing UDAF in other languages(like js, web assembly) in the future.

In this section we will talk about how to write aggregate functions in datatfuse.


## AggregateFunction trait introduction

All aggregate functions implement `AggregateFunction` trait, and we register them into a global static factory named `FactoryFuncRef`, the factory is just an index map and the keys are names of aggregate functions, noted that the function's name in datafuse are all case insensitive.


``` rust
pub trait AggregateFunction: fmt::Display + Sync + Send {
    fn name(&self) -> &str;
    fn return_type(&self) -> Result<DataType>;
    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool>;

    fn init_state(&self, place: StateAddr);
    fn state_layout(&self) -> Layout;

    fn accumulate(&self, _place: StateAddr, _arrays: &[Series], _input_rows: usize) -> Result<()>;

    fn accumulate_keys(
        &self,
        _places: &[StateAddr],
        _offset: usize,
        _arrays: &[Series],
        _input_rows: usize,
    ) -> Result<()>;

    fn serialize(&self, _place: StateAddr, _writer: &mut BytesMut) -> Result<()>;

    fn deserialize(&self, _place: StateAddr, _reader: &mut &[u8]) -> Result<()>;

    fn merge(&self, _place: StateAddr, _rhs: StateAddr) -> Result<()>;

    fn merge_result(&self, _place: StateAddr) -> Result<DataValue>;
}
```

### Understand the functions

- The function `name` indicates the name of this function, such as `sum`, `min`
- The function `return_type` indicates the return type of the function, it may vary with different arguments, such as `sum(int8)` -> `int64`, `sum(uint8)` -> `uint64`, `sum(float64)` -> `float64`.
- The function `nullable` indicates whether the `return_type` is nullable.

Before we start to introduce function `init_state`, let's ask a question:

 >  what's aggregate function state?

It indicates the temporary result of the aggregate function. Because aggregate function accumulates block by block and there will results from different query nodes after the accumulations. The state must be mergeable, serializable.

For example, in the `avg` aggregate function, we can represent the state like:

```
struct AggregateAvgState<T: BinarySer + BinaryDe> {
    pub value: T,
    pub count: u64,
}
```

- The function `init_state` wants us to initialize the aggregate function state, we ensure the memory is already allocated, we just need to initial the state with the initial value.
- The function `state_layout` indicates the memory layout of the state.
- The function `accumulate`, this function is used in aggregation with single batch, which means the whole block can be aggregated in a single state, no more other keys. A SQL query which applys aggregation without group-by columns will hit this function.

Noted that the argument `_arrays` is the function arguments, we can safely get the array by index without index bound check because we must validate the argument numbers and types in function constructor.

 The `_input_rows` is the rows of current block, it maybe useful when the `_arrays` is empty, eg: `count()` function.


- The function `accumulate_keys`, similar to `accumulate` but we must take into consideration of the `keys` and `offset`, each key reprsents an unique memory address named `place`.
- The function `serialize`, we used to serialize state into binary.
- The function `deserialize`, we used to deserialize state from binary.
- The function `merge`, can be used to merge other state into current state.
- The function `merge_result`, can be used to represent the aggregate function state into one-row field.

## Refer to other examples
As you see, adding a new aggregate function in datafuse is not as hard as you think.
Before we start to add one, please refer to other aggregate function examples, such like `min`, `count`, `sum`, `avg`.

## Summary
Welcome all community users to contribute more powerful functions into datafuse. If you found any problems, feel free to open an issue in Github, we will try our best to help you.
