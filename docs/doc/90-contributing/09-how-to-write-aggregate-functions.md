---
title: How to Write Aggregate Functions
---

An aggregate function, or aggregation function, is a function that combines values from multiple rows to form a single summary value. Common aggregate functions include sum, count, avg, and others.

## How to write aggregate functions

Databend allows us to write custom aggregate functions through rust code.

It's not an easy way because you need to be a rustacean first. Databend has a plan to support writing UDAFs in other languages(like js, web assembly) in the future.

In this section we will talk about how to write aggregate functions in Databend.

## Function Registration

The register function for aggregate functions takes a lowercase function name and an `AggregateFunctionDescription` object as parameters. Each registered function is stored in  `case_insensitive_desc` (HashMap data structure).

The `case_insensitive_combinator_desc` is used to store some combined functions, such as `count_if` and `sum_if` that are combined with the `_if` suffix.

```rust
pub struct AggregateFunctionFactory {
    case_insensitive_desc: HashMap<String, AggregateFunctionDescription>,
    case_insensitive_combinator_desc: Vec<(String, CombinatorDescription)>,
}
impl AggregateFunctionFactory {
  ...
pub fn register(&mut self, name: &str, desc: AggregateFunctionDescription) {
        let case_insensitive_desc = &mut self.case_insensitive_desc;
        case_insensitive_desc.insert(name.to_lowercase(), desc);
    }
  ...
}
```

Each registered function must implement both the `AggregateFunction` trait and the `AggregateFunctionFeatures` trait, where `AggregateFunctionFeatures` is more similar to `FunctionProperty` in `Scalar` in that they both store some properties of functions.

```rust
pub type AggregateFunctionRef = Arc<dyn AggregateFunction>;
pub type AggregateFunctionCreator =
    Box<dyn Fn(&str, Vec<Scalar>, Vec<DataType>) -> Result<AggregateFunctionRef> + Sync + Send>;
pub struct AggregateFunctionDescription {
    pub(crate) aggregate_function_creator: AggregateFunctionCreator,
    pub(crate) features: AggregateFunctionFeatures,
}
```

## Function Composition

Unlike `Scalar`, which directly uses a Struct, `AggregateFunction` is a trait. Because aggregate functions accumulate data from the block and generate some intermediate results during the accumulation process.

Therefore, **Aggregate Function** must have an initial state, and the results gen*erated during the aggregation process must be **mergeable** and **serializable**.

The main functions are:

- **name** represents the name of the function being registered, such as avg, sum, etc.
- **return_type** represents the return type of the registered function. The return value of the same function may change due to different parameter types. For example, the parameter of `sum(int8)` is of type `i8`, but the return value may be `int64`.
- **init_state** is used to initialize the state of the aggregate function.
- **state_layout** is used to represent the size of the **state** in memory and the arrangement of memory blocks.
- **accumulate** is used for `SingleStateAggregator`. That is, the entire block can be aggregated under a single state without any keys. For example, when `selecting count(*) from t`, there is no grouping column in the query, and the `accumulate` function will be called.
- **accumulate_keys** is used for `PartialAggregator`. Here, `key` and `offset` need to be considered, where each key represents a unique memory address, denoted as the function parameter place.
- **serialize** serializes the **state** in the aggregation process into binary.
- **deserialize** deserializes the binary into **state**.
- **merge** is used to* merge other state into the current state.
- **merge_result** can merge the **Aggregate Function state** into a single value.

## Example

**Take avg as an example**

The implementation details can be found in [aggregate_avg.rs](https://github.com/datafuselabs/databend/blob/d5e06af03ba0f99afdd6bdc974bf2f5c1c022db8/src/query/functions/src/aggregates/aggregate\_avg.rs).

Because we need to accumulate each value and divide by the total number of non-null rows. Therefore, the avg function is defined as the `struct AggregateAvgFunction<T, SumT>`. Where `T` and `SumT` are logical types that implement [Number](https://github.com/datafuselabs/databend/blob/2aec38605eebb7f0e1717f7f54ec52ae0f2e530b/src/query/expression/src/types/number.rs).

During the aggregation process, avg will generate the total sum of accumulated values and the number of non-null rows already scanned. Therefore, `AggregateAvgState` can be defined as the following structure.

```rust
#[derive(Serialize, Deserialize)]
struct AggregateAvgState<T: Number> {
    #[serde(bound(deserialize = "T: DeserializeOwned"))]
    pub value: T,
    pub count: u64,
}
```

- **return_type** is set to `Float64Type`. For example, `value = 3`, `count = 2`, and `avg = value/count`.
- **init_state** initializes the state with the value of `T`'s default and count set to `0`.
- **accumulate** accumulates count and value of `AggregateAvgState` for non-null rows in the block.
- **accumulate_keys** gets the corresponding state value with `place.get::<AggregateAvgState<SumT>>()`, and updates it.

```rust
fn accumulate_keys(
    &self,
    places: &[StateAddr],
    offset: usize,
    columns: &[Column],
    _input_rows: usize,
) -> Result<()> {
    let darray = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();
    darray.iter().zip(places.iter()).for_each(|(c, place)| {
        let place = place.next(offset);
        let state = place.get::<AggregateAvgState<SumT>>();
        state.add(c.as_(), 1);
    });
    Ok(())
}
```

## Refer to other examples

As you see, adding a new aggregate function in Databend is not as hard as you think.
Before you start to add one, please refer to other aggregate function examples, such as `sum`, `count`.

- [sum](https://github.com/datafuselabs/databend/blob/d5e06af03ba0f99afdd6bdc974bf2f5c1c022db8/src/query/functions/src/aggregates/aggregate\_sum.rs)
- [count](https://github.com/datafuselabs/databend/blob/d5e06af03ba0f99afdd6bdc974bf2f5c1c022db8/src/query/functions/src/aggregates/aggregate\_count.rs)

## Testing

To be a good engineer, don't forget to test your codes, please add unit tests and stateless tests after you finish the new aggregate functions.

### Unit Test

The unit tests for aggregate functions are located in [agg.rs](https://github.com/datafuselabs/databend/blob/d5e06af03ba0f99afdd6bdc974bf2f5c1c022db8/src/query/functions/tests/it/aggregates/agg.rs).

### Logic Test

The logic tests for functions are located in [tests/logictest/suites/base/02\_function/](https://github.com/datafuselabs/databend/tree/d5e06af03ba0f99afdd6bdc974bf2f5c1c022db8/tests/sqllogictests/suites/query/02\_function).
