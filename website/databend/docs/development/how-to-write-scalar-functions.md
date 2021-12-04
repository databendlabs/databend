---
title: How to write scalar functions
---

## What's scalar functions

Scalar functions (sometimes referred to as User-Defined Functions / UDFs) return a single value as a return value for each row, not as a result set, and can be used in most places within a query or SET statement, except for the FROM clause.

```
One to One Mapping execution

┌─────┐                    ┌──────┐
│  a  │                    │   x  │
├─────┤                    ├──────┤
│  b  │                    │   y  │
├─────┤    ScalarFunction  ├──────┤
│  c  │                    │   z  │
├─────┼────────────────────►──────┤
│  d  │     Exec           │   u  │
├─────┤                    ├──────┤
│  e  │                    │   v  │
├─────┤                    ├──────┤
│  f  │                    │   w  │
└─────┘                    └──────┘
```



## ScalarFunction trait introduction

All scalar functions implement `Function` trait, and we register them into a global static `FunctionFactory`, the factory is just an index map and the key is the name of the scalar function.

:::note
    Function name in Databend is case-insensitive.
:::

``` rust

pub trait Function: fmt::Display + Sync + Send + DynClone {
    fn name(&self) -> &str;

    fn num_arguments(&self) -> usize {
        0
    }

    // (1, 2) means we only accept [1, 2] arguments
    // None means it's not variadic function
    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        None
    }

    // return monotonicity node, should always return MonotonicityNode::Function
    fn get_monotonicity(&self, _args: &[MonotonicityNode]) -> Result<MonotonicityNode> {
        Ok(MonotonicityNode::Function(Monotonicity::default(), None))
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType>;
    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool>;
    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn>;
}

```

### Understand the functions

- The function `name` indicates the name of this function, such as `log`, `sign`. Sometimes we should store the name inside the function, because different names may share the same function, such as `pow` and `power`, we can take `power` as the alias(synonyms) function of `pow`.
- The function `num_arguments` indicates how many arguments can this function accept.
- Some functions may accept variadic arguments, that's the `variadic_arguments` function works. For example, `round` function accepts one or two functions, its range is [1,2], we use closed interval here.
- The function `get_monotonicity` indicates the monotonicity of this function, it can be used to optimize the execution.
- The function `return_type` indicates the return type of this function, we can also validate the `args` in this function.
- The function `nullable` indicates whether this function can return a column with a nullable field(For the time being, return true/false is ok).
- `eval` is the main function to execute the ScalarFunction, `columns` is the input columns, `input_rows` is the number of input rows, we will explain how to write `eval` function later.


### Knowledge before writing the eval function

####  Logical datatypes and physical datatypes.

Logical datatypes are the datatypes that we use in Databend, and physical datatypes are the datatypes that we use in the execution/compute engine.
Such as `Date32`, it's a logical data type, but its physical is `Int32`, so its column is represented by `DFInt32Array`.

We can get logical datatype by `data_type` function of `DataField` , and the physical datatype by `data_type` function in `DataColumn`.
`DataColumnsWithField` has `data_type` function which returns the logical datatype.

####  Arrow's memory layout

Databend's memory layout is based on the Arrow system, you can find Arrow's memory layout [here] (https://arrow.apache.org/docs/format/Columnar.html#format-columnar).

For example a primitive array of int32s:

[1, null, 2, 4, 8]
Would look like this:


```
* Length: 5, Null count: 1
* Validity bitmap buffer:

  |Byte 0 (validity bitmap) | Bytes 1-63            |
  |-------------------------|-----------------------|
  | 00011101                | 0 (padding)           |

* Value Buffer:

  |Bytes 0-3   | Bytes 4-7   | Bytes 8-11  | Bytes 12-15 | Bytes 16-19 | Bytes 20-63 |
  |------------|-------------|-------------|-------------|-------------|-------------|
  | 1          | unspecified | 2           | 4           | 8           | unspecified |

```

#### Constant column

Sometimes column is constant in the block, such as: `select 3 from table`, the column 3 is always 3, so we can use a constant column to represent it. This is useful to save the memory during computation.

So databend's DataColumn is represented by:

```
pub enum DataColumn {
    // Array of values. Series is wrap of arrow's array
    Array(Series),
    // A Single value.
    Constant(DataValue, usize),
}
```


### Writing function guidelines

#### Column Casting
To execute the scalar function, we surely need to iterate the input columns of arguments.
Since we already check the datatypes in `return_type` function, so we can cast the input columns to the specific columns like `DFInt32Array` using `i32` function.

#### Constant Column case
Noticed that we should take care of the Constant Column match case to improve memory usage.


#### Column Iteration and validity bitmap combination
To iterate the column, we can use `column.iter()` to generate an iterator, the item is `Option<T>`, `None` means it's null.
But this's inefficient because we need to check the null value every time we iterate the column inside the loop which will pollute the CPU cache.

According to the memory layout of Arrow, we can directly use the validity bitmap of the original column to indicate the null value.
So we have `ArrayApply` trait to help you iterate the column. If there are two zip iterator, we can use `binary` function to combine the validity bitmap of two columns.

ArrayApply
```rust
let array: DFUInt8Array = self.apply_cast_numeric(|a| {
                            AsPrimitive::<u8>::as_(a - (a / rhs) * rhs)
                        });
```

binary
```rust
binary(x_series.f64()?, y_series.f64()?, |x, y| x.pow(y))
```

### Nullable check
Nullable is anoying, but we can accept `DataType::Null` argument in most cases.

### Implicit cast
Databend can accept implicit cast, eg: `pow('3', 2)`, `sign('1232')` we can cast the argument to specific column using `cast_with_type`.



## Refer to other examples
As you see, adding a new scalar function in Databend is not as hard as you think.
Before you start to add one, please refer to other scalar function examples, such as `sign`, `expr`, `tan`, `atan`.

## Testing
To be a good engineer, don't forget to test your codes, please add unit tests and stateless tests after you finish the new scalar functions.

## Summary
We welcome all community users to contribute more powerful functions to Databend. If you find any problems, feel free to [open an issue](https://github.com/datafuselabs/databend/issues) in GitHub, we will use our best efforts to help you.
