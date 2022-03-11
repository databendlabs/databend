---
title: How to write scalar functions
---

## What's scalar functions

Scalar functions (sometimes referred to as User-Defined Functions / UDFs) return a single value as a return value for each row, not as a result set, and can be used in most places within a query or SET statement, except for the FROM clause.

```text
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
    /// Returns the name of the function, should be unique.
    fn name(&self) -> &str;

    // Returns the number of arguments the function accepts.
    fn num_arguments(&self) -> usize {
        0
    }

    /// (1, 2) means we only accept [1, 2] arguments
    /// None means it's not variadic function
    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        None
    }

    /// Calculate the monotonicity from arguments' monotonicity information.
    /// The input should be argument's monotonicity. For binary function it should be an
    /// array of left expression's monotonicity and right expression's monotonicity.
    /// For unary function, the input should be an array of the only argument's monotonicity.
    /// The returned monotonicity should have 'left' and 'right' fields None -- the boundary
    /// calculation relies on the function.eval method.
    fn get_monotonicity(&self, _args: &[Monotonicity]) -> Result<Monotonicity> {
        Ok(Monotonicity::default())
    }

    /// The method returns the return_type of this function.
    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataTypeAndNullable>;

    /// Evaluate the function, e.g. run/execute the function.
    fn eval(&self, _columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn>;

    /// Whether the function passes through null input.
    /// Return true is the function just return null with any given null input.
    /// Return false if the function may return non-null with null input.
    ///
    /// For example, arithmetic plus('+') will output null for any null input, like '12 + null = null'.
    /// It has no idea of how to handle null, but just pass through.
    ///
    /// While ISNULL function  treats null input as a valid one. For example ISNULL(NULL, 'test') will return 'test'.
    fn passthrough_null(&self) -> bool {
        true
    }
}

```

### Understand the functions

- The function `name` indicates the name of this function, such as `log`, `sign`. Sometimes we should store the name inside the function, because different names may share the same function, such as `pow` and `power`, we can take `power` as the alias(synonyms) function of `pow`.
- The function `num_arguments` indicates how many arguments can this function accept.
- Some functions may accept variadic arguments, that's the `variadic_arguments` function works. For example, `round` function accepts one or two functions, its range is [1,2], we use closed interval here.
- The function `get_monotonicity` indicates the monotonicity of this function, it can be used to optimize the execution.
- The function `return_type` indicates the return type of this function, we can also validate the `args` in this function.
- The function `nullable` indicates whether this function can return a column with a nullable field. The default behavior is checking if any nullable input column exists, return true if it does exist, otherwise false.
- `eval` is the main function to execute the ScalarFunction, `columns` is the input columns, `input_rows` is the number of input rows, we will explain how to write `eval` function false.
- `passthrough_null` indicates whether the function return null if one of the input is null. For most function it should be true (which is the default behavior). But some exceptions do exist, like `ISNULL, CONCAT_WS` etc.

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


```text
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

```rust
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
Most functions have default null behavior, see [Pass through null](#pass-through-null). We don't need to combine validity bitmap. But for functions that can produce valid result with null input, we need to generate our own bitmap inside the function `eval`.

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

### Return type
The return value of `return_type` is `DataTypeAndNullable`, which is a wrapper of `DataType` and `nullable`. Most functions produces nullable data type only when one of the input data types is nullable. Some exceptions include `sqrt(-1)`,`from_base64("1")`, etc.

### Implicit cast
Databend can accept implicit cast, eg: `pow('3', 2)`, `sign('1232')` we can cast the argument to specific column using `cast_with_type`.

### Pass through null
Most functions have default null behavior (thus you don't need to override), that is, a null value in any of the input produces a null result. For functions like this, the `eval` method doesn't need to conditionally calculate the result with checking the input is null or not. The bitmap with null bits will be applied outside the function by masking the return column. For example, arithmetic plus operation with inputs`[1, None, 1], [None, 2, 2]` doesn't need to check if every column value is null. The bitmap of `[1, 0, 1], [0, 1, 1]` will be merged to `[0, 0, 1]` and applied to the results, which ensures the first two values are none.

## Refer to other examples
As you see, adding a new scalar function in Databend is not as hard as you think.
Before you start to add one, please refer to other scalar function examples, such as `sign`, `expr`, `tan`, `atan`.

## Testing
To be a good engineer, don't forget to test your codes, please add unit tests and stateless tests after you finish the new scalar functions.

## Summary
We welcome all community users to contribute more powerful functions to Databend. If you find any problems, feel free to [open an issue](https://github.com/datafuselabs/databend/issues) in GitHub, we will use our best efforts to help you.
