---
title: How to Write Scalar Functions
---

## What's scalar functions

Scalar functions (sometimes referred to as User-Defined Functions / UDFs) return a single value as a return value for each row, not as a result set, and can be used in most places within a query or SET statement, except for the FROM clause.

```text title="One to One Mapping execution"

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


### Knowledge before writing the eval function

#### Logical datatypes and physical datatypes.

Logical datatypes are the datatypes that we use in Databend, and physical datatypes are the datatypes that we use in the execution/compute engine.
Such as `Date32`, it's a logical data type, but its physical is `Int32`, so its column is represented by `Int32Column`.

We can get logical datatype by `data_type` function of `DataField` , and the physical datatype by `data_type` function in `ColumnRef`.
`ColumnsWithField` has `data_type` function which returns the logical datatype.

#### Arrow's memory layout

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

In most cases, we can ignore null for simd operation, and add the null mask to the result after the operation.
This is very common optimization and widely used in arrow's compute system.

### Special column

-  Constant column

    Sometimes column is constant in the block, such as: `SELECT 3 from table`, the column 3 is always 3, so we can use a constant column to represent it. This is useful to save the memory space during computation.

    So databend's DataColumn is represented by:

    ```rust
    #[derive(Clone)]
    pub struct ConstColumn {
        length: usize,
        column: ColumnRef,
    }
    ```
- Nullable column

    By default, columns are not nullable. If we want a nullable column, we can use this to represent it.

    ```rust
    #[derive(Clone)]
    pub struct NullableColumn {
        validity: Bitmap,
        column: ColumnRef,
    }
    ```

### Writing function guidelines

## ScalarFunction trait introduction

All scalar functions implement `Function` trait, and we register them into a global static `FunctionFactory`, the factory is just an index map and the key is the name of the scalar function.

:::tip
    Function name in Databend is case-insensitive.
:::

``` rust

pub trait Function: fmt::Display + Sync + Send + DynClone {
    ...
}
```

 *Let's take function `sqrt` as an example*

- Declar the function named `SqrtFunction`
``` rust
#[derive(Clone)]
pub struct SqrtFunction {
    display_name: String,
}
```

- Implement `SqrtFunction` to have a constructor and description.

```rust
impl SqrtFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypePtr]) -> Result<Box<dyn Function>> {
        assert_numeric(args[0])?;
        Ok(Box::new(SqrtFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}
```

The `try_create` is useful to create this function, at last we can register it into the factory.

The `desc` is used to describe the function. Inside it, we can set the features, such as the number of arguments, the deterministic or not, etc.

- Implement the simple function for `sqrt`

```rust
fn sqrt<S>(value: S, _ctx: &mut EvalContext) -> f64
where S: AsPrimitive<f64> {
    value.as_().sqrt()
}
```

It's really simple, `S: AsPrimitive<f64>` means we can accept a primitive value as the argument.

- Implement Function trait

```rust
impl Function for SqrtFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self) -> DataTypePtr {
        Float64Type::arc()
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let mut ctx = EvalContext::default();
        with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
             let col = scalar_unary_op::<$S, f64, _>(columns[0].column(), sqrt::<$S>, &mut ctx)?;
             Ok(col.arc())
        },{
            unreachable!()
        })
    }
}
```

By defaults, we enable `passthrough_constant`, that means: `sqrt(constant_column)`  will be converted into `Consntat(sqrt(column), rows)` in `FunctionAdaptor`.

And we have enabled `passthrough_nullable`, that means: `sqrt(nullable_column)`  will be converted into `Nullable(sqrt(no_nullable_column), null_bitmaps)` in `FunctionAdaptor`.

So inside the `eval` function, we really don't need to care about constant or nullable cases. It's pretty simple and efficient.


The macro `with_match_primitive_type_id` will match the primitive type id, and cast the column into corresponding type, so we allowed `sqrt(i8)`, `sqrt(i16)` ... types.

The `scalar_unary_op` is a helper function to implement the scalar function for unary operator.
This is very commonly used and there is `scalar_binary_op` too. See more in [binary](https://github.com/datafuselabs/databend/blob/e7edeea2e3ae5fb1f8408903df10b1b641b57652/common/functions/src/scalars/expressions/binary.rs), [unary](https://github.com/datafuselabs/databend/blob/e7edeea2e3ae5fb1f8408903df10b1b641b57652/common/functions/src/scalars/expressions/unary.rs)


## Register the function into the factory

```rust
factory.register("sqrt", SqrtFunction::desc());
```


## Testing
To be a good engineer, don't forget to test your codes, please add unit tests and stateless tests after you finish the new scalar functions.

- [Unit tests](https://github.com/datafuselabs/databend/blob/034e1cd95c1376341b9421c08f8eb38b40fc5dda/common/functions/tests/it/scalars/maths/sqrt.rs)

- Stateless tests:

```sql

SELECT sqrt(-3), sqrt(3), sqrt(0), sqrt(3.0), sqrt( toUInt64(3) ), sqrt(null) ;
+----------+--------------------+---------+--------------------+--------------------+------------+
| sqrt(-3) | sqrt(3)            | sqrt(0) | sqrt(3)            | sqrt(toUInt64(3))  | sqrt(NULL) |
+----------+--------------------+---------+--------------------+--------------------+------------+
|      NaN | 1.7320508075688772 |       0 | 1.7320508075688772 | 1.7320508075688772 |       NULL |
+----------+--------------------+---------+--------------------+--------------------+------------+

SELECT sqrt('-3');
ERROR 1105 (HY000): Code: 1007, displayText = Expected a numeric type, but got String (while in SELECT before projection).
```

All is done!


## Refer to other examples
As you see, adding a new scalar function in Databend is not as hard as you think.
Before you start to add one, please refer to other scalar function examples, such as `sign`, `expr`, `tan`, `atan`

## Summary
We welcome all community users to contribute more powerful functions to Databend. If you find any problems, feel free to [open an issue](https://github.com/datafuselabs/databend/issues) in GitHub, we will use our best efforts to help you.
