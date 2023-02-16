---
title: How to Write a Scalar Function
---

## What's Scalar Function

A scalar function (also known as User-Defined Functions or UDFs) returns a single value for each row instead of a result set. Scalar functions can be used in most places within a query or SET statement (except the FROM clause).

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

### What You Need to Know before Writing

#### Logical Datatypes and Physical Datatypes

We use logical datatypes in Databend and physical datatypes in the execution/compute engine.

Take `Date` as an example, `Date` is a logical datatype while its physical datatype is `Int32`, so its column is represented by `Buffer<i32>`.

#### Arrow's Memory Layout

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

### Special Column

- Constant column

  Sometimes column is constant in the block, such as: `SELECT 3 from table`, the column 3 is always 3, so we can use a constant column to represent it. This helps save memory space during computation.

- Nullable column

  By default, columns are not nullable. To include null values in a column, you can use a nullable column.

## Function Registration

The `FunctionRegistry` is used to register functions.

```rust
#[derive(Default)]
pub struct FunctionRegistry {
    pub funcs: HashMap<&'static str, Vec<Arc<Function>>>,
    #[allow(clippy::type_complexity)]
    pub factories: HashMap<
        &'static str,
        Vec<Box<dyn Fn(&[usize], &[DataType]) -> Option<Arc<Function>> + 'static>>,
    >,
    pub aliases: HashMap<&'static str, &'static str>,
}
```

It contains three HashMaps: `funcs`, `factories`, and `aliases`.

Both `funcs` and `factories` store registered functions. `funcs` takes a fixed number of arguments (currently from 0 to 5), `register_0_arg`, `register_1_arg`, and so on. `factories` takes variable-length parameters (such as concat) and calls the function`register_function_factory`.

`aliases` uses key-value pairs to store aliases for functions. A function can have more than one alias (for example, `minus` has `subtract` and 'neg'). The key is the alias of a function, and the value is the name of the current function, and the `register_aliases` function will be called.

In addition, there are different levels of register api depending on the function required.

|                                     | Auto Vectorization | Access Output Column Builder | Auto Null Passthrough | Auto Combine Null | Auto Downcast | Throw Runtime Error | Varidic | Tuple |
| ----------------------------------- | -- | -- | -- | -- | -- | -- | -- | -- |
| register_n_arg                      | ✔️ | ❌ | ✔️ | ❌ | ✔️ | ❌ | ❌ | ❌ |
| register_passthrough_nullable_n_arg | ❌ | ✔️ | ✔️ | ❌ | ✔️ | ✔️ | ❌ | ❌ |
| register_combine_nullable_n_arg     | ❌ | ✔️ | ✔️ | ✔️ | ✔️ | ✔️ | ❌ | ❌ |
| register_n_arg_core                 | ❌ | ✔️ | ❌ | ❌ | ✔️ | ✔️ | ❌ | ❌ |
| register_function_factory           | ❌ | ✔️ | ❌ | ❌ | ❌ | ✔️ | ✔️ | ✔️ |

## Function Composition

Since the values of `funcs` are the body of the function, let's see how a `Function` is constructed in Databend.

```rust
pub struct Function {
    pub signature: FunctionSignature,
    #[allow(clippy::type_complexity)]
    pub calc_domain: Box<dyn Fn(&[Domain]) -> Option<Domain>>,
    #[allow(clippy::type_complexity)]
    pub eval: Box<dyn Fn(&[ValueRef<AnyType>], FunctionContext) -> Result<Value<AnyType>, String>>,
}
```

Functions are represented by the `Function` struct, which includes a function `signature`, a calculation domain (`cal_domain`), and an evaluation function (`eval`).

The signature includes the function name, the parameters type, the return type and the function properties (which are not currently available and are reserved for use with functions). Note in particular that the function name needs to be lowercase when registering. Some tokens are transformed via `src/query/ast/src/parser/token.rs`.

```rust
#[allow(non_camel_case_types)]
#[derive(Logos, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TokenKind {
    ...
    #[token("+")]
    Plus,
    ...
}
```

As an example, let's consider the addition function used in the query `select 1+2`. The `+` token is converted to `Plus`, and the function name needs to be lowercase. Therefore, the function name used for registration is `plus`.

```rust
with_number_mapped_type!(|NUM_TYPE| match left {
    NumberDataType::NUM_TYPE => {
        registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<NUM_TYPE>, _, _>(
            "plus",
            FunctionProperty::default(),
            |lhs| Some(lhs.clone()),
            |a, _| a,
        );
    }
});
```

`cal_domain` is used to calculate the input value set for the output value. This is described by a mathematical formula such as `y = f(x)` where the domain is the set of values `x` that can be used as arguments to `f` to generate values `y`. This allows us to easily filter out values that are not in the domain when indexing data, greatly improving response efficiency.

`eval` can be understood as the concrete implementation of a function, which takes characters or numbers as input, parses them into expressions, and converts them into another set of values.

## Example

There are several categories of functions, including arithmetic, array, boolean, control, comparison, datetime, math, and string.

### `length` function

The length function takes a `String` parameter and returns a `Number`. It is named as `length`, with **no domain restrictions** since each string should have a length. The last argument is a closure function that serves as the implementation of `length`.

```rust
registry.register_1_arg::<StringType, NumberType<u64>, _, _>(
    "length",
    FunctionProperty::default(),
    |_| None,
    |val, _| val.len() as u64,
);
```

In the implementation of `register_1_arg`, we see that the called function is `register_passthrough_nullable_1_arg`, whose name contains **nullable**. `eval` is called by `vectorize_1_arg`.

> It's worth noting that the [register.rs in src/query/expression/src](https://github.com/datafuselabs/databend/blob/2aec38605eebb7f0e1717f7f54ec52ae0f2e530b/src/query/codegen/src/writes/register.rs) should not be manually modified as it is generated by [src/query/codegen/src/writes/register.rs](https://github.com/datafuselabs/databend/blob/2aec38605eebb7f0e1717f7f54ec52ae0f2e530b/src/query/codegen/src/writes/register.rs).

```rust
pub fn register_1_arg<I1: ArgType, O: ArgType, F, G>(
    &mut self,
    name: &'static str,
    property: FunctionProperty,
    calc_domain: F,
    func: G,
) where
    F: Fn(&I1::Domain) -> Option<O::Domain> + 'static + Clone + Copy,
    G: Fn(I1::ScalarRef<'_>, FunctionContext) -> O::Scalar + 'static + Clone + Copy,
{
    self.register_passthrough_nullable_1_arg::<I1, O, _, _>(
        name,
        property,
        calc_domain,
        vectorize_1_arg(func),
    )
}
```

In practical scenarios, `eval` accepts not only strings or numbers, but also null or other various types. `null` is undoubtedly the most special one. The parameter we receive may also be a column or a value. For example, in the following SQL queries, length is called with a null value or a column:

```sql
select length(null);
+--------------+
| length(null) |
+--------------+
|         NULL |
+--------------+
select length(id) from t;
+------------+
| length(id) |
+------------+
|          2 |
|          3 |
+------------+
```

Therefore, if we don't need to handle `null` values in the function, we can simply use `register_x_arg`. Otherwise, we can refer to the implementation of [try_to_timestamp](https://github.com/datafuselabs/databend/blob/d5e06af03ba0f99afdd6bdc974bf2f5c1c022db8/src/query/functions/src/scalars/datetime.rs).

For functions that require specialization in vectorize, `register_passthrough_nullable_x_arg` should be used to perform specific vectorization optimization.

For example, the implementation of the `regexp` function takes two `String` parameters and returns a `Bool`. In order to further optimize and reduce the repeated parsing of regular expressions, a `HashMap` structure is introduced to vectorized execution. Therefore, `vectorize_regexp` is separately implemented to handle this optimization.

```rust
registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
    "regexp",
    FunctionProperty::default(),
    |_, _| None,
    vectorize_regexp(|str, pat, map, _| {
        let pattern = if let Some(pattern) = map.get(pat) {
            pattern
        } else {
            let re = regexp::build_regexp_from_pattern("regexp", pat, None)?;
            map.insert(pat.to_vec(), re);
            map.get(pat).unwrap()
        };
        Ok(pattern.is_match(str))
    }),
);
```

## Testing

As a good developer, you always test your code, don't you? Please add unit tests and logic tests after you complete the new scalar functions.

### Unit Test

The unit tests for scalar functions are located in [scalars](https://github.com/datafuselabs/databend/tree/d5e06af03ba0f99afdd6bdc974bf2f5c1c022db8/src/query/functions/tests/it/scalars).

### Logic Test

The logic tests for functions are located in [02_function](https://github.com/datafuselabs/databend/tree/d5e06af03ba0f99afdd6bdc974bf2f5c1c022db8/tests/sqllogictests/suites/query/02_function).
