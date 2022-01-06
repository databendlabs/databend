# New datatype system design

## Sumamry

We need to redesign the `datatype` system because current implementation had some shortcomes, including but not limited to:

- `DataType` is an enum type, we must use specific type after matching. For example, if we want to create deserializer/serializer by `DataType`, we should always do matching. It does not mean that match is not necessary. If we want to add more and more functions to `DataType`, matching may be very annoyment.

- `DataType` represented as enum type, we can't use it as generic argument.

- `DataType` may involve some nested datatypes, such as `DataType::Struct`, but we put `DataField` inside `DataType`, it's logically unreasonable。

- Hard to put attributes into enum based `DataType`, such as nullable attribute #3726 #3769


## New Datatype system design

### Enum `TypeID`

`TypeID` just represents the kind of `DataType`. It does not have extra attributes.

```rust
enum TypeID {
    Nothing,
    Int8,
    Int16,
    Int32,
    ...,
    Date,
    DateTime,
    List,
    Struct,
    Nullable,
}
```

### trait `DataType`

```rust
pub trait DataType {
    fn type_id() -> TypeID;
    fn is_nullable() -> bool;
    fn arrow_type() -> ArrowType;
    fn create_serializer() -> Box<dyn TypeSerializer>;
    fn create_deserializer() -> Box<dyn TypeDeserializer>;
    fn create_builder() -> Box<dyn ArrayBuilder>;
    ...
}
```

Each `DataType` we can get the `TypeID` to use in simple type match.

Then we can have `DataTypeInt8` and `DataTypeInt16` .. to implement trait `DataType`.

```rust
pub DataTypeInt8 = DataTypeNumber<i8>;

impl DataType for DataTypeInt8 {
    fn type_id() -> TypeID {
        TypeID::Int8
    }
    ...
}

```

`DataTypeNullable`:

```rust
struct DatTypeNullable {
    inner: Box<dyn DataType>
}
```

and `DataTypeStruct`:

```rust
struct DatTypeStruct {
    names: Vec<String>,
    inners: Vec<Box<dyn DataType>>
}
```


### struct `DataField`

Yes, we still need struct `DataField`, because we need it to store other attributes other than `DataType`.

```rust
struct DataField {
    name: String,
    nullable: bool,
    data_type: Box<dyn DataType>,
    ...
}
```


### Example of a function [`bin`](https://github.com/datafuselabs/databend/blob/7cadfd2b9d11406a06b31207d7e3634f36aa7f00/common/functions/src/scalars/strings/bin.rs)



```rust
 fn return_type(&self, args: &[Box<DataType>]) -> Result<Box<DataType>> {
        if !args[0].is_numeric(){
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected number or null, but got {}",
                args[0]
            )));
        }
        Ok(Box::new(DataTypeString::create()))
    }
```


### Example of `numerical_coercion`

```rust
pub fn numerical_coercion(
    lhs_type: &Box<DataType>,
    rhs_type: &Box<DataType>,
    allow_overflow: bool,
) -> Result<Box<DataType>> {

    let has_float = lhs_type.is_floating() || rhs_type.is_floating();
    let has_integer = lhs_type.is_integer() || rhs_type.is_integer();
    let has_signed = lhs_type.is_signed_numeric() || rhs_type.is_signed_numeric();

    ....
}
```

