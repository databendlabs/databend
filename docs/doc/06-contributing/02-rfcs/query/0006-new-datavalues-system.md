# New datavalues system design

## Sumamry

### Short comes of current `DataType`


- `DataType` is an enum type, we must use specific type after matching. For example, if we want to create deserializer/serializer by `DataType`, we should always do matching. It does not mean that match is not necessary. If we want to add more and more functions to `DataType`, matching may be very annoyment.

- `DataType` represented as enum type, we can't use it as generic argument.

- `DataType` may involve some nested datatypes, such as `DataType::Struct`, but we put `DataField` inside `DataType`, it's logically unreasonable。

- Hard to put attributes into enum based `DataType`, such as nullable attribute #3726 #3769

### Too many concepts about column (Series/Column/Array)

-  DataColumn is an enum, including `Constant(value)` and `Array(Series)`
```rust
pub enum DataColumn {
    // Array of values.
    Array(Series),
    // A Single value.
    Constant(DataValue, usize),
}
```

- Series is a wrap of `SeriesTrait`
```rust
pub struct Series(pub Arc<dyn SeriesTrait>);
```

- SeriesTrait can implement various array，using many macros.

```rust
pub struct SeriesWrap<T>(pub T);
   impl SeriesTrait for SeriesWrap<$da> {
            fn data_type(&self) -> &DataType {
                self.0.data_type()
            }

            fn len(&self) -> usize {
                self.0.len()
            }
            ...
  }
```

- For functions, we must consider about `Constant` case for `Column`, so there are many branch matching.
```rust
match (
            columns[0].column().cast_with_type(&DataType::String)?,
            columns[1].column().cast_with_type(&DataType::UInt64)?,
        ) {
            (
                DataColumn::Constant(DataValue::String(input_string), _),
                DataColumn::Constant(DataValue::UInt64(times), _),
            ) => Ok(DataColumn::Constant(
                DataValue::String(repeat(input_string, times)?),
                input_rows,
            )),
            (
                DataColumn::Constant(DataValue::String(input_string), _),
                DataColumn::Array(times),
            )
            ...
```


## New DataValues system design


### Introduce `DataType` as a trait

```rust
#[typetag::serde(tag = "type")]
pub trait DataType: std::fmt::Debug + Sync + Send + DynClone {
    fn data_type_id(&self) -> TypeID;

    fn is_nullable(&self) -> bool {
        false
    }
    ..
 }
```

Nullable is a special case of `DataType`, it's a wrapper of `DataType`.

```rust

pub struct DataTypeNull {inner: DataTypePtr}
```

### Simplify `DataValue`

```rust
pub enum DataValue {
    /// Base type.
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    String(Vec<u8>),
    // Container struct.
    Array(Vec<DataValue>),
    Struct(Vec<DataValue>),
}
```

`DataValue` can convert into proper `DataType` by it's value.

```rust
// convert to minialized data type
    pub fn data_type(&self) -> DataTypePtr {
        match self {
            DataValue::Null => Arc::new(NullType {}),
            DataValue::Boolean(_) => BooleanType::arc(),
            DataValue::Int64(n) => {
                if *n >= i8::MIN as i64 && *n <= i8::MAX as i64 {
                    return Int8Type::arc();
                }
            ...
   }
```

Also, `DataValue` can convert into rust primitive values and vice versa.

### Uniform `Series/Array/Column` into `Column`

- `Column` as a trait

```rust
pub type ColumnRef = Arc<dyn Column>;
pub trait Column: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    /// Type of data that column contains. It's an underlying physical type:
    /// UInt16 for Date, UInt32 for DateTime, so on.
    fn data_type_id(&self) -> TypeID {
        self.data_type().data_type_id()
    }
    fn data_type(&self) -> DataTypePtr;

    fn is_nullable(&self) -> bool {
        false
    }

    fn is_const(&self) -> bool {
        false
    }
   ..
 }

```

- Introduce `Constant column`

> `Constant column` is a wrapper of a `Column` with a single value(size = 1)
```rust
#[derive(Clone)]
pub struct ConstColumn {
    length: usize,
    column: ColumnRef,
}
impl Column for ConstColumn {..}
```

- Introduce `nullable column`

> `nullable column` is a wrapper of a `Column` and keep an extra bitmap to indicate null values.

```rust
pub struct NullableColumn {
    validity: Bitmap,
    column: ColumnRef,
}
impl Column for NullableColumn {..}
```

- No extra cost convert from or into Arrow's column format.

```rust
 fn as_arrow_array(&self) -> common_arrow::arrow::array::ArrayRef {
    let data_type = self.data_type().arrow_type();
    Arc::new(PrimitiveArray::<T>::from_data(
        data_type,
        self.values.clone(),
        None,
    ))
 }
```

- Keep `Series` as a tool struct, this may help to fast generate a column.

```rust
// nullable column from options
let column = Series::from_data(vec![Some(1i8), None, Some(3), Some(4), Some(5)]);

// no nullable column
let column = Series::from_data(vec![1，2，3，4);
```

- Downcast into the specific `Column`
```rust
impl Series {
    /// Get a pointer to the underlying data of this Series.
    /// Can be useful for fast comparisons.
    /// # Safety
    /// Assumes that the `column` is  T.
    pub unsafe fn static_cast<T>(column: &ColumnRef) -> &T {
        let object = column.as_ref();
        &*(object as *const dyn Column as *const T)
    }

    pub fn check_get<T: 'static + Column>(column: &ColumnRef) -> Result<&T> {
        let arr = column.as_any().downcast_ref::<T>().ok_or_else(|| {
            ErrorCode::UnknownColumn(format!(
                "downcast column error, column type: {:?}",
                column.data_type()
            ))
        });
        arr
    }
}
```

- Convinient way to view a column by `ColumnViewer`

No need to care about `Constants` and `Nullable`.

```rust
let wrapper = ColumnViewer::<i8>::try_create(&column)?;

assert_eq!(wrapper.len(), 10);
assert!(!wrapper.null_at(0));
for i in 0..wrapper.len() {
    assert_eq!(*wrapper.value(i), (i + 1) as i8);
}
Ok(())


let wrapper = ColumnViewer::<bool>::try_create(&column)?;
let c = wrapper.value(0);

let wrapper =  ColumnViewer::<&str>::try_create(&column)?;
let c = wrapper.value(1);
Ok(())
```

## TODO

- Make `datavalues2` more mature.
- Merge `datavalues2` into Databend.
