// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::iter::once;
use std::ops::Range;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::trusted_len::TrustedLen;
use enum_as_inner::EnumAsInner;

use crate::types::*;
use crate::util::append_bitmap;
use crate::util::bitmap_into_mut;
use crate::util::buffer_into_mut;
use crate::util::constant_bitmap;

#[derive(EnumAsInner)]
pub enum Value<T: ValueType> {
    Scalar(T::Scalar),
    Column(T::Column),
}

#[derive(EnumAsInner)]
pub enum ValueRef<'a, T: ValueType> {
    Scalar(T::ScalarRef<'a>),
    Column(T::Column),
}

#[derive(Debug, Clone, Default, EnumAsInner)]
pub enum Scalar {
    #[default]
    Null,
    EmptyArray,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    String(Vec<u8>),
    Array(Column),
    Tuple(Vec<Scalar>),
}

#[derive(Debug, Clone, Default, EnumAsInner)]
pub enum ScalarRef<'a> {
    #[default]
    Null,
    EmptyArray,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    String(&'a [u8]),
    Array(Column),
    Tuple(Vec<ScalarRef<'a>>),
}

#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum Column {
    Null {
        len: usize,
    },
    EmptyArray {
        len: usize,
    },
    Int8(Buffer<i8>),
    Int16(Buffer<i16>),
    Int32(Buffer<i32>),
    Int64(Buffer<i64>),
    UInt8(Buffer<u8>),
    UInt16(Buffer<u16>),
    UInt32(Buffer<u32>),
    UInt64(Buffer<u64>),
    Float32(Buffer<f32>),
    Float64(Buffer<f64>),
    Boolean(Bitmap),
    String {
        data: Buffer<u8>,
        offsets: Buffer<u64>,
    },
    Array {
        array: Box<Column>,
        offsets: Buffer<u64>,
    },
    Nullable {
        column: Box<Column>,
        validity: Bitmap,
    },
    Tuple {
        fields: Vec<Column>,
        len: usize,
    },
}

#[derive(Debug, Clone, EnumAsInner)]
pub enum ColumnBuilder {
    Null {
        len: usize,
    },
    EmptyArray {
        len: usize,
    },
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Boolean(MutableBitmap),
    String {
        data: Vec<u8>,
        offsets: Vec<u64>,
    },
    Array {
        array: Box<ColumnBuilder>,
        offsets: Vec<u64>,
    },
    Nullable {
        column: Box<ColumnBuilder>,
        validity: MutableBitmap,
    },
    Tuple {
        fields: Vec<ColumnBuilder>,
        len: usize,
    },
}

impl<'a, T: ValueType> ValueRef<'a, T> {
    pub fn to_owned(self) -> Value<T> {
        match self {
            ValueRef::Scalar(scalar) => Value::Scalar(T::to_owned_scalar(scalar)),
            ValueRef::Column(col) => Value::Column(col),
        }
    }
}

impl<'a, T: ValueType> Value<T> {
    pub fn as_ref(&'a self) -> ValueRef<'a, T> {
        match self {
            Value::Scalar(scalar) => ValueRef::Scalar(T::to_scalar_ref(scalar)),
            Value::Column(col) => ValueRef::Column(col.clone()),
        }
    }
}

impl<T: ArgType> Value<T> {
    pub fn upcast(self) -> Value<AnyType> {
        match self {
            Value::Scalar(scalar) => Value::Scalar(T::upcast_scalar(scalar)),
            Value::Column(col) => Value::Column(T::upcast_column(col)),
        }
    }
}

impl<'a> ValueRef<'a, AnyType> {
    pub fn try_downcast<'b, T: ArgType>(&'b self) -> Option<ValueRef<'a, T>> {
        Some(match self {
            ValueRef::Scalar(scalar) => ValueRef::Scalar(T::try_downcast_scalar(scalar)?),
            ValueRef::Column(col) => ValueRef::Column(T::try_downcast_column(col)?),
        })
    }
}

impl<'a, T: ValueType> Clone for ValueRef<'a, T> {
    fn clone(&self) -> Self {
        match self {
            ValueRef::Scalar(scalar) => ValueRef::Scalar(scalar.clone()),
            ValueRef::Column(col) => ValueRef::Column(col.clone()),
        }
    }
}

impl Scalar {
    pub fn as_ref(&self) -> ScalarRef {
        match self {
            Scalar::Null => ScalarRef::Null,
            Scalar::EmptyArray => ScalarRef::EmptyArray,
            Scalar::Int8(i) => ScalarRef::Int8(*i),
            Scalar::Int16(i) => ScalarRef::Int16(*i),
            Scalar::Int32(i) => ScalarRef::Int32(*i),
            Scalar::Int64(i) => ScalarRef::Int64(*i),
            Scalar::UInt8(i) => ScalarRef::UInt8(*i),
            Scalar::UInt16(i) => ScalarRef::UInt16(*i),
            Scalar::UInt32(i) => ScalarRef::UInt32(*i),
            Scalar::UInt64(i) => ScalarRef::UInt64(*i),
            Scalar::Float32(i) => ScalarRef::Float32(*i),
            Scalar::Float64(i) => ScalarRef::Float64(*i),
            Scalar::Boolean(b) => ScalarRef::Boolean(*b),
            Scalar::String(s) => ScalarRef::String(s.as_slice()),
            Scalar::Array(col) => ScalarRef::Array(col.clone()),
            Scalar::Tuple(fields) => ScalarRef::Tuple(fields.iter().map(Scalar::as_ref).collect()),
        }
    }
}

impl<'a> ScalarRef<'a> {
    pub fn to_owned(&self) -> Scalar {
        match self {
            ScalarRef::Null => Scalar::Null,
            ScalarRef::EmptyArray => Scalar::EmptyArray,
            ScalarRef::Int8(i) => Scalar::Int8(*i),
            ScalarRef::Int16(i) => Scalar::Int16(*i),
            ScalarRef::Int32(i) => Scalar::Int32(*i),
            ScalarRef::Int64(i) => Scalar::Int64(*i),
            ScalarRef::UInt8(i) => Scalar::UInt8(*i),
            ScalarRef::UInt16(i) => Scalar::UInt16(*i),
            ScalarRef::UInt32(i) => Scalar::UInt32(*i),
            ScalarRef::UInt64(i) => Scalar::UInt64(*i),
            ScalarRef::Float32(i) => Scalar::Float32(*i),
            ScalarRef::Float64(i) => Scalar::Float64(*i),
            ScalarRef::Boolean(b) => Scalar::Boolean(*b),
            ScalarRef::String(s) => Scalar::String(s.to_vec()),
            ScalarRef::Array(col) => Scalar::Array(col.clone()),
            ScalarRef::Tuple(fields) => {
                Scalar::Tuple(fields.iter().map(ScalarRef::to_owned).collect())
            }
        }
    }

    pub fn repeat(&self, n: usize) -> ColumnBuilder {
        match self {
            ScalarRef::Null => ColumnBuilder::Null { len: n },
            ScalarRef::EmptyArray => ColumnBuilder::EmptyArray { len: n },
            ScalarRef::Int8(i) => ColumnBuilder::Int8(vec![*i; n]),
            ScalarRef::Int16(i) => ColumnBuilder::Int16(vec![*i; n]),
            ScalarRef::Int32(i) => ColumnBuilder::Int32(vec![*i; n]),
            ScalarRef::Int64(i) => ColumnBuilder::Int64(vec![*i; n]),
            ScalarRef::UInt8(i) => ColumnBuilder::UInt8(vec![*i; n]),
            ScalarRef::UInt16(i) => ColumnBuilder::UInt16(vec![*i; n]),
            ScalarRef::UInt32(i) => ColumnBuilder::UInt32(vec![*i; n]),
            ScalarRef::UInt64(i) => ColumnBuilder::UInt64(vec![*i; n]),
            ScalarRef::Float32(i) => ColumnBuilder::Float32(vec![*i; n]),
            ScalarRef::Float64(i) => ColumnBuilder::Float64(vec![*i; n]),
            ScalarRef::Boolean(b) => ColumnBuilder::Boolean(constant_bitmap(*b, n)),
            ScalarRef::String(s) => {
                let len = s.len();
                let mut data = Vec::with_capacity(len * n);
                for _ in 0..n {
                    data.extend_from_slice(s);
                }
                let offsets = once(0)
                    .chain((0..n).map(|i| (len * (i + 1)) as u64))
                    .collect();
                ColumnBuilder::String { data, offsets }
            }
            ScalarRef::Array(col) => {
                let col = ColumnBuilder::from_column(col.clone());
                let len = col.len();
                let mut builder = col.clone();
                for _ in 1..n {
                    builder.append(&col);
                }
                let offsets = once(0)
                    .chain((0..n).map(|i| (len * (i + 1)) as u64))
                    .collect();
                ColumnBuilder::Array {
                    array: Box::new(builder),
                    offsets,
                }
            }
            ScalarRef::Tuple(fields) => ColumnBuilder::Tuple {
                fields: fields.iter().map(|field| field.repeat(n)).collect(),
                len: n,
            },
        }
    }
}

impl Column {
    pub fn len(&self) -> usize {
        match self {
            Column::Null { len } => *len,
            Column::EmptyArray { len } => *len,
            Column::Int8(col) => col.len(),
            Column::Int16(col) => col.len(),
            Column::Int32(col) => col.len(),
            Column::Int64(col) => col.len(),
            Column::UInt8(col) => col.len(),
            Column::UInt16(col) => col.len(),
            Column::UInt32(col) => col.len(),
            Column::UInt64(col) => col.len(),
            Column::Float32(col) => col.len(),
            Column::Float64(col) => col.len(),
            Column::Boolean(col) => col.len(),
            Column::String { data: _, offsets } => offsets.len() - 1,
            Column::Array { array: _, offsets } => offsets.len() - 1,
            Column::Nullable {
                column: _,
                validity,
            } => validity.len(),
            Column::Tuple { len, .. } => *len,
        }
    }

    pub fn index(&self, index: usize) -> Option<ScalarRef> {
        match self {
            Column::Null { .. } => Some(ScalarRef::Null),
            Column::EmptyArray { .. } => Some(ScalarRef::EmptyArray),
            Column::Int8(col) => Some(ScalarRef::Int8(col.get(index).cloned()?)),
            Column::Int16(col) => Some(ScalarRef::Int16(col.get(index).cloned()?)),
            Column::Int32(col) => Some(ScalarRef::Int32(col.get(index).cloned()?)),
            Column::Int64(col) => Some(ScalarRef::Int64(col.get(index).cloned()?)),
            Column::UInt8(col) => Some(ScalarRef::UInt8(col.get(index).cloned()?)),
            Column::UInt16(col) => Some(ScalarRef::UInt16(col.get(index).cloned()?)),
            Column::UInt32(col) => Some(ScalarRef::UInt32(col.get(index).cloned()?)),
            Column::UInt64(col) => Some(ScalarRef::UInt64(col.get(index).cloned()?)),
            Column::Float32(col) => Some(ScalarRef::Float32(col.get(index).cloned()?)),
            Column::Float64(col) => Some(ScalarRef::Float64(col.get(index).cloned()?)),
            Column::Boolean(col) => Some(ScalarRef::Boolean(col.get(index)?)),
            Column::String { data, offsets } => {
                if offsets.len() > index + 1 {
                    Some(ScalarRef::String(
                        &data[(offsets[index] as usize)..(offsets[index + 1] as usize)],
                    ))
                } else {
                    None
                }
            }
            Column::Array { array, offsets } => {
                if offsets.len() > index + 1 {
                    Some(ScalarRef::Array(array.slice(
                        (offsets[index] as usize)..(offsets[index + 1] as usize),
                    )))
                } else {
                    None
                }
            }
            Column::Nullable { column, validity } => Some(if validity.get(index)? {
                column.index(index).unwrap()
            } else {
                ScalarRef::Null
            }),
            Column::Tuple { fields, .. } => Some(ScalarRef::Tuple(
                fields
                    .iter()
                    .map(|field| field.index(index))
                    .collect::<Option<Vec<_>>>()?,
            )),
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        assert!(
            range.end <= self.len(),
            "range {:?} out of len {}",
            range,
            self.len()
        );
        match self {
            Column::Null { .. } => Column::Null {
                len: range.end - range.start,
            },
            Column::EmptyArray { .. } => Column::EmptyArray {
                len: range.end - range.start,
            },
            Column::Int8(col) => {
                Column::Int8(col.clone().slice(range.start, range.end - range.start))
            }
            Column::Int16(col) => {
                Column::Int16(col.clone().slice(range.start, range.end - range.start))
            }
            Column::Int32(col) => {
                Column::Int32(col.clone().slice(range.start, range.end - range.start))
            }
            Column::Int64(col) => {
                Column::Int64(col.clone().slice(range.start, range.end - range.start))
            }
            Column::UInt8(col) => {
                Column::UInt8(col.clone().slice(range.start, range.end - range.start))
            }
            Column::UInt16(col) => {
                Column::UInt16(col.clone().slice(range.start, range.end - range.start))
            }
            Column::UInt32(col) => {
                Column::UInt32(col.clone().slice(range.start, range.end - range.start))
            }
            Column::UInt64(col) => {
                Column::UInt64(col.clone().slice(range.start, range.end - range.start))
            }
            Column::Float32(col) => {
                Column::Float32(col.clone().slice(range.start, range.end - range.start))
            }
            Column::Float64(col) => {
                Column::Float64(col.clone().slice(range.start, range.end - range.start))
            }
            Column::Boolean(col) => {
                Column::Boolean(col.clone().slice(range.start, range.end - range.start))
            }
            Column::String { data, offsets } => {
                let offsets = offsets
                    .clone()
                    .slice(range.start, range.end - range.start + 1);
                Column::String {
                    data: data.clone(),
                    offsets,
                }
            }
            Column::Array { array, offsets } => {
                let offsets = offsets
                    .clone()
                    .slice(range.start, range.end - range.start + 1);
                Column::Array {
                    array: array.clone(),
                    offsets,
                }
            }
            Column::Nullable { column, validity } => {
                let validity = validity.clone().slice(range.start, range.end - range.start);
                Column::Nullable {
                    column: Box::new(column.slice(range)),
                    validity,
                }
            }
            Column::Tuple { fields, .. } => Column::Tuple {
                fields: fields
                    .iter()
                    .map(|field| field.slice(range.clone()))
                    .collect(),
                len: range.end - range.start,
            },
        }
    }

    pub fn iter(&self) -> ColumnIterator {
        ColumnIterator {
            column: self,
            index: 0,
            len: self.len(),
        }
    }

    pub fn arrow_type(&self) -> common_arrow::arrow::datatypes::DataType {
        use common_arrow::arrow::datatypes::DataType as ArrowDataType;
        use common_arrow::arrow::datatypes::Field;

        match self {
            Column::Null { .. } => ArrowDataType::Null,
            Column::EmptyArray { .. } => ArrowDataType::Extension(
                "EmptyArray".to_owned(),
                Box::new(ArrowDataType::Null),
                None,
            ),
            Column::Int8(_) => ArrowDataType::Int8,
            Column::Int16(_) => ArrowDataType::Int16,
            Column::Int32(_) => ArrowDataType::Int32,
            Column::Int64(_) => ArrowDataType::Int64,
            Column::UInt8(_) => ArrowDataType::UInt8,
            Column::UInt16(_) => ArrowDataType::UInt16,
            Column::UInt32(_) => ArrowDataType::UInt32,
            Column::UInt64(_) => ArrowDataType::UInt64,
            Column::Float32(_) => ArrowDataType::Float32,
            Column::Float64(_) => ArrowDataType::Float64,
            Column::Boolean(_) => ArrowDataType::Boolean,
            Column::String { .. } => ArrowDataType::LargeBinary,
            Column::Array {
                array: box Column::Nullable { column, .. },
                ..
            } => ArrowDataType::LargeList(Box::new(Field::new(
                "list".to_string(),
                column.arrow_type(),
                true,
            ))),
            Column::Array { array, .. } => ArrowDataType::LargeList(Box::new(Field::new(
                "list".to_string(),
                array.arrow_type(),
                false,
            ))),
            Column::Nullable { column, .. } => column.arrow_type(),
            Column::Tuple { fields, .. } => {
                let arrow_fields = fields
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| match field {
                        Column::Nullable { column, .. } => {
                            Field::new((idx + 1).to_string(), column.arrow_type(), true)
                        }
                        _ => Field::new((idx + 1).to_string(), field.arrow_type(), false),
                    })
                    .collect();
                ArrowDataType::Struct(arrow_fields)
            }
        }
    }

    pub fn as_arrow(&self) -> Box<dyn common_arrow::arrow::array::Array> {
        match self {
            Column::Null { len } => Box::new(common_arrow::arrow::array::NullArray::new_null(
                self.arrow_type(),
                *len,
            )),
            Column::EmptyArray { len } => Box::new(
                common_arrow::arrow::array::NullArray::new_null(self.arrow_type(), *len),
            ),
            Column::Int8(col) => {
                Box::new(common_arrow::arrow::array::PrimitiveArray::<i8>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ))
            }
            Column::Int16(col) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<i16>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ),
            ),
            Column::Int32(col) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<i32>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ),
            ),
            Column::Int64(col) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<i64>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ),
            ),
            Column::UInt8(col) => {
                Box::new(common_arrow::arrow::array::PrimitiveArray::<u8>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ))
            }
            Column::UInt16(col) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<u16>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ),
            ),
            Column::UInt32(col) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<u32>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ),
            ),
            Column::UInt64(col) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<u64>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ),
            ),
            Column::Float32(col) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<f32>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ),
            ),
            Column::Float64(col) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<f64>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ),
            ),
            Column::Boolean(col) => Box::new(common_arrow::arrow::array::BooleanArray::from_data(
                self.arrow_type(),
                col.clone(),
                None,
            )),
            Column::String { data, offsets } => {
                Box::new(common_arrow::arrow::array::BinaryArray::<i64>::from_data(
                    self.arrow_type(),
                    offsets.iter().map(|offset| *offset as i64).collect(),
                    data.clone(),
                    None,
                ))
            }
            Column::Array { array, offsets } => {
                Box::new(common_arrow::arrow::array::ListArray::<i64>::from_data(
                    self.arrow_type(),
                    offsets.iter().map(|offset| *offset as i64).collect(),
                    array.as_arrow(),
                    None,
                ))
            }
            Column::Nullable { column, validity } => {
                column.as_arrow().with_validity(Some(validity.clone()))
            }
            Column::Tuple { fields, .. } => {
                Box::new(common_arrow::arrow::array::StructArray::from_data(
                    self.arrow_type(),
                    fields.iter().map(|field| field.as_arrow()).collect(),
                    None,
                ))
            }
        }
    }

    pub fn from_arrow(arrow_col: &dyn common_arrow::arrow::array::Array) -> Column {
        use common_arrow::arrow::array::Array as _;
        use common_arrow::arrow::datatypes::DataType as ArrowDataType;

        let col = match arrow_col.data_type() {
            ArrowDataType::Null => Column::Null {
                len: arrow_col.len(),
            },
            ArrowDataType::Extension(name, _, _) if name == "EmptyArray" => Column::EmptyArray {
                len: arrow_col.len(),
            },
            ArrowDataType::Int8 => Column::Int8(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Int8Array>()
                    .expect("fail to read from arrow: array should be `Int8Array`")
                    .values()
                    .clone(),
            ),
            ArrowDataType::Int16 => Column::Int16(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Int16Array>()
                    .expect("fail to read from arrow: array should be `Int16Array`")
                    .values()
                    .clone(),
            ),
            ArrowDataType::Int32 => Column::Int32(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Int32Array>()
                    .expect("fail to read from arrow: array should be `Int32Array`")
                    .values()
                    .clone(),
            ),
            ArrowDataType::Int64 => Column::Int64(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Int64Array>()
                    .expect("fail to read from arrow: array should be `Int64Array`")
                    .values()
                    .clone(),
            ),
            ArrowDataType::UInt8 => Column::UInt8(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::UInt8Array>()
                    .expect("fail to read from arrow: array should be `UInt8Array`")
                    .values()
                    .clone(),
            ),
            ArrowDataType::UInt16 => Column::UInt16(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::UInt16Array>()
                    .expect("fail to read from arrow: array should be `UInt16Array`")
                    .values()
                    .clone(),
            ),
            ArrowDataType::UInt32 => Column::UInt32(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::UInt32Array>()
                    .expect("fail to read from arrow: array should be `UInt32Array`")
                    .values()
                    .clone(),
            ),
            ArrowDataType::UInt64 => Column::UInt64(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::UInt64Array>()
                    .expect("fail to read from arrow: array should be `UInt64Array`")
                    .values()
                    .clone(),
            ),
            ArrowDataType::Float32 => Column::Float32(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Float32Array>()
                    .expect("fail to read from arrow: array should be `Float32Array`")
                    .values()
                    .clone(),
            ),
            ArrowDataType::Float64 => Column::Float64(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Float64Array>()
                    .expect("fail to read from arrow: array should be `Float64Array`")
                    .values()
                    .clone(),
            ),
            ArrowDataType::Boolean => Column::Boolean(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::BooleanArray>()
                    .expect("fail to read from arrow: array should be `BooleanArray`")
                    .values()
                    .clone(),
            ),
            ArrowDataType::LargeBinary => {
                let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::BinaryArray<i64>>()
                    .expect("fail to read from arrow: array should be `BinaryArray<i64>`");
                let offsets = arrow_col
                    .offsets()
                    .iter()
                    .map(|x| *x as u64)
                    .collect::<Vec<_>>();
                Column::String {
                    data: arrow_col.values().clone(),
                    offsets: offsets.into(),
                }
            }
            // TODO: deprecate it and use LargeBinary instead
            ArrowDataType::Binary => {
                let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::BinaryArray<i32>>()
                    .expect("fail to read from arrow: array should be `BinaryArray<i32>`");
                let offsets = arrow_col
                    .offsets()
                    .iter()
                    .map(|x| *x as u64)
                    .collect::<Vec<_>>();
                Column::String {
                    data: arrow_col.values().clone(),
                    offsets: offsets.into(),
                }
            }
            // TODO: deprecate it and use LargeBinary instead
            ArrowDataType::Utf8 => {
                let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Utf8Array<i32>>()
                    .expect("fail to read from arrow: array should be `Utf8Array<i32>`");
                let offsets = arrow_col
                    .offsets()
                    .iter()
                    .map(|x| *x as u64)
                    .collect::<Vec<_>>();
                Column::String {
                    data: arrow_col.values().clone(),
                    offsets: offsets.into(),
                }
            }
            // TODO: deprecate it and use LargeBinary instead
            ArrowDataType::LargeUtf8 => {
                let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Utf8Array<i64>>()
                    .expect("fail to read from arrow: array should be `Utf8Array<i64>`");
                let offsets = arrow_col
                    .offsets()
                    .iter()
                    .map(|x| *x as u64)
                    .collect::<Vec<_>>();
                Column::String {
                    data: arrow_col.values().clone(),
                    offsets: offsets.into(),
                }
            }
            ArrowDataType::LargeList(_) => {
                let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::ListArray<i64>>()
                    .expect("fail to read from arrow: array should be `ListArray<i64>`");
                let array = Column::from_arrow(&**arrow_col.values());
                let offsets = arrow_col
                    .offsets()
                    .iter()
                    .map(|x| *x as u64)
                    .collect::<Vec<_>>();
                Column::Array {
                    array: Box::new(array),
                    offsets: offsets.into(),
                }
            }
            ArrowDataType::Struct(_) => {
                let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::StructArray>()
                    .expect("fail to read from arrow: array should be `StructArray`");
                let fields = arrow_col
                    .values()
                    .iter()
                    .map(|field| Column::from_arrow(&**field))
                    .collect::<Vec<_>>();
                Column::Tuple {
                    fields,
                    len: arrow_col.len(),
                }
            }
            ty => unimplemented!("unsupported arrow type {ty:?}"),
        };

        if let Some(validity) = arrow_col.validity() {
            Column::Nullable {
                column: Box::new(col),
                validity: validity.clone(),
            }
        } else {
            col
        }
    }
}

impl ColumnBuilder {
    pub fn from_column(col: Column) -> Self {
        match col {
            Column::Null { len } => ColumnBuilder::Null { len },
            Column::EmptyArray { len } => ColumnBuilder::EmptyArray { len },
            Column::Int8(col) => ColumnBuilder::Int8(buffer_into_mut(col)),
            Column::Int16(col) => ColumnBuilder::Int16(buffer_into_mut(col)),
            Column::Int32(col) => ColumnBuilder::Int32(buffer_into_mut(col)),
            Column::Int64(col) => ColumnBuilder::Int64(buffer_into_mut(col)),
            Column::UInt8(col) => ColumnBuilder::UInt8(buffer_into_mut(col)),
            Column::UInt16(col) => ColumnBuilder::UInt16(buffer_into_mut(col)),
            Column::UInt32(col) => ColumnBuilder::UInt32(buffer_into_mut(col)),
            Column::UInt64(col) => ColumnBuilder::UInt64(buffer_into_mut(col)),
            Column::Float32(col) => ColumnBuilder::Float32(buffer_into_mut(col)),
            Column::Float64(col) => ColumnBuilder::Float64(buffer_into_mut(col)),
            Column::Boolean(col) => ColumnBuilder::Boolean(bitmap_into_mut(col)),
            Column::String { data, offsets } => ColumnBuilder::String {
                data: buffer_into_mut(data),
                offsets: offsets.to_vec(),
            },
            Column::Array { array, offsets } => ColumnBuilder::Array {
                array: Box::new(ColumnBuilder::from_column(*array)),
                offsets: offsets.to_vec(),
            },
            Column::Nullable { column, validity } => ColumnBuilder::Nullable {
                column: Box::new(ColumnBuilder::from_column(*column)),
                validity: bitmap_into_mut(validity),
            },
            Column::Tuple { fields, len } => ColumnBuilder::Tuple {
                fields: fields
                    .iter()
                    .map(|col| ColumnBuilder::from_column(col.clone()))
                    .collect(),
                len,
            },
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnBuilder::Null { len } => *len,
            ColumnBuilder::EmptyArray { len } => *len,
            ColumnBuilder::Int8(col) => col.len(),
            ColumnBuilder::Int16(col) => col.len(),
            ColumnBuilder::Int32(col) => col.len(),
            ColumnBuilder::Int64(col) => col.len(),
            ColumnBuilder::UInt8(col) => col.len(),
            ColumnBuilder::UInt16(col) => col.len(),
            ColumnBuilder::UInt32(col) => col.len(),
            ColumnBuilder::UInt64(col) => col.len(),
            ColumnBuilder::Float32(col) => col.len(),
            ColumnBuilder::Float64(col) => col.len(),
            ColumnBuilder::Boolean(col) => col.len(),
            ColumnBuilder::String { data: _, offsets } => offsets.len() - 1,
            ColumnBuilder::Array { array: _, offsets } => offsets.len() - 1,
            ColumnBuilder::Nullable {
                column: _,
                validity,
            } => validity.len(),
            ColumnBuilder::Tuple { len, .. } => *len,
        }
    }

    pub fn with_capacity(ty: &DataType, capacity: usize) -> ColumnBuilder {
        match ty {
            DataType::Null => ColumnBuilder::Null { len: 0 },
            DataType::EmptyArray => ColumnBuilder::EmptyArray { len: 0 },
            DataType::Boolean => ColumnBuilder::Boolean(MutableBitmap::with_capacity(capacity)),
            DataType::String => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                ColumnBuilder::String {
                    data: Vec::new(),
                    offsets,
                }
            }
            DataType::UInt8 => ColumnBuilder::UInt8(Vec::with_capacity(capacity)),
            DataType::UInt16 => ColumnBuilder::UInt16(Vec::with_capacity(capacity)),
            DataType::UInt32 => ColumnBuilder::UInt32(Vec::with_capacity(capacity)),
            DataType::UInt64 => ColumnBuilder::UInt64(Vec::with_capacity(capacity)),
            DataType::Float32 => ColumnBuilder::Float32(Vec::with_capacity(capacity)),
            DataType::Float64 => ColumnBuilder::Float64(Vec::with_capacity(capacity)),
            DataType::Int8 => ColumnBuilder::Int8(Vec::with_capacity(capacity)),
            DataType::Int16 => ColumnBuilder::Int16(Vec::with_capacity(capacity)),
            DataType::Int32 => ColumnBuilder::Int32(Vec::with_capacity(capacity)),
            DataType::Int64 => ColumnBuilder::Int64(Vec::with_capacity(capacity)),
            DataType::Nullable(ty) => ColumnBuilder::Nullable {
                column: Box::new(Self::with_capacity(ty, capacity)),
                validity: MutableBitmap::with_capacity(capacity),
            },
            DataType::Array(ty) => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                ColumnBuilder::Array {
                    array: Box::new(Self::with_capacity(ty, 0)),
                    offsets,
                }
            }
            DataType::Tuple(fields) => ColumnBuilder::Tuple {
                fields: fields
                    .iter()
                    .map(|field| Self::with_capacity(field, capacity))
                    .collect(),
                len: 0,
            },
            DataType::Generic(_) => {
                unreachable!("unable to initialize column builder for generic type")
            }
        }
    }

    pub fn push(&mut self, item: ScalarRef) {
        match (self, item) {
            (ColumnBuilder::Null { len }, ScalarRef::Null) => *len += 1,
            (ColumnBuilder::EmptyArray { len }, ScalarRef::EmptyArray) => *len += 1,
            (ColumnBuilder::Int8(col), ScalarRef::Int8(value)) => col.push(value),
            (ColumnBuilder::Int16(col), ScalarRef::Int16(value)) => col.push(value),
            (ColumnBuilder::UInt8(col), ScalarRef::UInt8(value)) => col.push(value),
            (ColumnBuilder::UInt16(col), ScalarRef::UInt16(value)) => col.push(value),
            (ColumnBuilder::Boolean(col), ScalarRef::Boolean(value)) => col.push(value),
            (ColumnBuilder::String { data, offsets }, ScalarRef::String(value)) => {
                data.extend_from_slice(value);
                offsets.push(data.len() as u64);
            }
            (ColumnBuilder::Array { array, offsets }, ScalarRef::Array(value)) => {
                array.append(&ColumnBuilder::from_column(value));
                offsets.push(array.len() as u64);
            }
            (ColumnBuilder::Nullable { column, validity }, ScalarRef::Null) => {
                column.push_default();
                validity.push(false);
            }
            (ColumnBuilder::Nullable { column, validity }, scalar) => {
                column.push(scalar);
                validity.push(true);
            }
            (ColumnBuilder::Tuple { fields, len }, ScalarRef::Tuple(value)) => {
                assert_eq!(fields.len(), value.len());
                for (field, scalar) in fields.iter_mut().zip(value.iter()) {
                    field.push(scalar.clone());
                }
                *len += 1;
            }
            (builder, scalar) => unreachable!("unable to push {scalar:?} to {builder:?}"),
        }
    }

    pub fn push_default(&mut self) {
        match self {
            ColumnBuilder::Null { len } => *len += 1,
            ColumnBuilder::EmptyArray { len } => *len += 1,
            ColumnBuilder::Int8(col) => col.push(0),
            ColumnBuilder::Int16(col) => col.push(0),
            ColumnBuilder::Int32(col) => col.push(0),
            ColumnBuilder::Int64(col) => col.push(0),
            ColumnBuilder::UInt8(col) => col.push(0),
            ColumnBuilder::UInt16(col) => col.push(0),
            ColumnBuilder::UInt32(col) => col.push(0),
            ColumnBuilder::UInt64(col) => col.push(0),
            ColumnBuilder::Float32(col) => col.push(0f32),
            ColumnBuilder::Float64(col) => col.push(0f64),
            ColumnBuilder::Boolean(col) => col.push(false),
            ColumnBuilder::String { data, offsets } => {
                offsets.push(data.len() as u64);
            }
            ColumnBuilder::Array { array, offsets } => {
                offsets.push(array.len() as u64);
            }
            ColumnBuilder::Nullable { column, validity } => {
                column.push_default();
                validity.push(false);
            }
            ColumnBuilder::Tuple { fields, len } => {
                for field in fields {
                    field.push_default();
                }
                *len += 1;
            }
        }
    }

    pub fn append(&mut self, other: &ColumnBuilder) {
        match (self, other) {
            (ColumnBuilder::Null { len }, ColumnBuilder::Null { len: other_len }) => {
                *len += other_len;
            }
            (ColumnBuilder::EmptyArray { len }, ColumnBuilder::EmptyArray { len: other_len }) => {
                *len += other_len;
            }
            (ColumnBuilder::Int8(builder), ColumnBuilder::Int8(other_builder)) => {
                builder.extend_from_slice(other_builder);
            }
            (ColumnBuilder::Int16(builder), ColumnBuilder::Int16(other_builder)) => {
                builder.extend_from_slice(other_builder);
            }
            (ColumnBuilder::UInt8(builder), ColumnBuilder::UInt8(other_builder)) => {
                builder.extend_from_slice(other_builder);
            }
            (ColumnBuilder::UInt16(builder), ColumnBuilder::UInt16(other_builder)) => {
                builder.extend_from_slice(other_builder);
            }
            (ColumnBuilder::Boolean(builder), ColumnBuilder::Boolean(other_builder)) => {
                append_bitmap(builder, other_builder);
            }
            (
                ColumnBuilder::String { data, offsets },
                ColumnBuilder::String {
                    data: other_data,
                    offsets: other_offsets,
                },
            ) => {
                data.extend_from_slice(other_data);
                let start = offsets.last().cloned().unwrap();
                offsets.extend(other_offsets.iter().skip(1).map(|offset| start + offset));
            }
            (
                ColumnBuilder::Array { array, offsets },
                ColumnBuilder::Array {
                    array: other_array,
                    offsets: other_offsets,
                },
            ) => {
                array.append(other_array);
                let start = offsets.last().cloned().unwrap();
                offsets.extend(other_offsets.iter().skip(1).map(|offset| start + offset));
            }
            (
                ColumnBuilder::Nullable { column, validity },
                ColumnBuilder::Nullable {
                    column: other_column,
                    validity: other_validity,
                },
            ) => {
                column.append(other_column);
                append_bitmap(validity, other_validity);
            }
            (
                ColumnBuilder::Tuple { fields, len },
                ColumnBuilder::Tuple {
                    fields: other_fields,
                    len: other_len,
                },
            ) => {
                assert_eq!(fields.len(), other_fields.len());
                for (field, other_field) in fields.iter_mut().zip(other_fields.iter()) {
                    field.append(other_field);
                }
                *len += other_len;
            }
            (this, other) => unreachable!("unable append {other:?} onto {this:?}"),
        }
    }

    pub fn build(self) -> Column {
        match self {
            ColumnBuilder::Null { len } => Column::Null { len },
            ColumnBuilder::EmptyArray { len } => Column::EmptyArray { len },
            ColumnBuilder::Int8(builder) => Column::Int8(builder.into()),
            ColumnBuilder::Int16(builder) => Column::Int16(builder.into()),
            ColumnBuilder::Int32(builder) => Column::Int32(builder.into()),
            ColumnBuilder::Int64(builder) => Column::Int64(builder.into()),
            ColumnBuilder::UInt8(builder) => Column::UInt8(builder.into()),
            ColumnBuilder::UInt16(builder) => Column::UInt16(builder.into()),
            ColumnBuilder::UInt32(builder) => Column::UInt32(builder.into()),
            ColumnBuilder::UInt64(builder) => Column::UInt64(builder.into()),
            ColumnBuilder::Float32(builder) => Column::Float32(builder.into()),
            ColumnBuilder::Float64(builder) => Column::Float64(builder.into()),
            ColumnBuilder::Boolean(builder) => Column::Boolean(builder.into()),
            ColumnBuilder::String { data, offsets } => Column::String {
                data: data.into(),
                offsets: offsets.into(),
            },
            ColumnBuilder::Array { array, offsets } => Column::Array {
                array: Box::new(array.build()),
                offsets: offsets.into(),
            },
            ColumnBuilder::Nullable { column, validity } => Column::Nullable {
                column: Box::new(column.build()),
                validity: validity.into(),
            },
            ColumnBuilder::Tuple { fields, len } => Column::Tuple {
                fields: fields.into_iter().map(|field| field.build()).collect(),
                len,
            },
        }
    }

    pub fn build_scalar(self) -> Scalar {
        assert_eq!(self.len(), 1);
        match self {
            ColumnBuilder::Null { .. } => Scalar::Null,
            ColumnBuilder::EmptyArray { .. } => Scalar::EmptyArray,
            ColumnBuilder::Int8(builder) => Scalar::Int8(builder[0]),
            ColumnBuilder::Int16(builder) => Scalar::Int16(builder[0]),
            ColumnBuilder::Int32(builder) => Scalar::Int32(builder[0]),
            ColumnBuilder::Int64(builder) => Scalar::Int64(builder[0]),
            ColumnBuilder::UInt8(builder) => Scalar::UInt8(builder[0]),
            ColumnBuilder::UInt16(builder) => Scalar::UInt16(builder[0]),
            ColumnBuilder::UInt32(builder) => Scalar::UInt32(builder[0]),
            ColumnBuilder::UInt64(builder) => Scalar::UInt64(builder[0]),
            ColumnBuilder::Float32(builder) => Scalar::Float32(builder[0]),
            ColumnBuilder::Float64(builder) => Scalar::Float64(builder[0]),
            ColumnBuilder::Boolean(builder) => Scalar::Boolean(builder.get(0)),
            ColumnBuilder::String { data, offsets } => {
                Scalar::String(data[(offsets[0] as usize)..(offsets[1] as usize)].to_vec())
            }
            ColumnBuilder::Array { array, offsets } => Scalar::Array(
                array
                    .build()
                    .slice((offsets[0] as usize)..(offsets[1] as usize)),
            ),
            ColumnBuilder::Nullable { column, validity } => {
                if validity.get(0) {
                    column.build_scalar()
                } else {
                    Scalar::Null
                }
            }
            ColumnBuilder::Tuple { fields, .. } => Scalar::Tuple(
                fields
                    .into_iter()
                    .map(|field| field.build_scalar())
                    .collect(),
            ),
        }
    }
}

pub struct ColumnIterator<'a> {
    column: &'a Column,
    index: usize,
    len: usize,
}

impl<'a> Iterator for ColumnIterator<'a> {
    type Item = ScalarRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.len {
            let item = self.column.index(self.index)?;
            self.index += 1;
            Some(item)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remain = self.len - self.index;
        (remain, Some(remain))
    }
}

unsafe impl<'a> TrustedLen for ColumnIterator<'a> {}
