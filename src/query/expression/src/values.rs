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

use std::ops::Range;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::trusted_len::TrustedLen;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde::Deserialize;
use serde::Serialize;

use crate::property::BooleanDomain;
use crate::property::Domain;
use crate::property::NullableDomain;
use crate::property::StringDomain;
use crate::types::array::ArrayColumn;
use crate::types::array::ArrayColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::nullable::NullableColumnBuilder;
use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::*;
use crate::util::append_bitmap;
use crate::util::bitmap_into_mut;
use crate::util::buffer_into_mut;
use crate::util::constant_bitmap;
use crate::NumberDomain;

#[derive(Debug, Clone, EnumAsInner)]
pub enum Value<T: ValueType> {
    Scalar(T::Scalar),
    Column(T::Column),
}

#[derive(Debug, Clone, EnumAsInner)]
pub enum ValueRef<'a, T: ValueType> {
    Scalar(T::ScalarRef<'a>),
    Column(T::Column),
}

#[derive(Debug, Clone, PartialEq, Default, EnumAsInner, Serialize, Deserialize)]
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
    #[serde(skip)]
    Array(Column),
    Tuple(Vec<Scalar>),
}

#[derive(Clone, PartialEq, Default, EnumAsInner)]
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
    Null { len: usize },
    EmptyArray { len: usize },
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
    String(StringColumn),
    Array(Box<ArrayColumn<AnyType>>),
    Nullable(Box<NullableColumn<AnyType>>),
    Tuple { fields: Vec<Column>, len: usize },
}

#[derive(Debug, Clone, PartialEq, EnumAsInner)]
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
    String(StringColumnBuilder),
    Array(Box<ArrayColumnBuilder<AnyType>>),
    Nullable(Box<NullableColumnBuilder<AnyType>>),
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

    pub fn sematically_eq(&'a self, other: &'a Self) -> bool {
        match (self, other) {
            (ValueRef::Scalar(s1), ValueRef::Scalar(s2)) => s1 == s2,
            (ValueRef::Column(c1), ValueRef::Column(c2)) => c1 == c2,
            (ValueRef::Scalar(s), ValueRef::Column(c))
            | (ValueRef::Column(c), ValueRef::Scalar(s)) => {
                T::iter_column(c).all(|scalar| scalar == *s)
            }
        }
    }

    pub fn index(&'a self, index: usize) -> Option<T::ScalarRef<'a>> {
        match self {
            ValueRef::Scalar(scalar) => Some(scalar.clone()),
            ValueRef::Column(col) => T::index_column(col, index),
        }
    }

    /// # Safety
    pub unsafe fn index_unchecked(&'a self, index: usize) -> T::ScalarRef<'a> {
        match self {
            ValueRef::Scalar(scalar) => scalar.clone(),
            ValueRef::Column(c) => T::index_column_unchecked(c, index),
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
    pub fn try_downcast<T: ArgType>(&self) -> Option<ValueRef<'_, T>> {
        Some(match self {
            ValueRef::Scalar(scalar) => ValueRef::Scalar(T::try_downcast_scalar(scalar)?),
            ValueRef::Column(col) => ValueRef::Column(T::try_downcast_column(col)?),
        })
    }

    pub fn domain(&self) -> Domain {
        match self {
            ValueRef::Scalar(scalar) => scalar.domain(),
            ValueRef::Column(col) => col.domain(),
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
            ScalarRef::String(s) => ColumnBuilder::String(StringColumnBuilder::repeat(s, n)),
            ScalarRef::Array(col) => {
                ColumnBuilder::Array(Box::new(ArrayColumnBuilder::repeat(col, n)))
            }
            ScalarRef::Tuple(fields) => ColumnBuilder::Tuple {
                fields: fields.iter().map(|field| field.repeat(n)).collect(),
                len: n,
            },
        }
    }

    pub fn domain(&self) -> Domain {
        match self {
            ScalarRef::Null => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            ScalarRef::EmptyArray => Domain::Array(None),
            ScalarRef::Int8(i) => Domain::Int8(NumberDomain { min: *i, max: *i }),
            ScalarRef::Int16(i) => Domain::Int16(NumberDomain { min: *i, max: *i }),
            ScalarRef::Int32(i) => Domain::Int32(NumberDomain { min: *i, max: *i }),
            ScalarRef::Int64(i) => Domain::Int64(NumberDomain { min: *i, max: *i }),
            ScalarRef::UInt8(i) => Domain::UInt8(NumberDomain { min: *i, max: *i }),
            ScalarRef::UInt16(i) => Domain::UInt16(NumberDomain { min: *i, max: *i }),
            ScalarRef::UInt32(i) => Domain::UInt32(NumberDomain { min: *i, max: *i }),
            ScalarRef::UInt64(i) => Domain::UInt64(NumberDomain { min: *i, max: *i }),
            ScalarRef::Float32(i) => Domain::Float32(NumberDomain { min: *i, max: *i }),
            ScalarRef::Float64(i) => Domain::Float64(NumberDomain { min: *i, max: *i }),
            ScalarRef::Boolean(true) => Domain::Boolean(BooleanDomain {
                has_false: false,
                has_true: true,
            }),
            ScalarRef::Boolean(false) => Domain::Boolean(BooleanDomain {
                has_false: true,
                has_true: false,
            }),
            ScalarRef::String(s) => Domain::String(StringDomain {
                min: s.to_vec(),
                max: Some(s.to_vec()),
            }),
            ScalarRef::Array(array) => Domain::Array(Some(Box::new(array.domain()))),
            ScalarRef::Tuple(fields) => {
                Domain::Tuple(fields.iter().map(|field| field.domain()).collect())
            }
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
            Column::String(col) => col.len(),
            Column::Array(col) => col.len(),
            Column::Nullable(col) => col.len(),
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
            Column::String(col) => Some(ScalarRef::String(col.index(index)?)),
            Column::Array(col) => Some(ScalarRef::Array(col.index(index)?)),
            Column::Nullable(col) => Some(col.index(index)?.unwrap_or(ScalarRef::Null)),
            Column::Tuple { fields, .. } => Some(ScalarRef::Tuple(
                fields
                    .iter()
                    .map(|field| field.index(index))
                    .collect::<Option<Vec<_>>>()?,
            )),
        }
    }

    /// # Safety
    /// Assumes that the `index` is not out of range.
    pub unsafe fn index_unchecked(&self, index: usize) -> ScalarRef {
        match self {
            Column::Null { .. } => ScalarRef::Null,
            Column::EmptyArray { .. } => ScalarRef::EmptyArray,
            Column::Int8(col) => ScalarRef::Int8(*col.get_unchecked(index)),
            Column::Int16(col) => ScalarRef::Int16(*col.get_unchecked(index)),
            Column::Int32(col) => ScalarRef::Int32(*col.get_unchecked(index)),
            Column::Int64(col) => ScalarRef::Int64(*col.get_unchecked(index)),
            Column::UInt8(col) => ScalarRef::UInt8(*col.get_unchecked(index)),
            Column::UInt16(col) => ScalarRef::UInt16(*col.get_unchecked(index)),
            Column::UInt32(col) => ScalarRef::UInt32(*col.get_unchecked(index)),
            Column::UInt64(col) => ScalarRef::UInt64(*col.get_unchecked(index)),
            Column::Float32(col) => ScalarRef::Float32(*col.get_unchecked(index)),
            Column::Float64(col) => ScalarRef::Float64(*col.get_unchecked(index)),
            Column::Boolean(col) => ScalarRef::Boolean(col.get_bit_unchecked(index)),
            Column::String(col) => ScalarRef::String(col.index_unchecked(index)),
            Column::Array(col) => ScalarRef::Array(col.index_unchecked(index)),
            Column::Nullable(col) => col.index_unchecked(index).unwrap_or(ScalarRef::Null),
            Column::Tuple { fields, .. } => ScalarRef::Tuple(
                fields
                    .iter()
                    .map(|field| field.index_unchecked(index))
                    .collect::<Vec<_>>(),
            ),
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
            Column::String(col) => Column::String(col.slice(range)),
            Column::Array(col) => Column::Array(Box::new(col.slice(range))),
            Column::Nullable(col) => Column::Nullable(Box::new(col.slice(range))),
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

    pub fn domain(&self) -> Domain {
        assert!(self.len() > 0);
        match self {
            Column::Null { .. } => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            Column::EmptyArray { .. } => Domain::Array(None),
            Column::Int8(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                Domain::Int8(NumberDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::Int16(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                Domain::Int16(NumberDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::Int32(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                Domain::Int32(NumberDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::Int64(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                Domain::Int64(NumberDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::UInt8(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                Domain::UInt8(NumberDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::UInt16(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                Domain::UInt16(NumberDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::UInt32(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                Domain::UInt32(NumberDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::UInt64(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                Domain::UInt64(NumberDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::Float32(col) => {
                let (min, max) = col
                    .iter()
                    .cloned()
                    .map(OrderedFloat::from)
                    .minmax()
                    .into_option()
                    .unwrap();
                Domain::Float32(NumberDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::Float64(col) => {
                let (min, max) = col
                    .iter()
                    .cloned()
                    .map(OrderedFloat::from)
                    .minmax()
                    .into_option()
                    .unwrap();
                Domain::Float64(NumberDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::Boolean(col) => Domain::Boolean(BooleanDomain {
                has_false: col.unset_bits() > 0,
                has_true: col.len() - col.unset_bits() > 0,
            }),
            Column::String(col) => {
                let (min, max) = StringType::iter_column(col).minmax().into_option().unwrap();
                Domain::String(StringDomain {
                    min: min.to_vec(),
                    max: Some(max.to_vec()),
                })
            }
            Column::Array(col) => {
                let inner_domain = col.values.domain();
                Domain::Array(Some(Box::new(inner_domain)))
            }
            Column::Nullable(col) => {
                let inner_domain = col.column.domain();
                Domain::Nullable(NullableDomain {
                    has_null: col.validity.unset_bits() > 0,
                    value: Some(Box::new(inner_domain)),
                })
            }
            Column::Tuple { fields, .. } => {
                let domains = fields.iter().map(|col| col.domain()).collect::<Vec<_>>();
                Domain::Tuple(domains)
            }
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
            Column::Array(box ArrayColumn {
                values: Column::Nullable(box NullableColumn { column, .. }),
                ..
            }) => ArrowDataType::LargeList(Box::new(Field::new(
                "list".to_string(),
                column.arrow_type(),
                true,
            ))),
            Column::Array(box ArrayColumn { values, .. }) => ArrowDataType::LargeList(Box::new(
                Field::new("list".to_string(), values.arrow_type(), false),
            )),
            Column::Nullable(box NullableColumn { column, .. }) => column.arrow_type(),
            Column::Tuple { fields, .. } => {
                let arrow_fields = fields
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| match field {
                        Column::Nullable(box NullableColumn { column, .. }) => {
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
            Column::String(col) => {
                Box::new(common_arrow::arrow::array::BinaryArray::<i64>::from_data(
                    self.arrow_type(),
                    col.offsets.iter().map(|offset| *offset as i64).collect(),
                    col.data.clone(),
                    None,
                ))
            }
            Column::Array(col) => {
                Box::new(common_arrow::arrow::array::ListArray::<i64>::from_data(
                    self.arrow_type(),
                    col.offsets.iter().map(|offset| *offset as i64).collect(),
                    col.values.as_arrow(),
                    None,
                ))
            }
            Column::Nullable(col) => {
                let arrow_array = col.column.as_arrow();
                match arrow_array.data_type() {
                    ArrowType::Null => arrow_array,
                    ArrowType::Extension(_, t, _) if **t == ArrowType::Null => arrow_array,
                    _ => arrow_array.with_validity(Some(col.validity.clone())),
                }
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

        let column = match arrow_col.data_type() {
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
                Column::String(StringColumn {
                    data: arrow_col.values().clone(),
                    offsets: offsets.into(),
                })
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
                Column::String(StringColumn {
                    data: arrow_col.values().clone(),
                    offsets: offsets.into(),
                })
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
                Column::String(StringColumn {
                    data: arrow_col.values().clone(),
                    offsets: offsets.into(),
                })
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
                Column::String(StringColumn {
                    data: arrow_col.values().clone(),
                    offsets: offsets.into(),
                })
            }
            ArrowDataType::LargeList(_) => {
                let values_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::ListArray<i64>>()
                    .expect("fail to read from arrow: array should be `ListArray<i64>`");
                let values = Column::from_arrow(&**values_col.values());
                let offsets = values_col
                    .offsets()
                    .iter()
                    .map(|x| *x as u64)
                    .collect::<Vec<_>>();
                Column::Array(Box::new(ArrayColumn {
                    values,
                    offsets: offsets.into(),
                }))
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
            Column::Nullable(Box::new(NullableColumn {
                column,
                validity: validity.clone(),
            }))
        } else {
            column
        }
    }

    pub fn memory_size(&self) -> usize {
        match self {
            Column::Null { .. } => std::mem::size_of::<usize>(),
            Column::EmptyArray { .. } => std::mem::size_of::<usize>(),
            Column::Int8(_) => self.len(),
            Column::Int16(_) => self.len() * 2,
            Column::Int32(_) => self.len() * 4,
            Column::Int64(_) => self.len() * 8,
            Column::UInt8(_) => self.len(),
            Column::UInt16(_) => self.len() * 2,
            Column::UInt32(_) => self.len() * 4,
            Column::UInt64(_) => self.len() * 8,
            Column::Float32(_) => self.len() * 4,
            Column::Float64(_) => self.len() * 8,
            Column::Boolean(c) => c.as_slice().0.len(),
            Column::String(col) => col.data.len() + col.offsets.len() * 8,
            Column::Array(col) => col.values.memory_size() + col.offsets.len() * 8,
            Column::Nullable(c) => c.column.memory_size() + c.validity.as_slice().0.len(),
            Column::Tuple { fields, .. } => fields.iter().map(|f| f.memory_size()).sum(),
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
            Column::String(col) => ColumnBuilder::String(StringColumnBuilder::from_column(col)),
            Column::Array(box col) => {
                ColumnBuilder::Array(Box::new(ArrayColumnBuilder::from_column(col)))
            }
            Column::Nullable(box col) => {
                ColumnBuilder::Nullable(Box::new(NullableColumnBuilder::from_column(col)))
            }
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
            ColumnBuilder::Int8(builder) => builder.len(),
            ColumnBuilder::Int16(builder) => builder.len(),
            ColumnBuilder::Int32(builder) => builder.len(),
            ColumnBuilder::Int64(builder) => builder.len(),
            ColumnBuilder::UInt8(builder) => builder.len(),
            ColumnBuilder::UInt16(builder) => builder.len(),
            ColumnBuilder::UInt32(builder) => builder.len(),
            ColumnBuilder::UInt64(builder) => builder.len(),
            ColumnBuilder::Float32(builder) => builder.len(),
            ColumnBuilder::Float64(builder) => builder.len(),
            ColumnBuilder::Boolean(builder) => builder.len(),
            ColumnBuilder::String(builder) => builder.len(),
            ColumnBuilder::Array(builder) => builder.len(),
            ColumnBuilder::Nullable(builder) => builder.len(),
            ColumnBuilder::Tuple { len, .. } => *len,
        }
    }

    pub fn with_capacity(ty: &DataType, capacity: usize) -> ColumnBuilder {
        match ty {
            DataType::Null => ColumnBuilder::Null { len: 0 },
            DataType::EmptyArray => ColumnBuilder::EmptyArray { len: 0 },
            DataType::Boolean => ColumnBuilder::Boolean(MutableBitmap::with_capacity(capacity)),
            DataType::String => {
                ColumnBuilder::String(StringColumnBuilder::with_capacity(capacity, 0))
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
            DataType::Nullable(ty) => ColumnBuilder::Nullable(Box::new(NullableColumnBuilder {
                builder: Self::with_capacity(ty, capacity),
                validity: MutableBitmap::with_capacity(capacity),
            })),
            DataType::Array(ty) => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                ColumnBuilder::Array(Box::new(ArrayColumnBuilder {
                    builder: Self::with_capacity(ty, 0),
                    offsets,
                }))
            }
            DataType::Map(ty) => Self::with_capacity(
                &DataType::Array(Box::new(DataType::Tuple(vec![
                    DataType::String,
                    (**ty).clone(),
                ]))),
                capacity,
            ),
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
            (ColumnBuilder::Int8(builder), ScalarRef::Int8(value)) => builder.push(value),
            (ColumnBuilder::Int16(builder), ScalarRef::Int16(value)) => builder.push(value),
            (ColumnBuilder::Int32(builder), ScalarRef::Int32(value)) => builder.push(value),
            (ColumnBuilder::Int64(builder), ScalarRef::Int64(value)) => builder.push(value),
            (ColumnBuilder::UInt8(builder), ScalarRef::UInt8(value)) => builder.push(value),
            (ColumnBuilder::UInt16(builder), ScalarRef::UInt16(value)) => builder.push(value),
            (ColumnBuilder::UInt32(builder), ScalarRef::UInt32(value)) => builder.push(value),
            (ColumnBuilder::UInt64(builder), ScalarRef::UInt64(value)) => builder.push(value),
            (ColumnBuilder::Float32(builder), ScalarRef::Float32(value)) => builder.push(value),
            (ColumnBuilder::Float64(builder), ScalarRef::Float64(value)) => builder.push(value),
            (ColumnBuilder::Boolean(builder), ScalarRef::Boolean(value)) => builder.push(value),
            (ColumnBuilder::String(builder), ScalarRef::String(value)) => {
                builder.put_slice(value);
                builder.commit_row();
            }
            (ColumnBuilder::Array(builder), ScalarRef::Array(value)) => {
                builder.push(value);
            }
            (ColumnBuilder::Nullable(builder), ScalarRef::Null) => {
                builder.push(None);
            }
            (ColumnBuilder::Nullable(builder), scalar) => {
                builder.push(Some(scalar));
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
            ColumnBuilder::Int8(builder) => builder.push(0),
            ColumnBuilder::Int16(builder) => builder.push(0),
            ColumnBuilder::Int32(builder) => builder.push(0),
            ColumnBuilder::Int64(builder) => builder.push(0),
            ColumnBuilder::UInt8(builder) => builder.push(0),
            ColumnBuilder::UInt16(builder) => builder.push(0),
            ColumnBuilder::UInt32(builder) => builder.push(0),
            ColumnBuilder::UInt64(builder) => builder.push(0),
            ColumnBuilder::Float32(builder) => builder.push(0f32),
            ColumnBuilder::Float64(builder) => builder.push(0f64),
            ColumnBuilder::Boolean(builder) => builder.push(false),
            ColumnBuilder::String(builder) => builder.commit_row(),
            ColumnBuilder::Array(builder) => builder.push_default(),
            ColumnBuilder::Nullable(builder) => builder.push_null(),
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
            (ColumnBuilder::String(builder), ColumnBuilder::String(other_builder)) => {
                builder.append(other_builder);
            }
            (ColumnBuilder::Array(builder), ColumnBuilder::Array(other_builder)) => {
                builder.append(other_builder);
            }
            (ColumnBuilder::Nullable(builder), ColumnBuilder::Nullable(other_builder)) => {
                builder.append(other_builder);
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
            ColumnBuilder::String(builder) => Column::String(builder.build()),
            ColumnBuilder::Array(builder) => Column::Array(Box::new(builder.build())),
            ColumnBuilder::Nullable(builder) => Column::Nullable(Box::new(builder.build())),
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
            ColumnBuilder::String(builder) => Scalar::String(builder.build_scalar()),
            ColumnBuilder::Array(builder) => Scalar::Array(builder.build_scalar()),
            ColumnBuilder::Nullable(builder) => builder.build_scalar().unwrap_or(Scalar::Null),
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
