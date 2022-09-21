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

use std::cmp::Ordering;
use std::ops::Range;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::trusted_len::TrustedLen;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;
use serde::de::Visitor;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::property::Domain;
use crate::types::array::ArrayColumn;
use crate::types::array::ArrayColumnBuilder;
use crate::types::boolean::BooleanDomain;
use crate::types::nullable::NullableColumn;
use crate::types::nullable::NullableColumnBuilder;
use crate::types::nullable::NullableDomain;
use crate::types::number::NumberColumn;
use crate::types::number::NumberColumnBuilder;
use crate::types::number::NumberScalar;
use crate::types::number::F32;
use crate::types::number::F64;
use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::string::StringDomain;
use crate::types::timestamp::Timestamp;
use crate::types::timestamp::TimestampColumn;
use crate::types::timestamp::TimestampColumnBuilder;
use crate::types::timestamp::TimestampDomain;
use crate::types::variant::DEFAULT_JSONB;
use crate::types::*;
use crate::util::append_bitmap;
use crate::util::bitmap_into_mut;
use crate::util::constant_bitmap;
use crate::util::deserialize_arrow_array;
use crate::util::serialize_arrow_array;
use crate::with_number_type;

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

#[derive(Debug, Clone, PartialEq, PartialOrd, Default, EnumAsInner, Serialize, Deserialize)]
pub enum Scalar {
    #[default]
    Null,
    EmptyArray,
    Number(NumberScalar),
    Timestamp(Timestamp),
    Boolean(bool),
    String(Vec<u8>),
    Array(Column),
    Tuple(Vec<Scalar>),
    Variant(Vec<u8>),
}

#[derive(Clone, PartialEq, PartialOrd, Default, EnumAsInner)]
pub enum ScalarRef<'a> {
    #[default]
    Null,
    EmptyArray,
    Number(NumberScalar),
    Boolean(bool),
    String(&'a [u8]),
    Timestamp(Timestamp),
    Array(Column),
    Tuple(Vec<ScalarRef<'a>>),
    Variant(&'a [u8]),
}

#[derive(Clone, PartialEq, EnumAsInner)]
pub enum Column {
    Null { len: usize },
    EmptyArray { len: usize },
    Number(NumberColumn),
    Boolean(Bitmap),
    String(StringColumn),
    Timestamp(TimestampColumn),
    Array(Box<ArrayColumn<AnyType>>),
    Nullable(Box<NullableColumn<AnyType>>),
    Tuple { fields: Vec<Column>, len: usize },
    Variant(StringColumn),
}

#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum ColumnBuilder {
    Null {
        len: usize,
    },
    EmptyArray {
        len: usize,
    },
    Number(NumberColumnBuilder),
    Boolean(MutableBitmap),
    String(StringColumnBuilder),
    Timestamp(TimestampColumnBuilder),
    Array(Box<ArrayColumnBuilder<AnyType>>),
    Nullable(Box<NullableColumnBuilder<AnyType>>),
    Tuple {
        fields: Vec<ColumnBuilder>,
        len: usize,
    },
    Variant(StringColumnBuilder),
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
    pub fn try_downcast<T: ValueType>(&self) -> Option<ValueRef<'_, T>> {
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
            Scalar::Number(n) => ScalarRef::Number(*n),
            Scalar::Boolean(b) => ScalarRef::Boolean(*b),
            Scalar::String(s) => ScalarRef::String(s.as_slice()),
            Scalar::Timestamp(t) => ScalarRef::Timestamp(*t),
            Scalar::Array(col) => ScalarRef::Array(col.clone()),
            Scalar::Tuple(fields) => ScalarRef::Tuple(fields.iter().map(Scalar::as_ref).collect()),
            Scalar::Variant(s) => ScalarRef::Variant(s.as_slice()),
        }
    }
}

impl<'a> ScalarRef<'a> {
    pub fn to_owned(&self) -> Scalar {
        match self {
            ScalarRef::Null => Scalar::Null,
            ScalarRef::EmptyArray => Scalar::EmptyArray,
            ScalarRef::Number(n) => Scalar::Number(*n),
            ScalarRef::Boolean(b) => Scalar::Boolean(*b),
            ScalarRef::String(s) => Scalar::String(s.to_vec()),
            ScalarRef::Timestamp(t) => Scalar::Timestamp(*t),
            ScalarRef::Array(col) => Scalar::Array(col.clone()),
            ScalarRef::Tuple(fields) => {
                Scalar::Tuple(fields.iter().map(ScalarRef::to_owned).collect())
            }
            ScalarRef::Variant(s) => Scalar::Variant(s.to_vec()),
        }
    }

    pub fn repeat(&self, n: usize) -> ColumnBuilder {
        match self {
            ScalarRef::Null => ColumnBuilder::Null { len: n },
            ScalarRef::EmptyArray => ColumnBuilder::EmptyArray { len: n },
            ScalarRef::Number(num) => ColumnBuilder::Number(num.repeat(n)),
            ScalarRef::Boolean(b) => ColumnBuilder::Boolean(constant_bitmap(*b, n)),
            ScalarRef::String(s) => ColumnBuilder::String(StringColumnBuilder::repeat(s, n)),
            ScalarRef::Timestamp(t) => {
                ColumnBuilder::Timestamp(TimestampColumnBuilder::repeat(*t, n))
            }
            ScalarRef::Array(col) => {
                ColumnBuilder::Array(Box::new(ArrayColumnBuilder::repeat(col, n)))
            }
            ScalarRef::Tuple(fields) => ColumnBuilder::Tuple {
                fields: fields.iter().map(|field| field.repeat(n)).collect(),
                len: n,
            },
            ScalarRef::Variant(s) => ColumnBuilder::Variant(StringColumnBuilder::repeat(s, n)),
        }
    }

    pub fn domain(&self) -> Domain {
        match self {
            ScalarRef::Null => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            ScalarRef::EmptyArray => Domain::Array(None),
            ScalarRef::Number(num) => Domain::Number(num.domain()),
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
            ScalarRef::Timestamp(t) => Domain::Timestamp(TimestampDomain {
                min: t.ts,
                max: t.ts,
                precision: t.precision,
            }),
            ScalarRef::Array(array) => Domain::Array(Some(Box::new(array.domain()))),
            ScalarRef::Tuple(fields) => {
                Domain::Tuple(fields.iter().map(|field| field.domain()).collect())
            }
            ScalarRef::Variant(_) => Domain::Undefined,
        }
    }
}

impl PartialOrd for Column {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Column::Null { .. }, Column::Null { .. }) => Some(Ordering::Equal),
            (Column::EmptyArray { .. }, Column::EmptyArray { .. }) => Some(Ordering::Equal),
            (Column::Number(col1), Column::Number(col2)) => {
                with_number_type!(|NUM_TYPE| match (col1, col2) {
                    (NumberColumn::NUM_TYPE(c1), NumberColumn::NUM_TYPE(c2)) =>
                        c1.iter().partial_cmp(c2.iter()),
                    _ => unreachable!(),
                })
            }
            (Column::Boolean(col1), Column::Boolean(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::String(col1), Column::String(col2))
            | (Column::Variant(col1), Column::Variant(col2)) => {
                col1.iter().partial_cmp(col2.iter())
            }
            (Column::Timestamp(col1), Column::Timestamp(col2)) => {
                col1.iter().partial_cmp(col2.iter())
            }
            (Column::Array(col1), Column::Array(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Nullable(col1), Column::Nullable(col2)) => {
                col1.iter().partial_cmp(col2.iter())
            }
            (
                Column::Tuple {
                    fields: col1,
                    len: len1,
                },
                Column::Tuple {
                    fields: col2,
                    len: len2,
                },
            ) => {
                // array(tuple) : [      t1       ,       t2       ,       t3       ]
                // array1       : [(f11, f21, f31), (f12, f22, f32), (f13, f23, f33)]
                // array2       : [(f11, f21, f31), (f12, f22, f32), (f13, f23, f33)]
                if col1.len() == col2.len() {
                    for i in 0..*len1.min(len2) {
                        let iter1 = col1.iter().map(|c| c.index(i));
                        let iter2 = col2.iter().map(|c| c.index(i));
                        let ord = iter1.partial_cmp(iter2);
                        if ord.unwrap_or(Ordering::Equal) != Ordering::Equal {
                            return ord;
                        }
                    }
                    len1.partial_cmp(len2)
                } else {
                    unreachable!()
                }
            }
            _ => unreachable!(),
        }
    }
}

impl Column {
    pub fn len(&self) -> usize {
        match self {
            Column::Null { len } => *len,
            Column::EmptyArray { len } => *len,
            Column::Number(col) => col.len(),
            Column::Boolean(col) => col.len(),
            Column::String(col) => col.len(),
            Column::Timestamp(col) => col.len(),
            Column::Array(col) => col.len(),
            Column::Nullable(col) => col.len(),
            Column::Tuple { len, .. } => *len,
            Column::Variant(col) => col.len(),
        }
    }

    pub fn index(&self, index: usize) -> Option<ScalarRef> {
        match self {
            Column::Null { .. } => Some(ScalarRef::Null),
            Column::EmptyArray { .. } => Some(ScalarRef::EmptyArray),
            Column::Number(col) => Some(ScalarRef::Number(col.index(index)?)),
            Column::Boolean(col) => Some(ScalarRef::Boolean(col.get(index)?)),
            Column::String(col) => Some(ScalarRef::String(col.index(index)?)),
            Column::Timestamp(col) => Some(ScalarRef::Timestamp(col.index(index)?)),
            Column::Array(col) => Some(ScalarRef::Array(col.index(index)?)),
            Column::Nullable(col) => Some(col.index(index)?.unwrap_or(ScalarRef::Null)),
            Column::Tuple { fields, .. } => Some(ScalarRef::Tuple(
                fields
                    .iter()
                    .map(|field| field.index(index))
                    .collect::<Option<Vec<_>>>()?,
            )),
            Column::Variant(col) => Some(ScalarRef::Variant(col.index(index)?)),
        }
    }

    /// # Safety
    /// Assumes that the `index` is not out of range.
    pub unsafe fn index_unchecked(&self, index: usize) -> ScalarRef {
        match self {
            Column::Null { .. } => ScalarRef::Null,
            Column::EmptyArray { .. } => ScalarRef::EmptyArray,
            Column::Number(col) => ScalarRef::Number(col.index_unchecked(index)),
            Column::Boolean(col) => ScalarRef::Boolean(col.get_bit_unchecked(index)),
            Column::String(col) => ScalarRef::String(col.index_unchecked(index)),
            Column::Timestamp(col) => ScalarRef::Timestamp(col.index_unchecked(index)),
            Column::Array(col) => ScalarRef::Array(col.index_unchecked(index)),
            Column::Nullable(col) => col.index_unchecked(index).unwrap_or(ScalarRef::Null),
            Column::Tuple { fields, .. } => ScalarRef::Tuple(
                fields
                    .iter()
                    .map(|field| field.index_unchecked(index))
                    .collect::<Vec<_>>(),
            ),
            Column::Variant(col) => ScalarRef::Variant(col.index_unchecked(index)),
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
            Column::Number(col) => Column::Number(col.slice(range)),
            Column::Boolean(col) => {
                Column::Boolean(col.clone().slice(range.start, range.end - range.start))
            }
            Column::String(col) => Column::String(col.slice(range)),
            Column::Timestamp(col) => Column::Timestamp(col.slice(range)),
            Column::Array(col) => Column::Array(Box::new(col.slice(range))),
            Column::Nullable(col) => Column::Nullable(Box::new(col.slice(range))),
            Column::Tuple { fields, .. } => Column::Tuple {
                fields: fields
                    .iter()
                    .map(|field| field.slice(range.clone()))
                    .collect(),
                len: range.end - range.start,
            },
            Column::Variant(col) => Column::Variant(col.slice(range)),
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
            Column::Number(col) => Domain::Number(col.domain()),
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
            Column::Timestamp(col) => {
                let (min, max) = col.ts.iter().minmax().into_option().unwrap();
                Domain::Timestamp(TimestampDomain {
                    min: *min,
                    max: *max,
                    precision: col.precision,
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
            Column::Variant(_) => Domain::Undefined,
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
            Column::Number(NumberColumn::Int8(_)) => ArrowDataType::Int8,
            Column::Number(NumberColumn::Int16(_)) => ArrowDataType::Int16,
            Column::Number(NumberColumn::Int32(_)) => ArrowDataType::Int32,
            Column::Number(NumberColumn::Int64(_)) => ArrowDataType::Int64,
            Column::Number(NumberColumn::UInt8(_)) => ArrowDataType::UInt8,
            Column::Number(NumberColumn::UInt16(_)) => ArrowDataType::UInt16,
            Column::Number(NumberColumn::UInt32(_)) => ArrowDataType::UInt32,
            Column::Number(NumberColumn::UInt64(_)) => ArrowDataType::UInt64,
            Column::Number(NumberColumn::Float32(_)) => ArrowDataType::Float32,
            Column::Number(NumberColumn::Float64(_)) => ArrowDataType::Float64,
            Column::Boolean(_) => ArrowDataType::Boolean,
            Column::String { .. } => ArrowDataType::LargeBinary,
            Column::Timestamp(TimestampColumn { precision, .. }) => ArrowDataType::Extension(
                "Timestamp".to_owned(),
                Box::new(ArrowDataType::Int64),
                Some(precision.to_string()),
            ),
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
            Column::Variant(_) => {
                ArrowType::Extension("Variant".to_owned(), Box::new(ArrowType::LargeBinary), None)
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
            Column::Number(NumberColumn::UInt8(col)) => {
                Box::new(common_arrow::arrow::array::PrimitiveArray::<u8>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ))
            }
            Column::Number(NumberColumn::UInt16(col)) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<u16>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ),
            ),
            Column::Number(NumberColumn::UInt32(col)) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<u32>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ),
            ),
            Column::Number(NumberColumn::UInt64(col)) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<u64>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ),
            ),
            Column::Number(NumberColumn::Int8(col)) => {
                Box::new(common_arrow::arrow::array::PrimitiveArray::<i8>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ))
            }
            Column::Number(NumberColumn::Int16(col)) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<i16>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ),
            ),
            Column::Number(NumberColumn::Int32(col)) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<i32>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ),
            ),
            Column::Number(NumberColumn::Int64(col)) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<i64>::from_data(
                    self.arrow_type(),
                    col.clone(),
                    None,
                ),
            ),
            Column::Number(NumberColumn::Float32(col)) => {
                let values =
                    unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(col.clone()) };
                Box::new(
                    common_arrow::arrow::array::PrimitiveArray::<f32>::from_data(
                        self.arrow_type(),
                        values,
                        None,
                    ),
                )
            }
            Column::Number(NumberColumn::Float64(col)) => {
                let values =
                    unsafe { std::mem::transmute::<Buffer<F64>, Buffer<f64>>(col.clone()) };
                Box::new(
                    common_arrow::arrow::array::PrimitiveArray::<f64>::from_data(
                        self.arrow_type(),
                        values,
                        None,
                    ),
                )
            }
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
            Column::Timestamp(col) => Box::new(
                common_arrow::arrow::array::PrimitiveArray::<i64>::from_data(
                    self.arrow_type(),
                    col.ts.clone(),
                    None,
                ),
            ),
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
            Column::Variant(col) => {
                Box::new(common_arrow::arrow::array::BinaryArray::<i64>::from_data(
                    self.arrow_type(),
                    col.offsets.iter().map(|offset| *offset as i64).collect(),
                    col.data.clone(),
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
            ArrowDataType::UInt8 => Column::Number(NumberColumn::UInt8(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::UInt8Array>()
                    .expect("fail to read from arrow: array should be `UInt8Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::UInt16 => Column::Number(NumberColumn::UInt16(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::UInt16Array>()
                    .expect("fail to read from arrow: array should be `UInt16Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::UInt32 => Column::Number(NumberColumn::UInt32(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::UInt32Array>()
                    .expect("fail to read from arrow: array should be `UInt32Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::UInt64 => Column::Number(NumberColumn::UInt64(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::UInt64Array>()
                    .expect("fail to read from arrow: array should be `UInt64Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::Int8 => Column::Number(NumberColumn::Int8(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Int8Array>()
                    .expect("fail to read from arrow: array should be `Int8Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::Int16 => Column::Number(NumberColumn::Int16(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Int16Array>()
                    .expect("fail to read from arrow: array should be `Int16Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::Int32 => Column::Number(NumberColumn::Int32(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Int32Array>()
                    .expect("fail to read from arrow: array should be `Int32Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::Int64 => Column::Number(NumberColumn::Int64(
                arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Int64Array>()
                    .expect("fail to read from arrow: array should be `Int64Array`")
                    .values()
                    .clone(),
            )),
            ArrowDataType::Float32 => {
                let col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Float32Array>()
                    .expect("fail to read from arrow: array should be `Float32Array`")
                    .values()
                    .clone();
                let col = unsafe { std::mem::transmute::<Buffer<f32>, Buffer<F32>>(col) };
                Column::Number(NumberColumn::Float32(col))
            }
            ArrowDataType::Float64 => {
                let col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Float64Array>()
                    .expect("fail to read from arrow: array should be `Float64Array`")
                    .values()
                    .clone();
                let col = unsafe { std::mem::transmute::<Buffer<f64>, Buffer<F64>>(col) };
                Column::Number(NumberColumn::Float64(col))
            }
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
            ArrowDataType::Extension(name, _, Some(precision)) if name == "Timestamp" => {
                let ts = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::Int64Array>()
                    .expect("fail to read from arrow: array should be `Int64Array`")
                    .values()
                    .clone();
                let precision = precision.parse().unwrap();
                Column::Timestamp(TimestampColumn { ts, precision })
            }
            ArrowDataType::Extension(name, _, None) if name == "Variant" => {
                let arrow_col = arrow_col
                    .as_any()
                    .downcast_ref::<common_arrow::arrow::array::BinaryArray<i64>>()
                    .expect("fail to read from arrow: array should be `BinaryArray<i64>`");
                let offsets = arrow_col
                    .offsets()
                    .iter()
                    .map(|x| *x as u64)
                    .collect::<Vec<_>>();
                Column::Variant(StringColumn {
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

    pub fn remove_nullable(&self) -> Self {
        match self {
            Column::Nullable(inner) => inner.column.clone(),
            _ => self.clone(),
        }
    }

    pub fn memory_size(&self) -> usize {
        match self {
            Column::Null { .. } => std::mem::size_of::<usize>(),
            Column::EmptyArray { .. } => std::mem::size_of::<usize>(),
            Column::Number(NumberColumn::UInt8(_)) => self.len(),
            Column::Number(NumberColumn::UInt16(_)) => self.len() * 2,
            Column::Number(NumberColumn::UInt32(_)) => self.len() * 4,
            Column::Number(NumberColumn::UInt64(_)) => self.len() * 8,
            Column::Number(NumberColumn::Float32(_)) => self.len() * 4,
            Column::Number(NumberColumn::Float64(_)) => self.len() * 8,
            Column::Number(NumberColumn::Int8(_)) => self.len(),
            Column::Number(NumberColumn::Int16(_)) => self.len() * 2,
            Column::Number(NumberColumn::Int32(_)) => self.len() * 4,
            Column::Number(NumberColumn::Int64(_)) => self.len() * 8,
            Column::Boolean(c) => c.as_slice().0.len(),
            Column::String(col) => col.data.len() + col.offsets.len() * 8,
            Column::Timestamp(col) => col.len() * 8,
            Column::Array(col) => col.values.memory_size() + col.offsets.len() * 8,
            Column::Nullable(c) => c.column.memory_size() + c.validity.as_slice().0.len(),
            Column::Tuple { fields, .. } => fields.iter().map(|f| f.memory_size()).sum(),
            Column::Variant(col) => col.data.len() + col.offsets.len() * 8,
        }
    }
}

impl Serialize for Column {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_bytes(&serialize_arrow_array(self.as_arrow()))
    }
}

impl<'de> Deserialize<'de> for Column {
    fn deserialize<D>(deserializer: D) -> Result<Column, D::Error>
    where D: Deserializer<'de> {
        struct ColumnVisitor;

        impl<'de> Visitor<'de> for ColumnVisitor {
            type Value = Column;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an arrow chunk with exactly one column")
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
            where E: serde::de::Error {
                let array = deserialize_arrow_array(value)
                    .expect("expecting an arrow chunk with exactly one column");
                Ok(Column::from_arrow(&*array))
            }
        }

        deserializer.deserialize_bytes(ColumnVisitor)
    }
}

impl ColumnBuilder {
    pub fn from_column(col: Column) -> Self {
        match col {
            Column::Null { len } => ColumnBuilder::Null { len },
            Column::EmptyArray { len } => ColumnBuilder::EmptyArray { len },
            Column::Number(col) => ColumnBuilder::Number(NumberColumnBuilder::from_column(col)),
            Column::Boolean(col) => ColumnBuilder::Boolean(bitmap_into_mut(col)),
            Column::String(col) => ColumnBuilder::String(StringColumnBuilder::from_column(col)),
            Column::Timestamp(col) => {
                ColumnBuilder::Timestamp(TimestampColumnBuilder::from_column(col))
            }
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
            Column::Variant(col) => ColumnBuilder::Variant(StringColumnBuilder::from_column(col)),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnBuilder::Null { len } => *len,
            ColumnBuilder::EmptyArray { len } => *len,
            ColumnBuilder::Number(col) => col.len(),
            ColumnBuilder::Boolean(builder) => builder.len(),
            ColumnBuilder::String(builder) => builder.len(),
            ColumnBuilder::Timestamp(builder) => builder.len(),
            ColumnBuilder::Array(builder) => builder.len(),
            ColumnBuilder::Nullable(builder) => builder.len(),
            ColumnBuilder::Tuple { len, .. } => *len,
            ColumnBuilder::Variant(builder) => builder.len(),
        }
    }

    pub fn with_capacity(ty: &DataType, capacity: usize) -> ColumnBuilder {
        match ty {
            DataType::Null => ColumnBuilder::Null { len: 0 },
            DataType::EmptyArray => ColumnBuilder::EmptyArray { len: 0 },
            DataType::Number(num_ty) => {
                ColumnBuilder::Number(NumberColumnBuilder::with_capacity(num_ty, capacity))
            }
            DataType::Boolean => ColumnBuilder::Boolean(MutableBitmap::with_capacity(capacity)),
            DataType::String => {
                ColumnBuilder::String(StringColumnBuilder::with_capacity(capacity, 0))
            }
            DataType::Timestamp => {
                ColumnBuilder::Timestamp(TimestampColumnBuilder::with_capacity(capacity))
            }
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
            DataType::Variant => {
                ColumnBuilder::Variant(StringColumnBuilder::with_capacity(capacity, 0))
            }
            DataType::Generic(_) => {
                unreachable!("unable to initialize column builder for generic type")
            }
        }
    }

    pub fn push(&mut self, item: ScalarRef) {
        match (self, item) {
            (ColumnBuilder::Null { len }, ScalarRef::Null) => *len += 1,
            (ColumnBuilder::EmptyArray { len }, ScalarRef::EmptyArray) => *len += 1,
            (ColumnBuilder::Number(builder), ScalarRef::Number(value)) => builder.push(value),
            (ColumnBuilder::Boolean(builder), ScalarRef::Boolean(value)) => builder.push(value),
            (ColumnBuilder::String(builder), ScalarRef::String(value)) => {
                builder.put_slice(value);
                builder.commit_row();
            }
            (ColumnBuilder::Timestamp(builder), ScalarRef::Timestamp(value)) => {
                builder.push(value);
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
            (ColumnBuilder::Variant(builder), ScalarRef::Variant(value)) => {
                builder.put_slice(value);
                builder.commit_row();
            }
            (builder, scalar) => unreachable!("unable to push {scalar:?} to {builder:?}"),
        }
    }

    pub fn push_default(&mut self) {
        match self {
            ColumnBuilder::Null { len } => *len += 1,
            ColumnBuilder::EmptyArray { len } => *len += 1,
            ColumnBuilder::Number(builder) => builder.push_default(),
            ColumnBuilder::Boolean(builder) => builder.push(false),
            ColumnBuilder::String(builder) => builder.commit_row(),
            ColumnBuilder::Timestamp(builder) => builder.push_default(),
            ColumnBuilder::Array(builder) => builder.push_default(),
            ColumnBuilder::Nullable(builder) => builder.push_null(),
            ColumnBuilder::Tuple { fields, len } => {
                for field in fields {
                    field.push_default();
                }
                *len += 1;
            }
            ColumnBuilder::Variant(builder) => {
                builder.put_slice(DEFAULT_JSONB);
                builder.commit_row();
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
            (ColumnBuilder::Number(builder), ColumnBuilder::Number(other_builder)) => {
                builder.append(other_builder);
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
            ColumnBuilder::Number(builder) => Column::Number(builder.build()),
            ColumnBuilder::Boolean(builder) => Column::Boolean(builder.into()),
            ColumnBuilder::String(builder) => Column::String(builder.build()),
            ColumnBuilder::Timestamp(builder) => Column::Timestamp(builder.build()),
            ColumnBuilder::Array(builder) => Column::Array(Box::new(builder.build())),
            ColumnBuilder::Nullable(builder) => Column::Nullable(Box::new(builder.build())),
            ColumnBuilder::Tuple { fields, len } => Column::Tuple {
                fields: fields.into_iter().map(|field| field.build()).collect(),
                len,
            },
            ColumnBuilder::Variant(builder) => Column::Variant(builder.build()),
        }
    }

    pub fn build_scalar(self) -> Scalar {
        assert_eq!(self.len(), 1);
        match self {
            ColumnBuilder::Null { .. } => Scalar::Null,
            ColumnBuilder::EmptyArray { .. } => Scalar::EmptyArray,
            ColumnBuilder::Number(builder) => Scalar::Number(builder.build_scalar()),
            ColumnBuilder::Boolean(builder) => Scalar::Boolean(builder.get(0)),
            ColumnBuilder::String(builder) => Scalar::String(builder.build_scalar()),
            ColumnBuilder::Timestamp(builder) => Scalar::Timestamp(builder.build_scalar()),
            ColumnBuilder::Array(builder) => Scalar::Array(builder.build_scalar()),
            ColumnBuilder::Nullable(builder) => builder.build_scalar().unwrap_or(Scalar::Null),
            ColumnBuilder::Tuple { fields, .. } => Scalar::Tuple(
                fields
                    .into_iter()
                    .map(|field| field.build_scalar())
                    .collect(),
            ),
            ColumnBuilder::Variant(builder) => Scalar::Variant(builder.build_scalar()),
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
