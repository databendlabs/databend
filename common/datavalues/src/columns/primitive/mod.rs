// Copyright 2021 Datafuse Labs.
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

mod iterator;
mod mutable;

use std::iter::Copied;
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::compute::arity::unary;
use common_arrow::arrow::compute::cast;
use common_arrow::arrow::compute::cast::CastOptions;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::datatypes::TimeUnit;
pub use iterator::*;
pub use mutable::*;

use crate::prelude::*;

/// PrimitiveColumn is generic struct which wrapped arrow's PrimitiveArray
#[derive(Clone)]
pub struct PrimitiveColumn<T: PrimitiveType> {
    values: Buffer<T>,
}

impl<T: PrimitiveType> From<PrimitiveArray<T>> for PrimitiveColumn<T> {
    fn from(array: PrimitiveArray<T>) -> Self {
        Self::new(array)
    }
}

fn precision(x: &TimeUnit) -> usize {
    match x {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

impl<T: PrimitiveType> PrimitiveColumn<T> {
    pub fn new(array: PrimitiveArray<T>) -> Self {
        Self {
            values: array.values().clone(),
        }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        let expected_type = create_primitive_datatype::<T>();
        let expected_arrow = expected_type.arrow_type();
        let cast_options = CastOptions {
            wrapped: true,
            partial: true,
        };

        if &expected_arrow != array.data_type() {
            match array.data_type() {
                // u32
                ArrowDataType::Timestamp(x, _) => {
                    let p = precision(x);
                    let array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .expect("primitive cast should be ok");
                    let array = unary(array, |x| (x as usize / p) as u32, expected_arrow);

                    Self::from_arrow_array(&array)
                }
                ArrowDataType::Date32 => {
                    let array = cast::cast(array, &ArrowDataType::Int32, cast_options)
                        .expect("primitive cast should be ok");
                    let array = cast::cast(array.as_ref(), &expected_arrow, cast_options)
                        .expect("primitive cast should be ok");
                    Self::from_arrow_array(array.as_ref())
                }
                ArrowDataType::Date64 => {
                    let array = cast::cast(array, &ArrowDataType::Int64, cast_options)
                        .expect("primitive cast should be ok");
                    let array = cast::cast(array.as_ref(), &expected_arrow, cast_options)
                        .expect("primitive cast should be ok");

                    Self::from_arrow_array(array.as_ref())
                }
                ArrowDataType::Time32(x) => {
                    let p = precision(x);
                    let array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i32>>()
                        .expect("primitive cast should be ok");

                    let array = unary(array, |x| (x as usize / p) as u32, expected_arrow);

                    Self::from_arrow_array(&array)
                }
                ArrowDataType::Time64(x) => {
                    let p = precision(x);
                    let array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .expect("primitive cast should be ok");

                    let array = unary(array, |x| (x as usize / p) as u32, expected_arrow);
                    Self::from_arrow_array(&array)
                }
                _ => unreachable!(),
            }
        } else {
            let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
            Self::new(array.clone())
        }
    }

    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> T {
        // soundness: the invariant of the function
        *self.values.get_unchecked(i)
    }

    pub fn values(&self) -> &[T] {
        self.values.as_slice()
    }

    /// Create a new DataArray by taking ownership of the Vec. This operation is zero copy.
    pub fn new_from_vec(values: Vec<T>) -> Self {
        Self {
            values: values.into(),
        }
    }
}

impl<T: PrimitiveType> Column for PrimitiveColumn<T> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataTypePtr {
        create_primitive_datatype::<T>()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn validity(&self) -> (bool, Option<&Bitmap>) {
        (false, None)
    }

    fn memory_size(&self) -> usize {
        self.values.len() * std::mem::size_of::<T>()
    }

    fn as_arrow_array(&self) -> common_arrow::arrow::array::ArrayRef {
        let data_type = self.data_type().arrow_type();
        Arc::new(PrimitiveArray::<T>::from_data(
            data_type,
            self.values.clone(),
            None,
        ))
    }

    fn arc(&self) -> ColumnRef {
        Arc::new(self.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> ColumnRef {
        let values = self.values.clone().slice(offset, length);
        Arc::new(Self { values })
    }

    fn filter(&self, filter: &BooleanColumn) -> ColumnRef {
        let length = filter.values().len() - filter.values().null_count();
        if length == self.len() {
            return Arc::new(self.clone());
        }
        let iter = self
            .values()
            .iter()
            .zip(filter.values().iter())
            .filter(|(_, f)| *f)
            .map(|(v, _)| *v);

        let values: Vec<T> = iter.collect();
        let col = PrimitiveColumn {
            values: values.into(),
        };

        Arc::new(col)
    }

    fn scatter(&self, indices: &[usize], scattered_size: usize) -> Vec<ColumnRef> {
        let mut builders = Vec::with_capacity(scattered_size);
        for _i in 0..scattered_size {
            builders.push(MutablePrimitiveColumn::<T>::with_capacity(self.len()));
        }

        indices
            .iter()
            .zip(self.values())
            .for_each(|(index, value)| {
                builders[*index].append_value(*value);
            });

        builders.iter_mut().map(|b| b.to_column()).collect()
    }

    fn replicate(&self, offsets: &[usize]) -> ColumnRef {
        debug_assert!(
            offsets.len() == self.len(),
            "Size of offsets must match size of column"
        );

        if offsets.is_empty() {
            return self.slice(0, 0);
        }

        let mut builder =
            MutablePrimitiveColumn::<T>::with_capacity(*offsets.last().unwrap() as usize);

        let mut previous_offset: usize = 0;

        (0..self.len()).for_each(|i| {
            let offset: usize = offsets[i];
            let data = unsafe { self.value_unchecked(i) };
            builder
                .values
                .extend(std::iter::repeat(data).take(offset - previous_offset));
            previous_offset = offset;
        });
        builder.to_column()
    }

    fn convert_full_column(&self) -> ColumnRef {
        Arc::new(self.clone())
    }

    /// Note this doesn't do any bound checking, for performance reason.
    fn get(&self, index: usize) -> DataValue {
        let v = unsafe { self.value_unchecked(index) };
        v.into()
    }
}

impl<T> ScalarColumn for PrimitiveColumn<T>
where
    T: Scalar<ColumnType = Self> + PrimitiveType,
    for<'a> T: ScalarRef<'a, ScalarType = T, ColumnType = Self>,
    for<'a> T: Scalar<RefType<'a> = T>,
{
    type Builder = MutablePrimitiveColumn<T>;
    type OwnedItem = T;
    type RefItem<'a> = T;
    type Iterator<'a> = Copied<std::slice::Iter<'a, T>>;

    #[inline]
    fn get_data(&self, idx: usize) -> Self::RefItem<'_> {
        self.values[idx]
    }

    fn scalar_iter(&self) -> Self::Iterator<'_> {
        self.iter().copied()
    }

    fn from_slice(data: &[Self::RefItem<'_>]) -> Self {
        let values = Vec::<T>::from(data);
        PrimitiveColumn {
            values: values.into(),
        }
    }

    fn from_iterator<'a>(it: impl Iterator<Item = Self::RefItem<'a>>) -> Self {
        let values: Vec<T> = it.collect();
        PrimitiveColumn {
            values: values.into(),
        }
    }

    fn from_owned_iterator(it: impl Iterator<Item = Self::OwnedItem>) -> Self {
        let values: Vec<T> = it.collect();
        PrimitiveColumn {
            values: values.into(),
        }
    }

    fn from_vecs(values: Vec<Self::OwnedItem>) -> Self {
        PrimitiveColumn {
            values: values.into(),
        }
    }
}

pub type UInt8Column = PrimitiveColumn<u8>;
pub type UInt16Column = PrimitiveColumn<u16>;
pub type UInt32Column = PrimitiveColumn<u32>;
pub type UInt64Column = PrimitiveColumn<u64>;

pub type Int8Column = PrimitiveColumn<i8>;
pub type Int16Column = PrimitiveColumn<i16>;
pub type Int32Column = PrimitiveColumn<i32>;
pub type Int64Column = PrimitiveColumn<i64>;

pub type Float32Column = PrimitiveColumn<f32>;
pub type Float64Column = PrimitiveColumn<f64>;

impl<T: PrimitiveType> std::fmt::Debug for PrimitiveColumn<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let iter = self.iter();
        let head = "PrimitiveColumn";
        display_fmt(iter, head, self.len(), self.data_type_id(), f)
    }
}
