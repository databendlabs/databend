// Copyright 2020 Datafuse Labs.
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

use common_arrow::arrow::array::*;

use crate::prelude::*;
use crate::utils::get_iter_capacity;
use crate::utils::NoNull;

pub struct PrimitiveArrayBuilder<T>
where T: DFPrimitiveType
{
    builder: MutablePrimitiveArray<T>,
}

pub type DFUInt8ArrayBuilder = PrimitiveArrayBuilder<u8>;
pub type DFInt8ArrayBuilder = PrimitiveArrayBuilder<i8>;
pub type DFUInt16ArrayBuilder = PrimitiveArrayBuilder<u16>;
pub type DFInt16ArrayBuilder = PrimitiveArrayBuilder<i16>;
pub type DFUInt32ArrayBuilder = PrimitiveArrayBuilder<u32>;
pub type DFInt32ArrayBuilder = PrimitiveArrayBuilder<i32>;
pub type DFUInt64ArrayBuilder = PrimitiveArrayBuilder<u64>;
pub type DFInt64ArrayBuilder = PrimitiveArrayBuilder<i64>;
pub type DFFloat32ArrayBuilder = PrimitiveArrayBuilder<f32>;
pub type DFFloat64ArrayBuilder = PrimitiveArrayBuilder<f64>;

impl<T> ArrayBuilder<T, DFPrimitiveArray<T>> for PrimitiveArrayBuilder<T>
where T: DFPrimitiveType
{
    /// Appends a value of type `T` into the builder
    #[inline]
    fn append_value(&mut self, v: T) {
        self.builder.push(Some(v))
    }

    /// Appends a null slot into the builder
    #[inline]
    fn append_null(&mut self) {
        self.builder.push_null();
    }

    fn finish(&mut self) -> DFPrimitiveArray<T> {
        let array = self.builder.as_arc();
        DFPrimitiveArray::<T>::from_arrow_array(array.as_ref())
    }
}

impl<T> PrimitiveArrayBuilder<T>
where T: DFPrimitiveType
{
    pub fn with_capacity(capacity: usize) -> Self {
        PrimitiveArrayBuilder {
            builder: MutablePrimitiveArray::<T>::with_capacity(capacity),
        }
    }
}

impl<T> NewDataArray<T> for DFPrimitiveArray<T>
where T: DFPrimitiveType
{
    fn new_from_slice(v: &[T]) -> Self {
        Self::new_from_iter(v.iter().copied())
    }

    fn new_from_opt_slice(opt_v: &[Option<T>]) -> Self {
        Self::new_from_opt_iter(opt_v.iter().copied())
    }

    fn new_from_opt_iter(it: impl Iterator<Item = Option<T>>) -> DFPrimitiveArray<T> {
        let mut builder = PrimitiveArrayBuilder::with_capacity(get_iter_capacity(&it));
        it.for_each(|opt| builder.append_option(opt));
        builder.finish()
    }

    /// Create a new DataArray from an iterator.
    fn new_from_iter(it: impl Iterator<Item = T>) -> DFPrimitiveArray<T> {
        let ca: NoNull<DFPrimitiveArray<_>> = it.collect();
        ca.into_inner()
    }
}
