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
use common_exception::Result;
use num::Num;

use crate::arrays::DataArray;
use crate::prelude::*;

pub trait ArrayBuilder<N, T> {
    fn append_value(&mut self, val: N);
    fn append_null(&mut self);
    fn append_option(&mut self, opt_val: Option<N>) {
        match opt_val {
            Some(v) => self.append_value(v),
            None => self.append_null(),
        }
    }
    fn finish(&mut self) -> DataArray<T>;
}

pub trait ArrayDeserializer {
    fn de(&mut self, reader: &mut &[u8]) -> Result<()>;
    fn de_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()>;
    /// If error occurrs, append a null by default
    fn de_text(&mut self, reader: &[u8]);
    fn de_null(&mut self);
    fn finish_to_series(&mut self) -> Series;
}

pub type DFUInt8ArrayBuilder = PrimitiveArrayBuilder<UInt8Type>;
pub type DFInt8ArrayBuilder = PrimitiveArrayBuilder<Int8Type>;
pub type DFUInt16ArrayBuilder = PrimitiveArrayBuilder<UInt16Type>;
pub type DFInt16ArrayBuilder = PrimitiveArrayBuilder<Int16Type>;
pub type DFUInt32ArrayBuilder = PrimitiveArrayBuilder<UInt32Type>;
pub type DFInt32ArrayBuilder = PrimitiveArrayBuilder<Int32Type>;
pub type DFUInt64ArrayBuilder = PrimitiveArrayBuilder<UInt64Type>;
pub type DFInt64ArrayBuilder = PrimitiveArrayBuilder<Int64Type>;

pub trait NewDataArray<T, N> {
    fn new_from_slice(v: &[N]) -> Self;
    fn new_from_opt_slice(opt_v: &[Option<N>]) -> Self;

    /// Create a new DataArray from an iterator.
    fn new_from_opt_iter(it: impl Iterator<Item = Option<N>>) -> Self;

    /// Create a new DataArray from an iterator.
    fn new_from_iter(it: impl Iterator<Item = N>) -> Self;
}

pub trait ListBuilderTrait {
    fn append_opt_series(&mut self, opt_s: Option<&Series>);
    fn append_series(&mut self, s: &Series);
    fn append_null(&mut self);
    fn finish(&mut self) -> DFListArray;
}

type LargePrimitiveBuilder<T> = MutableListArray<i64, MutablePrimitiveArray<T>>;
type LargeListUtf8Builder = MutableListArray<i64, MutableUtf8Array<i64>>;
type LargeListBooleanBuilder = MutableListArray<i64, MutableBooleanArray>;

pub struct ListPrimitiveArrayBuilder<T>
where T: DFPrimitiveType
{
    pub builder: LargePrimitiveBuilder<T::Native>,
}

macro_rules! finish_list_builder {
    ($self:ident) => {{
        let arr = $self.builder.as_arc();
        DFListArray::from(arr as ArrayRef)
    }};
}

impl<T> ListPrimitiveArrayBuilder<T>
where T: DFPrimitiveType
{
    pub fn with_capacity(values_capacity: usize, capacity: usize) -> Self {
        let values = MutablePrimitiveArray::<T::Native>::with_capacity(values_capacity);
        let builder = LargePrimitiveBuilder::<T::Native>::new_with_capacity(values, capacity);

        ListPrimitiveArrayBuilder { builder }
    }

    pub fn append_slice(&mut self, opt_v: Option<&[T::Native]>) {
        match opt_v {
            Some(items) => {
                let values = self.builder.mut_values();
                // Safety:
                // A slice is a trusted length iterator
                unsafe { values.extend_trusted_len_unchecked(items.iter().map(Some)) }
                self.builder.try_push_valid().unwrap();
            }
            None => {
                self.builder.push_null();
            }
        }
    }
}

impl<T> ListBuilderTrait for ListPrimitiveArrayBuilder<T>
where
    T: DFPrimitiveType,
    T::Native: Num,
{
    #[inline]
    fn append_opt_series(&mut self, opt_s: Option<&Series>) {
        match opt_s {
            Some(s) => self.append_series(s),
            None => {
                self.builder.push_null();
            }
        }
    }

    #[inline]
    fn append_null(&mut self) {
        self.builder.push_null();
    }

    #[inline]
    fn append_series(&mut self, s: &Series) {
        let array = s.get_array_ref();
        let arr = array
            .as_any()
            .downcast_ref::<PrimitiveArray<T::Native>>()
            .unwrap();

        let values = self.builder.mut_values();
        unsafe { values.extend_trusted_len_unchecked(arr.into_iter()) }
        self.builder.try_push_valid().unwrap();
    }

    fn finish(&mut self) -> DFListArray {
        finish_list_builder!(self)
    }
}

pub struct ListUtf8ArrayBuilder {
    builder: LargeListUtf8Builder,
}

type LargeMutableUtf8Array = MutableUtf8Array<i64>;
impl ListUtf8ArrayBuilder {
    pub fn with_capacity(values_capacity: usize, capacity: usize) -> Self {
        let values = LargeMutableUtf8Array::with_capacity(values_capacity);
        let builder = LargeListUtf8Builder::new_with_capacity(values, capacity);

        ListUtf8ArrayBuilder { builder }
    }
}

impl ListBuilderTrait for ListUtf8ArrayBuilder {
    fn append_opt_series(&mut self, opt_s: Option<&Series>) {
        match opt_s {
            Some(s) => self.append_series(s),
            None => {
                self.builder.push_null();
            }
        }
    }

    #[inline]
    fn append_null(&mut self) {
        self.builder.push_null();
    }

    #[inline]
    fn append_series(&mut self, s: &Series) {
        let ca = s.utf8().unwrap();
        let value_builder = self.builder.mut_values();
        value_builder.try_extend(ca).unwrap();
        self.builder.try_push_valid().unwrap();
    }

    fn finish(&mut self) -> DFListArray {
        finish_list_builder!(self)
    }
}

pub struct ListBooleanArrayBuilder {
    builder: LargeListBooleanBuilder,
}

impl ListBooleanArrayBuilder {
    pub fn with_capacity(values_capacity: usize, capacity: usize) -> Self {
        let values = MutableBooleanArray::with_capacity(values_capacity);
        let builder = LargeListBooleanBuilder::new_with_capacity(values, capacity);
        Self { builder }
    }
}

impl ListBuilderTrait for ListBooleanArrayBuilder {
    fn append_opt_series(&mut self, opt_s: Option<&Series>) {
        match opt_s {
            Some(s) => self.append_series(s),
            None => {
                self.builder.push_null();
            }
        }
    }

    #[inline]
    fn append_null(&mut self) {
        self.builder.push_null();
    }

    #[inline]
    fn append_series(&mut self, s: &Series) {
        let ca = s.bool().unwrap();
        let value_builder = self.builder.mut_values();
        value_builder.try_extend(ca).unwrap();
        self.builder.try_push_valid().unwrap();
    }

    fn finish(&mut self) -> DFListArray {
        finish_list_builder!(self)
    }
}

pub fn get_list_builder(
    dt: &DataType,
    value_capacity: usize,
    list_capacity: usize,
) -> Box<dyn ListBuilderTrait> {
    macro_rules! get_primitive_builder {
        ($type:ty) => {{
            let builder =
                ListPrimitiveArrayBuilder::<$type>::with_capacity(value_capacity, list_capacity);
            Box::new(builder)
        }};
    }
    macro_rules! get_bool_builder {
        () => {{
            let builder = ListBooleanArrayBuilder::with_capacity(value_capacity, list_capacity);
            Box::new(builder)
        }};
    }
    macro_rules! get_utf8_builder {
        () => {{
            let builder = ListUtf8ArrayBuilder::with_capacity(value_capacity, list_capacity);
            Box::new(builder)
        }};
    }
    match_data_type_apply_macro!(
        dt,
        get_primitive_builder,
        get_utf8_builder,
        get_bool_builder
    )
}

pub struct StringArrayBuilder {
    builder: MutableBinaryArray<i64>,
}

impl StringArrayBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: MutableBinaryArray::<i64>::with_capacity(capacity),
        }
    }

    pub fn append_value(&mut self, value: impl AsRef<[u8]>) {
        self.builder.push(Some(value))
    }

    #[inline]
    pub fn append_null(&mut self) {
        self.builder.push_null();
    }

    pub fn finish(&mut self) -> DataArray<StringType> {
        let array = self.builder.as_arc();
        DFStringArray::from(array)
    }
}
