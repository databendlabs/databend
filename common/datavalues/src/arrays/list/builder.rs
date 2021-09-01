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
use num::Num;

use crate::prelude::*;

pub trait ListBuilderTrait {
    fn append_opt_series(&mut self, opt_s: Option<&Series>);
    fn append_series(&mut self, s: &Series);
    fn append_null(&mut self);
    fn finish(&mut self) -> DFListArray;
}

type LargeListPrimitiveBuilder<T> = MutableListArray<i64, MutablePrimitiveArray<T>>;
type LargeListUtf8Builder = MutableListArray<i64, MutableUtf8Array<i64>>;
type LargeListBooleanBuilder = MutableListArray<i64, MutableBooleanArray>;

pub struct ListPrimitiveArrayBuilder<T>
where T: DFPrimitiveType
{
    pub builder: LargeListPrimitiveBuilder<T>,
}

macro_rules! finish_list_builder {
    ($self:ident) => {{
        let arr = $self.builder.as_arc();
        DFListArray::from_arrow_array(arr.as_ref())
    }};
}

impl<T> ListPrimitiveArrayBuilder<T>
where T: DFPrimitiveType
{
    pub fn with_capacity(values_capacity: usize, capacity: usize) -> Self {
        let values = MutablePrimitiveArray::<T>::with_capacity(values_capacity);
        let builder = LargeListPrimitiveBuilder::<T>::new_with_capacity(values, capacity);

        ListPrimitiveArrayBuilder { builder }
    }

    pub fn append_slice(&mut self, opt_v: Option<&[T]>) {
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
    T: Num,
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
        let arr = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

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
