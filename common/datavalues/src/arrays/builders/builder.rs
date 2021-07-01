// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::BooleanBuilder;
use common_arrow::arrow::array::ListBuilder;
use common_arrow::arrow::array::PrimitiveBuilder;
use common_arrow::arrow::array::StringBuilder;
use common_arrow::arrow::buffer::Buffer;
use num::Num;

use super::ArrowBooleanArrayBuilder;
use super::ArrowPrimitiveArrayBuilder;
use crate::arrays::DataArray;
use crate::arrays::GetValues;
use crate::data_df_type::*;
use crate::prelude::*;
use crate::utils::get_iter_capacity;
use crate::utils::NoNull;
use crate::BooleanType;
use crate::DFBooleanArray;
use crate::DFListArray;
use crate::DFNumericType;
use crate::DFPrimitiveType;
use crate::DFUtf8Array;
use crate::Utf8Type;

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

pub struct BooleanArrayBuilder {
    builder: BooleanBuilder,
}

impl ArrayBuilder<bool, BooleanType> for BooleanArrayBuilder {
    /// Appends a value of type `T` into the builder
    #[inline]
    fn append_value(&mut self, v: bool) {
        self.builder.append_value(v).unwrap();
    }

    /// Appends a null slot into the builder
    #[inline]
    fn append_null(&mut self) {
        self.builder.append_null().unwrap();
    }

    fn finish(&mut self) -> DFBooleanArray {
        let array = Arc::new(self.builder.finish()) as ArrayRef;
        array.into()
    }
}

impl BooleanArrayBuilder {
    pub fn new(capacity: usize) -> Self {
        BooleanArrayBuilder {
            builder: BooleanBuilder::new(capacity),
        }
    }
}

pub struct PrimitiveArrayBuilder<T>
where
    T: DFNumericType,
    T::Native: Default,
{
    builder: PrimitiveBuilder<T>,
}

impl<T> ArrayBuilder<T::Native, T> for PrimitiveArrayBuilder<T>
where
    T: DFNumericType,
    T::Native: Default,
{
    /// Appends a value of type `T` into the builder
    #[inline]
    fn append_value(&mut self, v: T::Native) {
        self.builder.append_value(v).unwrap();
    }

    /// Appends a null slot into the builder
    #[inline]
    fn append_null(&mut self) {
        self.builder.append_null().unwrap();
    }

    fn finish(&mut self) -> DataArray<T> {
        let array = Arc::new(self.builder.finish()) as ArrayRef;

        array.into()
    }
}

impl<T> PrimitiveArrayBuilder<T>
where T: DFNumericType
{
    pub fn new(capacity: usize) -> Self {
        PrimitiveArrayBuilder {
            builder: PrimitiveBuilder::<T>::new(capacity),
        }
    }
}

pub struct Utf8ArrayBuilder {
    pub builder: StringBuilder,
    pub capacity: usize,
}

impl Utf8ArrayBuilder {
    /// Create a new UtfArrayBuilder
    ///
    /// # Arguments
    ///
    /// * `capacity` - Number of string elements in the final array.
    /// * `bytes_capacity` - Number of bytes needed to store the string values.
    pub fn new(capacity: usize, bytes_capacity: usize) -> Self {
        Utf8ArrayBuilder {
            builder: StringBuilder::with_capacity(bytes_capacity, capacity),
            capacity,
        }
    }

    /// Appends a value of type `T` into the builder
    #[inline]
    pub fn append_value<S: AsRef<str>>(&mut self, v: S) {
        self.builder.append_value(v.as_ref()).unwrap();
    }

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_null(&mut self) {
        self.builder.append_null().unwrap();
    }

    #[inline]
    pub fn append_option<S: AsRef<str>>(&mut self, opt: Option<S>) {
        match opt {
            Some(s) => self.append_value(s.as_ref()),
            None => self.append_null(),
        }
    }

    pub fn finish(&mut self) -> DFUtf8Array {
        let array = Arc::new(self.builder.finish()) as ArrayRef;
        array.into()
    }
}

/// Get the null count and the null bitmap of the arrow array
pub fn get_bitmap<T: Array + ?Sized>(arr: &T) -> (usize, Option<Buffer>) {
    let data = arr.data();
    (
        data.null_count(),
        data.null_bitmap().as_ref().map(|bitmap| {
            let buff = bitmap.buffer_ref();
            buff.clone()
        }),
    )
}

pub trait NewDataArray<T, N> {
    fn new_from_slice(v: &[N]) -> Self;
    fn new_from_opt_slice(opt_v: &[Option<N>]) -> Self;

    /// Create a new DataArray from an iterator.
    fn new_from_opt_iter(it: impl Iterator<Item = Option<N>>) -> Self;

    /// Create a new DataArray from an iterator.
    fn new_from_iter(it: impl Iterator<Item = N>) -> Self;
}

impl<T> NewDataArray<T, T::Native> for DataArray<T>
where T: DFNumericType
{
    fn new_from_slice(v: &[T::Native]) -> Self {
        Self::new_from_iter(v.iter().copied())
    }

    fn new_from_opt_slice(opt_v: &[Option<T::Native>]) -> Self {
        Self::new_from_opt_iter(opt_v.iter().copied())
    }

    fn new_from_opt_iter(it: impl Iterator<Item = Option<T::Native>>) -> DataArray<T> {
        let mut builder = PrimitiveArrayBuilder::new(get_iter_capacity(&it));
        it.for_each(|opt| builder.append_option(opt));
        builder.finish()
    }

    /// Create a new DataArray from an iterator.
    fn new_from_iter(it: impl Iterator<Item = T::Native>) -> DataArray<T> {
        let ca: NoNull<DataArray<_>> = it.collect();
        ca.into_inner()
    }
}

impl NewDataArray<BooleanType, bool> for DFBooleanArray {
    fn new_from_slice(v: &[bool]) -> Self {
        Self::new_from_iter(v.iter().copied())
    }

    fn new_from_opt_slice(opt_v: &[Option<bool>]) -> Self {
        Self::new_from_opt_iter(opt_v.iter().copied())
    }

    fn new_from_opt_iter(it: impl Iterator<Item = Option<bool>>) -> DFBooleanArray {
        let mut builder = BooleanArrayBuilder::new(get_iter_capacity(&it));
        it.for_each(|opt| builder.append_option(opt));
        builder.finish()
    }

    /// Create a new DataArray from an iterator.
    fn new_from_iter(it: impl Iterator<Item = bool>) -> DFBooleanArray {
        it.collect()
    }
}

impl<S> NewDataArray<Utf8Type, S> for DFUtf8Array
where S: AsRef<str>
{
    fn new_from_slice(v: &[S]) -> Self {
        let values_size = v.iter().fold(0, |acc, s| acc + s.as_ref().len());

        let mut builder = StringBuilder::with_capacity(values_size, v.len());
        v.iter().for_each(|val| {
            builder.append_value(val.as_ref()).unwrap();
        });

        let array = Arc::new(builder.finish()) as ArrayRef;
        array.into()
    }

    fn new_from_opt_slice(opt_v: &[Option<S>]) -> Self {
        let values_size = opt_v.iter().fold(0, |acc, s| match s {
            Some(s) => acc + s.as_ref().len(),
            None => acc,
        });
        let mut builder = Utf8ArrayBuilder::new(values_size, opt_v.len());

        opt_v.iter().for_each(|opt| match opt {
            Some(v) => builder.append_value(v.as_ref()),
            None => builder.append_null(),
        });
        builder.finish()
    }

    fn new_from_opt_iter(it: impl Iterator<Item = Option<S>>) -> Self {
        let cap = get_iter_capacity(&it);
        let mut builder = Utf8ArrayBuilder::new(cap, cap * 5);
        it.for_each(|opt| builder.append_option(opt));
        builder.finish()
    }

    /// Create a new DataArray from an iterator.
    fn new_from_iter(it: impl Iterator<Item = S>) -> Self {
        let cap = get_iter_capacity(&it);
        let mut builder = Utf8ArrayBuilder::new(cap, cap * 5);
        it.for_each(|v| builder.append_value(v));
        builder.finish()
    }
}

pub trait ListBuilderTrait {
    fn append_opt_series(&mut self, opt_s: Option<&Series>);
    fn append_series(&mut self, s: &Series);
    fn append_null(&mut self);
    fn finish(&mut self) -> DFListArray;
}

pub struct ListPrimitiveArrayBuilder<T>
where T: DFPrimitiveType
{
    pub builder: ListBuilder<ArrowPrimitiveArrayBuilder<T>>,
}

macro_rules! finish_list_builder {
    ($self:ident) => {{
        let arr = Arc::new($self.builder.finish());
        DFListArray::from(arr as ArrayRef)
    }};
}

impl<T> ListPrimitiveArrayBuilder<T>
where T: DFPrimitiveType
{
    pub fn new(values_builder: ArrowPrimitiveArrayBuilder<T>, capacity: usize) -> Self {
        let builder = ListBuilder::with_capacity(values_builder, capacity);
        ListPrimitiveArrayBuilder { builder }
    }

    pub fn append_slice(&mut self, opt_v: Option<&[T::Native]>) {
        match opt_v {
            Some(v) => {
                self.builder.values().append_slice(v);
                self.builder.append(true).expect("should not fail");
            }
            None => {
                self.builder.append(false).expect("should not fail");
            }
        }
    }

    pub fn append_null(&mut self) {
        self.builder.append(false).expect("should not fail");
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
                self.builder.append(false).unwrap();
            }
        }
    }

    #[inline]
    fn append_null(&mut self) {
        let builder = self.builder.values();
        builder.append_null();
        self.builder.append(true).unwrap();
    }

    #[inline]
    fn append_series(&mut self, s: &Series) {
        let builder = self.builder.values();
        let array = s.get_array_ref();
        let values = array.get_values::<T>();
        // we would like to check if array has no null values.
        // however at the time of writing there is a bug in append_slice, because it does not update
        // the null bitmap
        if s.null_count() == 0 {
            builder.append_slice(values);
        } else {
            values.iter().enumerate().for_each(|(idx, v)| {
                if array.is_valid(idx) {
                    builder.append_value(*v);
                } else {
                    builder.append_null();
                }
            });
        }
        self.builder.append(true).unwrap();
    }

    fn finish(&mut self) -> DFListArray {
        finish_list_builder!(self)
    }
}

pub struct ListUtf8ArrayBuilder {
    builder: ListBuilder<StringBuilder>,
}

impl ListUtf8ArrayBuilder {
    pub fn new(values_builder: StringBuilder, capacity: usize) -> Self {
        let builder = ListBuilder::with_capacity(values_builder, capacity);
        ListUtf8ArrayBuilder { builder }
    }
}

impl ListBuilderTrait for ListUtf8ArrayBuilder {
    fn append_opt_series(&mut self, opt_s: Option<&Series>) {
        match opt_s {
            Some(s) => self.append_series(s),
            None => {
                self.builder.append(false).unwrap();
            }
        }
    }

    #[inline]
    fn append_null(&mut self) {
        let builder = self.builder.values();
        builder.append_null().unwrap();
        self.builder.append(true).unwrap();
    }

    #[inline]
    fn append_series(&mut self, s: &Series) {
        let ca = s.utf8().unwrap();
        let value_builder = self.builder.values();
        for s in ca {
            match s {
                Some(s) => value_builder.append_value(s).unwrap(),
                None => value_builder.append_null().unwrap(),
            };
        }
        self.builder.append(true).unwrap();
    }

    fn finish(&mut self) -> DFListArray {
        finish_list_builder!(self)
    }
}

pub struct ListBooleanArrayBuilder {
    builder: ListBuilder<ArrowBooleanArrayBuilder>,
}

impl ListBooleanArrayBuilder {
    pub fn new(values_builder: ArrowBooleanArrayBuilder, capacity: usize) -> Self {
        let builder = ListBuilder::with_capacity(values_builder, capacity);

        Self { builder }
    }
}

impl ListBuilderTrait for ListBooleanArrayBuilder {
    fn append_opt_series(&mut self, opt_s: Option<&Series>) {
        match opt_s {
            Some(s) => self.append_series(s),
            None => {
                self.builder.append(false).unwrap();
            }
        }
    }

    #[inline]
    fn append_null(&mut self) {
        let builder = self.builder.values();
        builder.append_null();
        self.builder.append(true).unwrap();
    }

    #[inline]
    fn append_series(&mut self, s: &Series) {
        let ca = s.bool().unwrap();
        let value_builder = self.builder.values();
        for s in ca {
            match s {
                Some(s) => value_builder.append_value(s),
                None => value_builder.append_null(),
            };
        }
        self.builder.append(true).unwrap();
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
            let values_builder = ArrowPrimitiveArrayBuilder::<$type>::new(value_capacity);
            let builder = ListPrimitiveArrayBuilder::new(values_builder, list_capacity);
            Box::new(builder)
        }};
    }
    macro_rules! get_bool_builder {
        () => {{
            let values_builder = ArrowBooleanArrayBuilder::new(value_capacity);
            let builder = ListBooleanArrayBuilder::new(values_builder, list_capacity);
            Box::new(builder)
        }};
    }
    macro_rules! get_utf8_builder {
        () => {{
            let values_builder = StringBuilder::with_capacity(value_capacity * 5, value_capacity);
            let builder = ListUtf8ArrayBuilder::new(values_builder, list_capacity);
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
