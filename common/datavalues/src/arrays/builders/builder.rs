// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::*;
use num::Num;

use crate::arrays::DataArray;
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
    builder: MutableBooleanArray,
}

impl ArrayBuilder<bool, BooleanType> for BooleanArrayBuilder {
    /// Appends a value of type `T` into the builder
    #[inline]
    fn append_value(&mut self, v: bool) {
        self.builder.push(Some(v))
    }

    /// Appends a null slot into the builder
    #[inline]
    fn append_null(&mut self) {
        self.builder.push_null();
    }

    fn finish(&mut self) -> DFBooleanArray {
        let array = self.builder.as_arc();
        array.into()
    }
}

impl BooleanArrayBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        BooleanArrayBuilder {
            builder: MutableBooleanArray::with_capacity(capacity),
        }
    }
}

pub struct PrimitiveArrayBuilder<T>
where
    T: DFNumericType,
    T::Native: Default,
{
    builder: MutablePrimitiveArray<T::Native>,
}

impl<T> ArrayBuilder<T::Native, T> for PrimitiveArrayBuilder<T>
where
    T: DFNumericType,
    T::Native: Default,
{
    /// Appends a value of type `T` into the builder
    #[inline]
    fn append_value(&mut self, v: T::Native) {
        self.builder.push(Some(v))
    }

    /// Appends a null slot into the builder
    #[inline]
    fn append_null(&mut self) {
        self.builder.push_null();
    }

    fn finish(&mut self) -> DataArray<T> {
        let array = self.builder.as_arc();

        array.into()
    }
}

impl<T> PrimitiveArrayBuilder<T>
where T: DFNumericType
{
    pub fn with_capacity(capacity: usize) -> Self {
        PrimitiveArrayBuilder {
            builder: MutablePrimitiveArray::<T::Native>::with_capacity(capacity),
        }
    }
}

pub type DFUInt8ArrayBuilder = PrimitiveArrayBuilder<UInt8Type>;
pub type DFInt8ArrayBuilder = PrimitiveArrayBuilder<Int8Type>;
pub type DFUInt16ArrayBuilder = PrimitiveArrayBuilder<UInt16Type>;
pub type DFInt16ArrayBuilder = PrimitiveArrayBuilder<Int16Type>;
pub type DFUInt32ArrayBuilder = PrimitiveArrayBuilder<UInt32Type>;
pub type DFInt32ArrayBuilder = PrimitiveArrayBuilder<Int32Type>;
pub type DFUInt64ArrayBuilder = PrimitiveArrayBuilder<UInt64Type>;
pub type DFInt64ArrayBuilder = PrimitiveArrayBuilder<Int64Type>;

pub struct Utf8ArrayBuilder {
    pub builder: MutableUtf8Array<i64>,
}

impl Utf8ArrayBuilder {
    /// Create a new UtfArrayBuilder
    ///
    /// # Arguments
    ///
    /// * `capacity` - Number of string elements in the final array.
    pub fn with_capacity(bytes_capacity: usize) -> Self {
        Utf8ArrayBuilder {
            builder: MutableUtf8Array::with_capacity(bytes_capacity),
        }
    }

    /// Appends a value of type `T` into the builder
    #[inline]
    pub fn append_value<S: AsRef<str>>(&mut self, v: S) {
        self.builder.push(Some(v))
    }

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_null(&mut self) {
        self.builder.push_null();
    }

    #[inline]
    pub fn append_option<S: AsRef<str>>(&mut self, opt: Option<S>) {
        match opt {
            Some(s) => self.append_value(s.as_ref()),
            None => self.append_null(),
        }
    }

    pub fn finish(&mut self) -> DFUtf8Array {
        let array = self.builder.as_arc();
        array.into()
    }
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
        let mut builder = PrimitiveArrayBuilder::with_capacity(get_iter_capacity(&it));
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
        let mut builder = BooleanArrayBuilder::with_capacity(get_iter_capacity(&it));
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
        let mut builder = Utf8ArrayBuilder::with_capacity(values_size);
        v.iter().for_each(|val| {
            builder.append_value(val.as_ref());
        });

        builder.finish()
    }

    fn new_from_opt_slice(opt_v: &[Option<S>]) -> Self {
        let values_size = opt_v.iter().fold(0, |acc, s| match s {
            Some(s) => acc + s.as_ref().len(),
            None => acc,
        });
        let mut builder = Utf8ArrayBuilder::with_capacity(values_size);
        opt_v.iter().for_each(|opt| match opt {
            Some(v) => builder.append_value(v.as_ref()),
            None => builder.append_null(),
        });
        builder.finish()
    }

    fn new_from_opt_iter(it: impl Iterator<Item = Option<S>>) -> Self {
        let cap = get_iter_capacity(&it);
        let mut builder = Utf8ArrayBuilder::with_capacity(cap * 5);
        it.for_each(|opt| builder.append_option(opt));
        builder.finish()
    }

    /// Create a new DataArray from an iterator.
    fn new_from_iter(it: impl Iterator<Item = S>) -> Self {
        let cap = get_iter_capacity(&it);
        let mut builder = Utf8ArrayBuilder::with_capacity(cap * 5);
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

pub struct BinaryArrayBuilder {
    builder: MutableBinaryArray<i64>,
}

impl BinaryArrayBuilder {
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

    pub fn finish(&mut self) -> DataArray<BinaryType> {
        let array = self.builder.as_arc();
        DFBinaryArray::from(array)
    }
}
