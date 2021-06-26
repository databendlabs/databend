use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::BooleanBuilder;
use common_arrow::arrow::array::LargeListBuilder;
use common_arrow::arrow::array::LargeStringBuilder;
use common_arrow::arrow::array::PrimitiveBuilder;
use common_arrow::arrow::buffer::Buffer;

use crate::arrays::DataArray;
use crate::series::Series;
use crate::utils::get_iter_capacity;
use crate::utils::NoNull;
use crate::BooleanType;
use crate::DFBooleanArray;
use crate::DFNumericType;
use crate::DFStringArray;
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
    fn finish(self) -> DataArray<T>;
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

    fn finish(mut self) -> DFBooleanArray {
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

    fn finish(mut self) -> DataArray<T> {
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
    pub builder: LargeStringBuilder,
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
            builder: LargeStringBuilder::with_capacity(bytes_capacity, capacity),
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

    pub fn finish(mut self) -> DFStringArray {
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

impl<S> NewDataArray<Utf8Type, S> for DFStringArray
where S: AsRef<str>
{
    fn new_from_slice(v: &[S]) -> Self {
        let values_size = v.iter().fold(0, |acc, s| acc + s.as_ref().len());

        let mut builder = LargeStringBuilder::with_capacity(values_size, v.len());
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
