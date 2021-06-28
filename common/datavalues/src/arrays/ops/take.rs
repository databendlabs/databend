//! Traits to provide fast Random access to DataArrays data.
//! This prevents downcasting every iteration.
//! IntoTakeRandom provides structs that implement the TakeRandom trait.
//! There are several structs that implement the fastest path for random access.
//!
use std::ops::Deref;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::compute;
use common_arrow::arrow::compute::kernels::take::take;
use common_exception::Result;

use super::TakeIdx;
use crate::arrays::kernels::*;
use crate::arrays::DataArray;
use crate::prelude::*;
use crate::utils::NoNull;
use crate::*;

pub trait ArrayTake {
    /// Take values from DataArray by index.
    ///
    /// # Safety
    ///
    /// Doesn't do any bound checking.
    unsafe fn take_unchecked<I, INulls>(&self, indices: TakeIdx<I, INulls>) -> Result<Self>
    where
        Self: std::marker::Sized,
        I: Iterator<Item = usize>,
        INulls: Iterator<Item = Option<usize>>,
    {
        unimplemented!()
    }

    /// Take values from DataArray by index.
    fn take<I, INulls>(&self, indices: TakeIdx<I, INulls>) -> Result<Self>
    where
        Self: std::marker::Sized,
        I: Iterator<Item = usize>,
        INulls: Iterator<Item = Option<usize>>,
    {
        unimplemented!()
    }
}

/// Traverse and collect every nth element
pub trait ArrayTakeEvery<T> {
    /// Traverse and collect every nth element in a new array.
    fn take_every(&self, n: usize) -> DataArray<T> {
        unimplemented!()
    }
}

macro_rules! take_iter_n_arrays {
    ($ca:expr, $indices:expr) => {{
        let taker = $ca.take_rand();
        $indices.into_iter().map(|idx| taker.get(idx)).collect()
    }};
}

macro_rules! take_opt_iter_n_arrays {
    ($ca:expr, $indices:expr) => {{
        let taker = $ca.take_rand();
        $indices
            .into_iter()
            .map(|opt_idx| opt_idx.and_then(|idx| taker.get(idx)))
            .collect()
    }};
}

macro_rules! take_iter_n_arrays_unchecked {
    ($ca:expr, $indices:expr) => {{
        let taker = $ca.take_rand();
        $indices
            .into_iter()
            .map(|idx| taker.get_unchecked(idx))
            .collect()
    }};
}

macro_rules! take_opt_iter_n_arrays_unchecked {
    ($ca:expr, $indices:expr) => {{
        let taker = $ca.take_rand();
        $indices
            .into_iter()
            .map(|opt_idx| opt_idx.map(|idx| taker.get_unchecked(idx)))
            .collect()
    }};
}

/// Fast access by index.
impl<T> ArrayTake for DataArray<T>
where T: DFNumericType
{
    unsafe fn take_unchecked<I, INulls>(&self, indices: TakeIdx<I, INulls>) -> Result<Self>
    where
        Self: std::marker::Sized,
        I: Iterator<Item = usize>,
        INulls: Iterator<Item = Option<usize>>,
    {
        let mut primitive_array = self.downcast_ref();
        match indices {
            TakeIdx::Array(array) => {
                if self.is_empty() {
                    return Ok(Self::full_null(array.len()));
                }

                // take(chunks.next(), array, None),
                match self.null_count() {
                    0 => Ok(Self::from(
                        take_no_null_primitive(primitive_array, array) as ArrayRef
                    )),
                    _ => {
                        let taked_array = compute::take(self.array.as_ref(), array, None)?;
                        Ok(Self::from(taked_array))
                    }
                }
            }
            TakeIdx::Iter(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let array = match self.null_count() {
                    0 => take_no_null_primitive_iter_unchecked(primitive_array, iter) as ArrayRef,
                    _ => take_primitive_iter_unchecked(primitive_array, iter) as ArrayRef,
                };
                Ok(Self::from(array))
            }
            TakeIdx::IterNulls(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let array = match self.null_count() {
                    0 => {
                        take_no_null_primitive_opt_iter_unchecked(primitive_array, iter) as ArrayRef
                    }
                    _ => take_primitive_opt_iter_unchecked(primitive_array, iter) as ArrayRef,
                };
                Ok(Self::from(array))
            }
        }
    }

    fn take<I, INulls>(&self, indices: TakeIdx<I, INulls>) -> Result<Self>
    where
        Self: std::marker::Sized,
        I: Iterator<Item = usize>,
        INulls: Iterator<Item = Option<usize>>,
    {
        let mut primitive_array = self.downcast_ref();
        match indices {
            TakeIdx::Array(array) => {
                if self.is_empty() {
                    return Ok(Self::full_null(array.len()));
                }
                let array = compute::take(array, array, None)?;
                Ok(Self::from(array))
            }
            TakeIdx::Iter(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let array = match self.null_count() {
                    0 => take_no_null_primitive_iter(primitive_array, iter) as ArrayRef,
                    _ => take_primitive_iter(primitive_array, iter) as ArrayRef,
                };
                Ok(Self::from(array))
            }
            TakeIdx::IterNulls(_) => {
                panic!("not supported in take, only supported in take_unchecked for the join operation")
            }
        }
    }
}

impl ArrayTake for DFBooleanArray {
    unsafe fn take_unchecked<I, INulls>(&self, indices: TakeIdx<I, INulls>) -> Result<Self>
    where
        Self: std::marker::Sized,
        I: Iterator<Item = usize>,
        INulls: Iterator<Item = Option<usize>>,
    {
        let mut boolean_array = self.downcast_ref();
        match indices {
            TakeIdx::Array(array) => {
                if self.is_empty() {
                    return Ok(Self::full_null(array.len()));
                }
                let array = compute::take(array, array, None)?;
                Ok(Self::from(array))
            }
            TakeIdx::Iter(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let array = match self.null_count() {
                    0 => take_no_null_bool_iter_unchecked(boolean_array, iter) as ArrayRef,
                    _ => take_bool_iter_unchecked(boolean_array, iter) as ArrayRef,
                };
                Ok(Self::from(array))
            }
            TakeIdx::IterNulls(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let array = match self.null_count() {
                    0 => take_no_null_bool_opt_iter_unchecked(boolean_array, iter) as ArrayRef,
                    _ => take_bool_opt_iter_unchecked(boolean_array, iter) as ArrayRef,
                };
                Ok(Self::from(array))
            }
        }
    }

    fn take<I, INulls>(&self, indices: TakeIdx<I, INulls>) -> Result<Self>
    where
        Self: std::marker::Sized,
        I: Iterator<Item = usize>,
        INulls: Iterator<Item = Option<usize>>,
    {
        let mut boolean_array = self.downcast_ref();
        match indices {
            TakeIdx::Array(array) => {
                if self.is_empty() {
                    return Ok(Self::full_null(array.len()));
                }
                let array = compute::take(boolean_array, array, None)?;
                Ok(Self::from(array))
            }
            TakeIdx::Iter(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let array = match self.null_count() {
                    0 => take_no_null_bool_iter(boolean_array, iter) as ArrayRef,
                    _ => take_bool_iter(boolean_array, iter) as ArrayRef,
                };
                Ok(Self::from(array))
            }
            TakeIdx::IterNulls(_) => {
                panic!("not supported in take, only supported in take_unchecked for the join operation")
            }
        }
    }
}

impl ArrayTake for DFUtf8Array {
    unsafe fn take_unchecked<I, INulls>(&self, indices: TakeIdx<I, INulls>) -> Result<Self>
    where
        Self: std::marker::Sized,
        I: Iterator<Item = usize>,
        INulls: Iterator<Item = Option<usize>>,
    {
        let str_array = self.downcast_ref();
        match indices {
            TakeIdx::Array(array) => {
                let array = compute::take(str_array, array, None)?;
                Ok(Self::from(array))
            }
            TakeIdx::Iter(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let array = match self.null_count() {
                    0 => take_no_null_utf8_iter_unchecked(str_array, iter) as ArrayRef,
                    _ => take_utf8_iter_unchecked(str_array, iter) as ArrayRef,
                };
                Ok(Self::from(array))
            }
            TakeIdx::IterNulls(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let array = match self.null_count() {
                    0 => take_no_null_utf8_opt_iter_unchecked(str_array, iter) as ArrayRef,
                    _ => take_utf8_opt_iter_unchecked(str_array, iter) as ArrayRef,
                };
                Ok(Self::from(array))
            }
        }
    }

    fn take<I, INulls>(&self, indices: TakeIdx<I, INulls>) -> Result<Self>
    where
        Self: std::marker::Sized,
        I: Iterator<Item = usize>,
        INulls: Iterator<Item = Option<usize>>,
    {
        let str_array = self.downcast_ref();
        match indices {
            TakeIdx::Array(array) => {
                let array = compute::take(str_array, array, None)?;
                Ok(Self::from(array))
            }
            TakeIdx::Iter(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let array = match self.null_count() {
                    0 => take_no_null_utf8_iter(str_array, iter) as ArrayRef,
                    _ => take_utf8_iter(str_array, iter) as ArrayRef,
                };
                Ok(Self::from(array))
            }
            TakeIdx::IterNulls(_) => {
                panic!("not supported in take, only supported in take_unchecked for the join operation")
            }
        }
    }
}

impl ArrayTake for DFListArray {
    unsafe fn take_unchecked<I, INulls>(&self, indices: TakeIdx<I, INulls>) -> Result<Self>
    where
        Self: std::marker::Sized,
        I: Iterator<Item = usize>,
        INulls: Iterator<Item = Option<usize>>,
    {
        self.take(indices)
    }

    fn take<I, INulls>(&self, indices: TakeIdx<I, INulls>) -> Result<Self>
    where
        Self: std::marker::Sized,
        I: Iterator<Item = usize>,
        INulls: Iterator<Item = Option<usize>>,
    {
        let list_array = self.downcast_ref();
        match indices {
            TakeIdx::Array(array) => {
                let array = compute::take(list_array, array, None)?;
                Ok(Self::from(array))
            }
            TakeIdx::Iter(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let mut ca: DFListArray = take_iter_n_arrays!(self, iter);
                Ok(ca)
            }
            TakeIdx::IterNulls(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }

                let mut ca: DFListArray = take_opt_iter_n_arrays!(self, iter);
                Ok(ca)
            }
        }
    }
}

impl ArrayTake for DFNullArray {}
impl ArrayTake for DFStructArray {}
impl ArrayTake for DFBinaryArray {}

pub trait AsTakeIndex {
    fn as_take_iter<'a>(&'a self) -> Box<dyn Iterator<Item = usize> + 'a>;

    fn as_opt_take_iter<'a>(&'a self) -> Box<dyn Iterator<Item = Option<usize>> + 'a> {
        unimplemented!()
    }

    fn take_index_len(&self) -> usize;
}

impl<T> ArrayTakeEvery<T> for DataArray<T>
where T: DFNumericType
{
    fn take_every(&self, n: usize) -> DataArray<T> {
        if self.null_count() == 0 {
            let a: NoNull<_> = self.into_no_null_iter().step_by(n).collect();
            a.into_inner()
        } else {
            self.downcast_iter().step_by(n).collect()
        }
    }
}

impl ArrayTakeEvery<BooleanType> for DFBooleanArray {
    fn take_every(&self, n: usize) -> DFBooleanArray {
        if self.null_count() == 0 {
            self.into_no_null_iter().step_by(n).collect()
        } else {
            self.into_iter().step_by(n).collect()
        }
    }
}

impl ArrayTakeEvery<Utf8Type> for DFUtf8Array {
    fn take_every(&self, n: usize) -> DFUtf8Array {
        if self.null_count() == 0 {
            self.into_no_null_iter().step_by(n).collect()
        } else {
            self.into_iter().step_by(n).collect()
        }
    }
}

impl ArrayTakeEvery<ListType> for DFListArray {
    fn take_every(&self, n: usize) -> DFListArray {
        if self.null_count() == 0 {
            self.into_no_null_iter().step_by(n).collect()
        } else {
            self.into_iter().step_by(n).collect()
        }
    }
}

impl ArrayTakeEvery<NullType> for DFNullArray {}
impl ArrayTakeEvery<StructType> for DFStructArray {}
impl ArrayTakeEvery<BinaryType> for DFBinaryArray {}
