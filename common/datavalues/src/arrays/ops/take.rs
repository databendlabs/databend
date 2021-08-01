// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

//! Traits to provide fast Random access to DataArrays data.
//! This prevents downcasting every iteration.
//! IntoTakeRandom provides structs that implement the TakeRandom trait.
//! There are several structs that implement the fastest path for random access.
//!

use std::fmt::Debug;

use common_arrow::arrow::array::*;
use common_arrow::arrow::compute::take;
use common_exception::ErrorCode;
use common_exception::Result;

use super::TakeIdx;
use crate::arrays::kernels::*;
use crate::arrays::DataArray;
use crate::prelude::*;
use crate::*;

// TODO add unchecked take
pub trait ArrayTake: Debug {
    /// Take values from DataArray by index.
    ///
    /// # Safety
    ///
    /// Doesn't do any bound checking.
    unsafe fn take_unchecked<I, INulls>(&self, _indices: TakeIdx<I, INulls>) -> Result<Self>
    where
        Self: std::marker::Sized,
        I: Iterator<Item = usize>,
        INulls: Iterator<Item = Option<usize>>,
    {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported take_unchecked operation for {:?}",
            self,
        )))
    }

    /// Take values from DataArray by index.
    fn take<I, INulls>(&self, _indices: TakeIdx<I, INulls>) -> Result<Self>
    where
        Self: std::marker::Sized,
        I: Iterator<Item = usize>,
        INulls: Iterator<Item = Option<usize>>,
    {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported take operation for {:?}",
            self,
        )))
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
        let primitive_array = self.downcast_ref();
        match indices {
            TakeIdx::Array(array) => {
                if self.is_empty() {
                    return Ok(Self::full_null(array.len()));
                }
                let taked_array = take::take(self.array.as_ref(), array)?;
                Ok(Self::from(taked_array))
            }
            TakeIdx::Iter(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let array =
                    take_primitive_iter_unchecked::<T, _>(primitive_array, iter) as ArrayRef;
                Ok(Self::from(array))
            }
            TakeIdx::IterNulls(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let array =
                    take_primitive_opt_iter_unchecked::<T, _>(primitive_array, iter) as ArrayRef;
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
        unsafe { self.take_unchecked(indices) }
    }
}

impl ArrayTake for DFBooleanArray {
    unsafe fn take_unchecked<I, INulls>(&self, indices: TakeIdx<I, INulls>) -> Result<Self>
    where
        Self: std::marker::Sized,
        I: Iterator<Item = usize>,
        INulls: Iterator<Item = Option<usize>>,
    {
        let boolean_array = self.downcast_ref();
        match indices {
            TakeIdx::Array(array) => {
                if self.is_empty() {
                    return Ok(Self::full_null(array.len()));
                }
                let array = take::take(array, array)?;
                Ok(Self::from(array))
            }
            TakeIdx::Iter(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let array = take_bool_iter_unchecked(boolean_array, iter) as ArrayRef;
                Ok(Self::from(array))
            }
            TakeIdx::IterNulls(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let array = take_bool_opt_iter_unchecked(boolean_array, iter) as ArrayRef;
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
        unsafe { self.take_unchecked(indices) }
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
                let array = take::take(str_array, array)?;
                Ok(Self::from(array))
            }
            TakeIdx::Iter(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let array = take_utf8_iter_unchecked(str_array, iter) as ArrayRef;
                Ok(Self::from(array))
            }
            TakeIdx::IterNulls(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let array = take_utf8_opt_iter_unchecked(str_array, iter) as ArrayRef;
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
        unsafe { self.take_unchecked(indices) }
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
                let array = take::take(list_array, array)?;
                Ok(Self::from(array))
            }
            TakeIdx::Iter(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }
                let ca: DFListArray = take_iter_n_arrays!(self, iter);
                Ok(ca)
            }
            TakeIdx::IterNulls(iter) => {
                if self.is_empty() {
                    return Ok(Self::full_null(iter.size_hint().0));
                }

                let ca: DFListArray = take_opt_iter_n_arrays!(self, iter);
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
