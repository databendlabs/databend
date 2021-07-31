// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use std::sync::Arc;
use super::take_random::TakeRandom;
use super::take_random::TakeRandomUtf8;
use crate::arrays::DataArray;
use crate::prelude::LargeUtf8Array;
use crate::DFBooleanArray;
use crate::DFListArray;
use crate::DFNumericType;
use crate::DFUtf8Array;

macro_rules! impl_take_random_get {
    ($self:ident, $index:ident) => {{
        // Safety:
        // index should be in bounds
        let arr = $self.downcast_ref();
        if arr.is_valid($index) {
            Some(arr.value_unchecked($index))
        } else {
            None
        }
    }};
}

macro_rules! impl_take_random_get_unchecked {
    ($self:ident, $index:ident) => {{
        let arr = $self.downcast_ref();
        arr.value_unchecked($index)
    }};
}

impl<T> TakeRandom for DataArray<T>
    where T: DFNumericType
{
    type Item = T::Native;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        unsafe { impl_take_random_get!(self, index) }
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        impl_take_random_get_unchecked!(self, index)
    }
}

impl<'a, T> TakeRandom for &'a DataArray<T>
    where T: DFNumericType
{
    type Item = T::Native;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        (*self).get(index)
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        (*self).get_unchecked(index)
    }
}

impl TakeRandom for DFBooleanArray {
    type Item = bool;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        // Safety:
        // Out of bounds is checked and downcast is of correct type
        unsafe { impl_take_random_get!(self, index) }
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        impl_take_random_get_unchecked!(self, index)
    }
}

impl<'a> TakeRandom for &'a DFUtf8Array {
    type Item = &'a str;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        // Safety:
        // Out of bounds is checked and downcast is of correct type
        unsafe { impl_take_random_get!(self, index) }
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        impl_take_random_get_unchecked!(self, index)
    }
}

// extra trait such that it also works without extra reference.
// Autoref will insert the reference and
impl<'a> TakeRandomUtf8 for &'a DFUtf8Array {
    type Item = &'a str;

    #[inline]
    fn get(self, index: usize) -> Option<Self::Item> {
        // Safety:
        // Out of bounds is checkedn and downcast is of correct type
        unsafe { impl_take_random_get!(self, index) }
    }

    #[inline]
    unsafe fn get_unchecked(self, index: usize) -> Self::Item {
        impl_take_random_get_unchecked!(self, index)
    }
}




macro_rules! impl_take_random_get {
    ($self:ident, $index:ident) => {{
        // Safety:
        // index should be in bounds
        let arr = $self.downcast_ref();
        if arr.is_valid($index) {
            Some(arr.value_unchecked($index))
        } else {
            None
        }
    }};
}

macro_rules! impl_take_random_get_unchecked {
    ($self:ident, $index:ident) => {{
        let arr = $self.downcast_ref();
        arr.value_unchecked($index)
    }};
}

impl TakeRandom for DFListArray {
    type Item = ArrayRef;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        // Safety:
        // Out of bounds is checked and downcast is of correct type
        let arr = self.downcast_ref();
        if arr.is_valid(index) {
            return Some(Arc::from(arr.value(index)));
        }
        return None;
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        let arr = self.downcast_ref();
        return Arc::from(arr.value_unchecked(index));
    }
}
