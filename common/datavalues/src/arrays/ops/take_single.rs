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
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;

use super::take_random::TakeRandom;
use super::take_random::TakeRandomUtf8;
use crate::prelude::*;

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

impl<T> TakeRandom for DFPrimitiveArray<T>
where T: DFPrimitiveType
{
    type Item = T;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        unsafe { impl_take_random_get!(self, index) }
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        impl_take_random_get_unchecked!(self, index)
    }
}

impl<'a, T> TakeRandom for &'a DFPrimitiveArray<T>
where T: DFPrimitiveType
{
    type Item = T;

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
        None
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        let arr = self.downcast_ref();
        return Arc::from(arr.value_unchecked(index));
    }
}
