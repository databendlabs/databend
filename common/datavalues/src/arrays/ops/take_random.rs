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

use common_arrow::arrow::array::*;

use crate::prelude::*;
use crate::series::IntoSeries;
use crate::series::Series;
use crate::series::SeriesWrap;

/// Random access
pub trait TakeRandom {
    type Item;

    /// Get a nullable value by index.
    ///
    /// # Safety
    ///
    /// Out of bounds access doesn't Error but will return a Null value
    fn get(&self, index: usize) -> Option<Self::Item>;

    /// Get a value by index and ignore the null bit.
    ///
    /// # Safety
    ///
    /// This doesn't check if the underlying type is null or not and may return an uninitialized value.
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item;
}

// Utility trait because associated type needs a lifetime
pub trait TakeRandomUtf8 {
    type Item;

    /// Get a nullable value by index.
    ///
    /// # Safety
    ///
    /// Out of bounds access doesn't Error but will return a Null value
    fn get(self, index: usize) -> Option<Self::Item>;

    /// Get a value by index and ignore the null bit.
    ///
    /// # Safety
    /// This doesn't check if the underlying type is null or not and may return an uninitialized value.
    unsafe fn get_unchecked(self, index: usize) -> Self::Item;
}

pub enum TakeIdx<'a, I, INulls>
where
    I: Iterator<Item = usize>,
    INulls: Iterator<Item = Option<usize>>,
{
    Array(&'a UInt32Array),
    Iter(I),
    // will return a null where None
    IterNulls(INulls),
}

pub type DummyIter<T> = std::iter::Once<T>;
pub type TakeIdxIter<'a, I> = TakeIdx<'a, I, DummyIter<Option<usize>>>;
pub type TakeIdxIterNull<'a, INull> = TakeIdx<'a, DummyIter<usize>, INull>;

impl<'a, I> From<I> for TakeIdx<'a, I, DummyIter<Option<usize>>>
where I: Iterator<Item = usize>
{
    fn from(iter: I) -> Self {
        TakeIdx::Iter(iter)
    }
}

impl<'a, INulls> From<SeriesWrap<INulls>> for TakeIdx<'a, DummyIter<usize>, INulls>
where INulls: Iterator<Item = Option<usize>>
{
    fn from(iter: SeriesWrap<INulls>) -> Self {
        TakeIdx::IterNulls(iter.0)
    }
}

macro_rules! take_random_get {
    ($self:ident, $index:ident) => {{
        if $self.arr.is_null($index) {
            None
        } else {
            // Safety:
            // bound checked above
            unsafe { Some($self.arr.value_unchecked($index)) }
        }
    }};
}

/// Create a type that implements a faster `TakeRandom`.
pub trait IntoTakeRandom<'a> {
    type Item;
    type TakeRandom;
    /// Create a type that implements `TakeRandom`.
    fn take_rand(&self) -> Self::TakeRandom;
}

pub enum TakeRandBranch<N, S> {
    SingleNoNull(N),
    Single(S),
}

impl<N, S, I> TakeRandom for TakeRandBranch<N, S>
where
    N: TakeRandom<Item = I>,
    S: TakeRandom<Item = I>,
{
    type Item = I;

    fn get(&self, index: usize) -> Option<Self::Item> {
        match self {
            Self::SingleNoNull(s) => s.get(index),
            Self::Single(s) => s.get(index),
        }
    }

    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        match self {
            Self::SingleNoNull(s) => s.get_unchecked(index),
            Self::Single(s) => s.get_unchecked(index),
        }
    }
}

impl<'a, T> IntoTakeRandom<'a> for &'a DFPrimitiveArray<T>
where T: DFPrimitiveType
{
    type Item = T;
    type TakeRandom = TakeRandBranch<NumTakeRandomCont<'a, T>, NumTakeRandomSingleArray<'a, T>>;

    #[inline]
    fn take_rand(&self) -> Self::TakeRandom {
        if self.null_count() == 0 {
            let t = NumTakeRandomCont {
                slice: self.inner().values(),
            };
            TakeRandBranch::SingleNoNull(t)
        } else {
            let t = NumTakeRandomSingleArray {
                arr: self.inner(),
            };
            TakeRandBranch::Single(t)
        }
    }
}

pub struct Utf8TakeRandom<'a> {
    arr: &'a LargeUtf8Array,
}

impl<'a> TakeRandom for Utf8TakeRandom<'a> {
    type Item = &'a str;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        take_random_get!(self, index)
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        self.arr.value_unchecked(index)
    }
}

impl<'a> IntoTakeRandom<'a> for &'a DFUtf8Array {
    type Item = &'a str;
    type TakeRandom = TakeRandBranch<Utf8TakeRandom<'a>, Utf8TakeRandom<'a>>;

    fn take_rand(&self) -> Self::TakeRandom {
        let arr = self.inner();
        let t = Utf8TakeRandom { arr };
        TakeRandBranch::Single(t)
    }
}

impl<'a> IntoTakeRandom<'a> for &'a DFBooleanArray {
    type Item = bool;
    type TakeRandom = TakeRandBranch<BoolTakeRandom<'a>, BoolTakeRandom<'a>>;

    fn take_rand(&self) -> Self::TakeRandom {
        let arr = self.inner();
        let t = BoolTakeRandom { arr };
        TakeRandBranch::Single(t)
    }
}

impl<'a> IntoTakeRandom<'a> for &'a DFListArray {
    type Item = Series;
    type TakeRandom = TakeRandBranch<ListTakeRandom<'a>, ListTakeRandom<'a>>;

    fn take_rand(&self) -> Self::TakeRandom {
        let t = ListTakeRandom {
            arr: self.inner(),
        };
        TakeRandBranch::Single(t)
    }
}

pub struct NumTakeRandomCont<'a, T> {
    slice: &'a [T],
}

impl<'a, T> TakeRandom for NumTakeRandomCont<'a, T>
where T: Copy
{
    type Item = T;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        self.slice.get(index).copied()
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        *self.slice.get_unchecked(index)
    }
}

pub struct NumTakeRandomSingleArray<'a, T>
where T: DFPrimitiveType
{
    arr: &'a PrimitiveArray<T>,
}

impl<'a, T> TakeRandom for NumTakeRandomSingleArray<'a, T>
where T: DFPrimitiveType
{
    type Item = T;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        take_random_get!(self, index)
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        self.arr.value_unchecked(index)
    }
}

pub struct BoolTakeRandom<'a> {
    arr: &'a BooleanArray,
}

impl<'a> TakeRandom for BoolTakeRandom<'a> {
    type Item = bool;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        take_random_get!(self, index)
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        self.arr.value_unchecked(index)
    }
}

pub struct ListTakeRandom<'a> {
    arr: &'a LargeListArray,
}

impl<'a> TakeRandom for ListTakeRandom<'a> {
    type Item = Series;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        let v: Option<Box<dyn Array>> = take_random_get!(self, index);
        let v = v.map(|c| {
            let c: Arc<dyn Array> = Arc::from(c);
            c
        });
        v.map(|v| v.into_series())
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        let v: Arc<dyn Array> = Arc::from(self.arr.value_unchecked(index));
        v.into_series()
    }
}
