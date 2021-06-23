use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::LargeListArray;
use common_arrow::arrow::array::LargeStringArray;
use common_arrow::arrow::array::PrimitiveArray;
use unsafe_unwrap::UnsafeUnwrap;

use super::take_random::TakeRandom;
use super::take_random::TakeRandomUtf8;
use crate::arrays::DataArrayBase;
use crate::arrays::DataArrayRef;
use crate::DFBooleanArray;
use crate::DFListArray;
use crate::DFNumericType;
use crate::DFStringArray;

macro_rules! impl_take_random_get {
    ($self:ident, $index:ident, $array_type:ty) => {{
        let array = $self.array;
        // Safety:
        // caller should give right array type
        let arr = &*(array as *const ArrayRef as *const Arc<$array_type>);
        // Safety:
        // index should be in bounds
        if arr.is_valid($index) {
            Some(arr.value_unchecked($index))
        } else {
            None
        }
    }};
}

macro_rules! impl_take_random_get_unchecked {
    ($self:ident, $index:ident, $array_type:ty) => {{
        let array = $self.array;
        let arr = &*(array as *const ArrayRef as *const Arc<$array_type>);
        arr.value_unchecked($index)
    }};
}

impl<T> TakeRandom for DataArrayBase<T>
where T: DFNumericType
{
    type Item = T::Native;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        unsafe { impl_take_random_get!(self, index, PrimitiveArray<T>) }
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        impl_take_random_get_unchecked!(self, index, PrimitiveArray<T>)
    }
}

impl<'a, T> TakeRandom for &'a DataArrayBase<T>
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
        unsafe { impl_take_random_get!(self, index, BooleanArray) }
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        impl_take_random_get_unchecked!(self, index, BooleanArray)
    }
}

impl<'a> TakeRandom for &'a DFStringArray {
    type Item = &'a str;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        // Safety:
        // Out of bounds is checked and downcast is of correct type
        unsafe { impl_take_random_get!(self, index, LargeStringArray) }
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        let (chunk_idx, idx) = self.index_to_chunked_index(index);
        let arr = {
            let arr = self.chunks.get_unchecked(chunk_idx);
            &*(arr as *const ArrayRef as *const Arc<LargeStringArray>)
        };
        arr.value_unchecked(idx)
    }
}

// extra trait such that it also works without extra reference.
// Autoref will insert the reference and
impl<'a> TakeRandomUtf8 for &'a DFStringArray {
    type Item = &'a str;

    #[inline]
    fn get(self, index: usize) -> Option<Self::Item> {
        // Safety:
        // Out of bounds is checkedn and downcast is of correct type
        unsafe { impl_take_random_get!(self, index, LargeStringArray) }
    }

    #[inline]
    unsafe fn get_unchecked(self, index: usize) -> Self::Item {
        let (chunk_idx, idx) = self.index_to_chunked_index(index);
        let arr = {
            let arr = self.chunks.get_unchecked(chunk_idx);
            &*(arr as *const ArrayRef as *const Arc<LargeStringArray>)
        };
        arr.value_unchecked(idx)
    }
}

impl TakeRandom for DFListArray {
    type Item = DataArrayRef;

    #[inline]
    fn get(&self, index: usize) -> Option<Self::Item> {
        // Safety:
        // Out of bounds is checked and downcast is of correct type
        let opt_arr = unsafe { impl_take_random_get!(self, index, LargeListArray) };
        opt_arr.map(|arr| {
            let s = DataArrayRef::try_from(arr);
            s.unwrap()
        })
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> Self::Item {
        let (chunk_idx, idx) = self.index_to_chunked_index(index);
        let arr = {
            let arr = self.chunks.get_unchecked(chunk_idx);
            &*(arr as *const ArrayRef as *const Arc<LargeListArray>)
        };
        let arr = arr.value_unchecked(idx);
        let s = DataArrayRef::try_from(arr);
        s.unwrap()
    }
}
