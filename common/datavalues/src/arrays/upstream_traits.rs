//! Implementations of upstream traits for DataArrayBase<T>
use std::borrow::Borrow;
use std::borrow::Cow;
use std::collections::LinkedList;
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::LargeStringArray;
use common_arrow::arrow::array::PrimitiveArray;

use crate::arrays::DataArrayBase;
use crate::utils::NoNull;
use crate::vec::AlignedVec;
use crate::BooleanType;
use crate::DFBooleanArray;
use crate::DFListArray;
use crate::DFNumericType;
use crate::DFPrimitiveType;
use crate::DFStringArray;
use crate::Utf8Type;

/// FromIterator trait

impl<T> FromIterator<Option<T::Native>> for DataArrayBase<T>
where T: DFPrimitiveType
{
    fn from_iter<I: IntoIterator<Item = Option<T::Native>>>(iter: I) -> Self {
        let iter = iter.into_iter();

        let arr: PrimitiveArray<T> = match iter.size_hint() {
            (a, Some(b)) if a == b => {
                // 2021-02-07: ~40% faster than builder.
                // It is unsafe because we cannot be certain that the iterators length can be trusted.
                // For most iterators that report the same upper bound as lower bound it is, but still
                // somebody can create an iterator that incorrectly gives those bounds.
                // This will not lead to UB, but will panic.
                unsafe {
                    let arr = PrimitiveArray::from_trusted_len_iter(iter);
                    assert_eq!(arr.len(), a);
                    arr
                }
            }
            _ => {
                // 2021-02-07: ~1.5% slower than builder. Will still use this as it is more idiomatic and will
                // likely improve over time.
                PrimitiveArray::from_iter(iter)
            }
        };
        let array = Arc::new(arr) as ArrayRef;
        array.into()
    }
}

// NoNull is only a wrapper needed for specialization
impl<T> FromIterator<T::Native> for NoNull<DataArrayBase<T>>
where T: DFPrimitiveType
{
    // We use AlignedVec because it is way faster than Arrows builder. We can do this because we
    // know we don't have null values.
    fn from_iter<I: IntoIterator<Item = T::Native>>(iter: I) -> Self {
        // 2021-02-07: aligned vec was ~2x faster than arrow collect.
        let iter = iter.into_iter();
        let mut av = AlignedVec::with_capacity_aligned(0);
        av.extend(iter);
        NoNull::new(DataArrayBase::new_from_aligned_vec("", av))
    }
}

impl FromIterator<Option<bool>> for DFBooleanArray {
    fn from_iter<I: IntoIterator<Item = Option<bool>>>(iter: I) -> Self {
        let array = Arc::new(BooleanArray::from_iter(iter)) as ArrayRef;
        array.into()
    }
}

impl FromIterator<bool> for DFBooleanArray {
    fn from_iter<I: IntoIterator<Item = bool>>(iter: I) -> Self {
        // 2021-02-07: this was ~70% faster than with the builder, even with the extra Option<T> added.
        let arr = BooleanArray::from_iter(iter.into_iter().map(Some));

        let array = Arc::new(arr) as ArrayRef;
        array.into()
    }
}

impl FromIterator<bool> for NoNull<DFBooleanArray> {
    fn from_iter<I: IntoIterator<Item = bool>>(iter: I) -> Self {
        let ca = iter.into_iter().collect::<DFBooleanArray>();
        NoNull::new(ca)
    }
}

// FromIterator for Utf8Type variants.Array

impl<Ptr> FromIterator<Option<Ptr>> for DFStringArray
where Ptr: AsRef<str>
{
    fn from_iter<I: IntoIterator<Item = Option<Ptr>>>(iter: I) -> Self {
        // 2021-02-07: this was ~30% faster than with the builder.
        let arr = LargeStringArray::from_iter(iter);
        let array = Arc::new(arr) as ArrayRef;
        array.into()
    }
}

/// Local AsRef<T> trait to circumvent the orphan rule.
pub trait DFAsRef<T: ?Sized>: AsRef<T> {}

impl DFAsRef<str> for String {}
impl DFAsRef<str> for &str {}
// &["foo", "bar"]
impl DFAsRef<str> for &&str {}
impl<'a> DFAsRef<str> for Cow<'a, str> {}

impl<Ptr> FromIterator<Ptr> for DFStringArray
where Ptr: DFAsRef<str>
{
    fn from_iter<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        let arr = LargeStringArray::from_iter_values(iter);

        let array = Arc::new(arr) as ArrayRef;
        array.into()
    }
}

/// From trait
impl<'a> From<&'a DFStringArray> for Vec<Option<&'a str>> {
    fn from(ca: &'a DFStringArray) -> Self {
        ca.downcast_iter().collect()
    }
}

impl From<DFStringArray> for Vec<Option<String>> {
    fn from(ca: DFStringArray) -> Self {
        ca.downcast_iter()
            .map(|opt| opt.map(|s| s.to_string()))
            .collect()
    }
}

impl<'a> From<&'a DFBooleanArray> for Vec<Option<bool>> {
    fn from(ca: &'a DFBooleanArray) -> Self {
        ca.downcast_iter().collect()
    }
}

impl From<DFBooleanArray> for Vec<Option<bool>> {
    fn from(ca: DFBooleanArray) -> Self {
        ca.downcast_iter().collect()
    }
}

impl<'a, T> From<&'a DataArrayBase<T>> for Vec<Option<T::Native>>
where T: DFNumericType
{
    fn from(ca: &'a DataArrayBase<T>) -> Self {
        ca.downcast_iter().collect()
    }
}
