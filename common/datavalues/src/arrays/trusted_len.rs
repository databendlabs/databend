use std::borrow::Borrow;

use common_arrow::arrow::array::*;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::trusted_len::TrustedLen;

use super::DFAsRef;
use crate::prelude::*;
use crate::utils::NoNull;

pub trait FromTrustedLenIterator<A>: Sized {
    fn from_iter_trusted_length<T: IntoIterator<Item = A>>(iter: T) -> Self
    where T::IntoIter: TrustedLen;
}

impl<T> FromTrustedLenIterator<Option<T::Native>> for DataArray<T>
where T: DFPrimitiveType
{
    fn from_iter_trusted_length<I: IntoIterator<Item = Option<T::Native>>>(iter: I) -> Self {
        let iter = iter.into_iter();

        let arr = unsafe {
            PrimitiveArray::from_trusted_len_iter_unchecked(iter).to(T::data_type().to_arrow())
        };
        Self::from_arrow_array(arr)
    }
}

// NoNull is only a wrapper needed for specialization
impl<T> FromTrustedLenIterator<T::Native> for NoNull<DataArray<T>>
where T: DFPrimitiveType
{
    // We use AlignedVec because it is way faster than Arrows builder. We can do this because we
    // know we don't have null values.
    fn from_iter_trusted_length<I: IntoIterator<Item = T::Native>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let values = unsafe { Buffer::from_trusted_len_iter_unchecked(iter) };
        let arr = PrimitiveArray::from_data(T::data_type().to_arrow(), values, None);

        NoNull::new(DataArray::<T>::from_arrow_array(arr))
    }
}
impl<Ptr> FromTrustedLenIterator<Ptr> for DFListArray
where Ptr: Borrow<Series>
{
    fn from_iter_trusted_length<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        let iter = iter.into_iter();
        iter.collect()
    }
}

impl<Ptr> FromTrustedLenIterator<Option<Ptr>> for DFListArray
where Ptr: Borrow<Series>
{
    fn from_iter_trusted_length<I: IntoIterator<Item = Option<Ptr>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        iter.collect()
    }
}

impl FromTrustedLenIterator<Option<bool>> for DFBooleanArray {
    fn from_iter_trusted_length<I: IntoIterator<Item = Option<bool>>>(iter: I) -> Self
    where I::IntoIter: TrustedLen {
        let iter = iter.into_iter();
        let arr = BooleanArray::from_trusted_len_iter(iter);
        Self::from_arrow_array(arr)
    }
}

impl FromTrustedLenIterator<bool> for DFBooleanArray {
    fn from_iter_trusted_length<I: IntoIterator<Item = bool>>(iter: I) -> Self
    where I::IntoIter: TrustedLen {
        let iter = iter.into_iter();
        let arr = BooleanArray::from_trusted_len_values_iter(iter);
        Self::from_arrow_array(arr)
    }
}

impl FromTrustedLenIterator<bool> for NoNull<DFBooleanArray> {
    fn from_iter_trusted_length<I: IntoIterator<Item = bool>>(iter: I) -> Self {
        let iter = iter.into_iter();
        iter.collect()
    }
}
impl<Ptr> FromTrustedLenIterator<Ptr> for DFUtf8Array
where Ptr: DFAsRef<str>
{
    fn from_iter_trusted_length<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        let iter = iter.into_iter();
        iter.collect()
    }
}
