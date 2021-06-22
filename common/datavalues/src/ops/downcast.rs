use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::LargeListArray;
use common_arrow::arrow::array::LargeStringArray;
use common_arrow::arrow::array::PrimitiveArray;

use crate::data_array_base::DataArrayBase;
use crate::DFBooleanArray;
use crate::DFListArray;
use crate::DFNumericType;
use crate::DFStringArray;

impl<T> DataArrayBase<T>
where T: DFNumericType
{
    pub fn downcast_iter(&self) -> impl Iterator<Item = Option<T::Native>> + DoubleEndedIterator {
        let arr = &*self.array;
        let arr = unsafe { &*(arr as *const dyn Array as *const PrimitiveArray<T>) };
        arr.into_iter()
    }
}

impl DFBooleanArray {
    pub fn downcast_iter(&self) -> impl Iterator<Item = Option<bool>> + DoubleEndedIterator {
        let arr = &*self.array;
        let arr = unsafe { &*(arr as *const dyn Array as *const BooleanArray) };
        arr.into_iter()
    }
}

impl DFStringArray {
    pub fn downcast_iter<'a>(&self) -> impl Iterator<Item = Option<&'a str>> + DoubleEndedIterator {
        let arr = &*self.array;
        let arr = unsafe { &*(arr as *const dyn Array as *const LargeStringArray) };
        arr.into_iter()
    }
}

impl DFListArray {
    pub fn downcast_iter<'a>(
        &self,
    ) -> impl Iterator<Item = Option<ArrayRef>> + DoubleEndedIterator {
        let arr = &*self.array;
        let arr = unsafe { &*(arr as *const dyn Array as *const LargeListArray) };

        arr.iter()
    }
}
