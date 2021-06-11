use std::marker::PhantomData;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayData;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::PrimitiveArray;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::data_new::*;

#[derive(Clone)]
pub struct DFArray<T> {
    pub array: ArrayRef,
    phantom: PhantomData<T>,
}

pub type DFBooleanArray = DFArray<BooleanType>;
pub type DFUInt8Array = DFArray<UInt8Type>;
pub type DFUInt16Array = DFArray<UInt16Type>;
pub type DFUInt32Array = DFArray<UInt32Type>;
pub type DFUInt64Array = DFArray<UInt64Type>;
pub type DFInt8Array = DFArray<Int8Type>;
pub type DFInt16Array = DFArray<Int16Type>;
pub type DFInt32Array = DFArray<Int32Type>;
pub type DFInt64Array = DFArray<Int64Type>;
pub type DFFloat32Array = DFArray<Float32Type>;
pub type DFFloat64Array = DFArray<Float64Type>;
pub type DFUtf8Array = DFArray<Utf8Type>;
pub type DFListArray = DFArray<ListType>;
pub type DFDate32Array = DFArray<Date32Type>;
pub type DFDate64Array = DFArray<Date64Type>;
pub type DFDurationNanosecondArray = DFArray<DurationNanosecondType>;
pub type DFDurationMillisecondArray = DFArray<DurationMillisecondType>;
pub type DFTime64NanosecondArray = DFArray<Time64NanosecondType>;

impl<T> DFArray<T> {
    pub fn array_data(&self) -> &ArrayData {
        self.array.data()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn data_type(&self) -> DataType {
        self.array.data_type().clone()
    }

    #[inline]
    pub fn get_array_memory_size(&self) -> usize {
        self.array.get_array_memory_size()
    }

    #[inline]
    pub fn clone_empty(&self) -> Self {
        Self {
            array: self.array.slice(0, 0),
            phantom: PhantomData,
        }
    }
}

impl<T> DFArray<T>
where T: DFNumericType
{
    pub fn downcast(&self) -> &PrimitiveArray<T> {
        let arr = &*self.array;
        unsafe { &*(arr as *const dyn Array as *const PrimitiveArray<T>) }
    }
}
