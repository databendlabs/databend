// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::BinaryArray;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::ListArray;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::array::StringArray;
use common_arrow::arrow::array::StructArray;

use crate::arrays::DataArray;
use crate::series::IntoSeries;
use crate::series::Series;
use crate::DFBinaryArray;
use crate::DFBooleanArray;
use crate::DFListArray;
use crate::DFPrimitiveType;
use crate::DFStructArray;
use crate::DFUtf8Array;

impl<T> AsRef<PrimitiveArray<T>> for DataArray<T>
where T: DFPrimitiveType
{
    fn as_ref(&self) -> &PrimitiveArray<T> {
        self.downcast_ref()
    }
}

impl<T> DataArray<T>
where T: DFPrimitiveType
{
    pub fn downcast_ref(&self) -> &PrimitiveArray<T> {
        let arr = &*self.array;
        unsafe { &*(arr as *const dyn Array as *const PrimitiveArray<T>) }
    }

    pub fn downcast_iter(&self) -> impl Iterator<Item = Option<T::Native>> + DoubleEndedIterator {
        let arr = &*self.array;
        let arr = unsafe { &*(arr as *const dyn Array as *const PrimitiveArray<T>) };
        arr.iter()
    }

    pub fn collect_values(&self) -> Vec<Option<T::Native>> {
        self.downcast_iter().collect()
    }

    pub fn from_arrow_array(array: PrimitiveArray<T>) -> Self {
        let array_ref = Arc::new(array) as ArrayRef;
        array_ref.into()
    }
}

impl AsRef<BooleanArray> for DFBooleanArray {
    fn as_ref(&self) -> &BooleanArray {
        self.downcast_ref()
    }
}

impl DFBooleanArray {
    pub fn downcast_ref(&self) -> &BooleanArray {
        let arr = &*self.array;
        unsafe { &*(arr as *const dyn Array as *const BooleanArray) }
    }

    pub fn downcast_iter(&self) -> impl Iterator<Item = Option<bool>> + DoubleEndedIterator {
        let arr = &*self.array;
        let arr = unsafe { &*(arr as *const dyn Array as *const BooleanArray) };
        arr.iter()
    }

    pub fn collect_values(&self) -> Vec<Option<bool>> {
        self.downcast_iter().collect()
    }

    pub fn from_arrow_array(array: BooleanArray) -> Self {
        let array_ref = Arc::new(array) as ArrayRef;
        array_ref.into()
    }
}

impl AsRef<StringArray> for DFUtf8Array {
    fn as_ref(&self) -> &StringArray {
        self.downcast_ref()
    }
}

impl DFUtf8Array {
    pub fn downcast_ref(&self) -> &StringArray {
        let arr = &*self.array;
        unsafe { &*(arr as *const dyn Array as *const StringArray) }
    }

    pub fn downcast_iter<'a>(&self) -> impl Iterator<Item = Option<&'a str>> + DoubleEndedIterator {
        let arr = &*self.array;
        let arr = unsafe { &*(arr as *const dyn Array as *const StringArray) };
        arr.iter()
    }

    pub fn collect_values<'a>(&self) -> Vec<Option<&'a str>> {
        self.downcast_iter().collect()
    }

    pub fn from_arrow_array(array: StringArray) -> Self {
        let array_ref = Arc::new(array) as ArrayRef;
        array_ref.into()
    }
}

impl AsRef<ListArray> for DFListArray {
    fn as_ref(&self) -> &ListArray {
        self.downcast_ref()
    }
}

impl DFListArray {
    pub fn downcast_ref(&self) -> &ListArray {
        let arr = &*self.array;
        unsafe { &*(arr as *const dyn Array as *const ListArray) }
    }

    pub fn downcast_iter(&self) -> impl Iterator<Item = Option<Series>> + DoubleEndedIterator {
        let arr = &*self.array;
        let arr = unsafe { &*(arr as *const dyn Array as *const ListArray) };

        arr.iter().map(|a| a.map(|a| a.into_series()))
    }

    pub fn from_arrow_array(array: ListArray) -> Self {
        let array_ref = Arc::new(array) as ArrayRef;
        array_ref.into()
    }
}

impl AsRef<BinaryArray> for DFBinaryArray {
    fn as_ref(&self) -> &BinaryArray {
        self.downcast_ref()
    }
}

impl DFBinaryArray {
    pub fn downcast_ref(&self) -> &BinaryArray {
        let arr = &*self.array;
        unsafe { &*(arr as *const dyn Array as *const BinaryArray) }
    }

    pub fn from_arrow_array(array: BinaryArray) -> Self {
        let array_ref = Arc::new(array) as ArrayRef;
        array_ref.into()
    }
}

impl AsRef<StructArray> for DFStructArray {
    fn as_ref(&self) -> &StructArray {
        self.downcast_ref()
    }
}
impl DFStructArray {
    pub fn downcast_ref(&self) -> &StructArray {
        let arr = &*self.array;
        unsafe { &*(arr as *const dyn Array as *const StructArray) }
    }

    pub fn from_arrow_array(array: StructArray) -> Self {
        let array_ref = Arc::new(array) as ArrayRef;
        array_ref.into()
    }
}
