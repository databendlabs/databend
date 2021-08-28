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
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::compute::aggregate;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct DFNullArray {
    pub(crate) array: NullArray,
}

impl From<NullArray> for DFNullArray {
    fn from(array: NullArray) -> Self {
        Self::new(array)
    }
}

impl DFNullArray {
    pub fn new(array: NullArray) -> Self {
        Self { array }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        Self::new(array.as_any().downcast_ref::<NullArray>().unwrap().clone())
    }
    pub fn data_type(&self) -> &DataType {
        &DataType::Null
    }

    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.
    pub unsafe fn try_get(&self, _index: usize) -> Result<DataValue> {
        Ok(DataValue::Null)
    }

    pub fn get_inner(&self) -> &NullArray {
        &self.array
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn null_count(&self) -> usize {
        self.array.null_count()
    }

    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        self.array.is_null(i)
    }

    /// Take a view of top n elements
    pub fn limit(&self, num_elements: usize) -> Self {
        self.slice(0, num_elements)
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let array = self.array.slice(offset, length);
        Self::new(array)
    }

    /// Unpack a array to the same physical type.
    ///
    /// # Safety
    ///
    /// This is unsafe as the data_type may be uncorrect and
    /// is assumed to be correct in other unsafe code.
    pub unsafe fn unpack(&self, array: &Series) -> Result<&Self> {
        let array_trait = &**array;
        if self.data_type() == array.data_type() {
            let ca = &*(array_trait as *const dyn SeriesTrait as *const Self);
            Ok(ca)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "cannot unpack array {:?} into matching type {:?}",
                array,
                self.data_type()
            )))
        }
    }
}

#[derive(Debug, Clone)]
pub struct DFBooleanArray {
    pub(crate) array: BooleanArray,
}

impl From<BooleanArray> for DFBooleanArray {
    fn from(array: BooleanArray) -> Self {
        Self { array }
    }
}

impl DFBooleanArray {
    pub fn new(array: BooleanArray) -> Self {
        Self { array }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        Self::new(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .clone(),
        )
    }

    pub fn get_inner(&self) -> &BooleanArray {
        &self.array
    }

    pub fn data_type(&self) -> &DataType {
        &DataType::Boolean
    }

    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.
    pub unsafe fn try_get(&self, index: usize) -> Result<DataValue> {
        let v = match self.array.is_null(index) {
            true => None,
            false => Some(self.array.value_unchecked(index)),
        };
        Ok(v.into())
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn null_count(&self) -> usize {
        self.array.null_count()
    }
    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        self.array.is_null(i)
    }

    /// Take a view of top n elements
    pub fn limit(&self, num_elements: usize) -> Self {
        self.slice(0, num_elements)
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let array = self.array.slice(offset, length);
        Self::new(array)
    }

    /// Unpack a array to the same physical type.
    ///
    /// # Safety
    ///
    /// This is unsafe as the data_type may be uncorrect and
    /// is assumed to be correct in other unsafe code.
    pub unsafe fn unpack(&self, array: &Series) -> Result<&Self> {
        let array_trait = &**array;
        if self.data_type() == array.data_type() {
            let ca = &*(array_trait as *const dyn SeriesTrait as *const Self);
            Ok(ca)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "cannot unpack array {:?} into matching type {:?}",
                array,
                self.data_type()
            )))
        }
    }

    #[inline]
    pub fn all_is_null(&self) -> bool {
        self.null_count() == self.len()
    }

    #[inline]
    pub fn get_array_ref(&self) -> ArrayRef {
        Arc::new(self.array.clone()) as ArrayRef
    }

    #[inline]
    /// Get the null count and the buffer of bits representing null values
    pub fn null_bits(&self) -> (usize, &Option<Bitmap>) {
        (self.array.null_count(), self.array.validity())
    }

    pub fn get_array_memory_size(&self) -> usize {
        aggregate::estimated_bytes_size(&self.array)
    }
}

#[derive(Debug, Clone)]
pub struct DFUtf8Array {
    pub(crate) array: LargeUtf8Array,
}

impl From<LargeUtf8Array> for DFUtf8Array {
    fn from(array: LargeUtf8Array) -> Self {
        Self { array }
    }
}

impl DFUtf8Array {
    pub fn new(array: LargeUtf8Array) -> Self {
        Self { array }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        Self::new(
            array
                .as_any()
                .downcast_ref::<LargeUtf8Array>()
                .unwrap()
                .clone(),
        )
    }

    pub fn get_inner(&self) -> &LargeUtf8Array {
        &self.array
    }

    pub fn data_type(&self) -> &DataType {
        &DataType::Utf8
    }

    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.
    pub unsafe fn try_get(&self, index: usize) -> Result<DataValue> {
        let v = match self.array.is_null(index) {
            true => None,
            false => Some(self.array.value_unchecked(index)),
        };
        Ok(v.into())
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn null_count(&self) -> usize {
        self.array.null_count()
    }

    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        self.array.is_null(i)
    }

    #[inline]
    pub fn all_is_null(&self) -> bool {
        self.null_count() == self.len()
    }

    #[inline]
    /// Get the null count and the buffer of bits representing null values
    pub fn null_bits(&self) -> (usize, &Option<Bitmap>) {
        (self.array.null_count(), self.array.validity())
    }

    /// Take a view of top n elements
    pub fn limit(&self, num_elements: usize) -> Self {
        self.slice(0, num_elements)
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let array = self.array.slice(offset, length);
        Self::new(array)
    }

    /// Unpack a array to the same physical type.
    ///
    /// # Safety
    ///
    /// This is unsafe as the data_type may be uncorrect and
    /// is assumed to be correct in other unsafe code.
    pub unsafe fn unpack(&self, array: &Series) -> Result<&Self> {
        let array_trait = &**array;
        if self.data_type() == array.data_type() {
            let ca = &*(array_trait as *const dyn SeriesTrait as *const Self);
            Ok(ca)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "cannot unpack array {:?} into matching type {:?}",
                array,
                self.data_type()
            )))
        }
    }
}

#[derive(Debug, Clone)]
pub struct DFBinaryArray {
    pub(crate) array: LargeBinaryArray,
}

impl From<LargeBinaryArray> for DFBinaryArray {
    fn from(array: LargeBinaryArray) -> Self {
        Self { array }
    }
}

impl DFBinaryArray {
    pub fn new(array: LargeBinaryArray) -> Self {
        Self { array }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        Self::new(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .clone(),
        )
    }

    pub fn get_inner(&self) -> &LargeBinaryArray {
        &self.array
    }

    pub fn data_type(&self) -> &DataType {
        &DataType::Binary
    }

    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.
    pub unsafe fn try_get(&self, index: usize) -> Result<DataValue> {
        let v = match self.array.is_null(index) {
            true => None,
            false => Some(self.array.value_unchecked(index).to_owned()),
        };
        Ok(v.into())
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn null_count(&self) -> usize {
        self.array.null_count()
    }

    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        self.array.is_null(i)
    }

    #[inline]
    /// Get the null count and the buffer of bits representing null values
    pub fn null_bits(&self) -> (usize, &Option<Bitmap>) {
        (self.array.null_count(), self.array.validity())
    }

    /// Take a view of top n elements
    pub fn limit(&self, num_elements: usize) -> Self {
        self.slice(0, num_elements)
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let array = self.array.slice(offset, length);
        Self::new(array)
    }

    /// Unpack a array to the same physical type.
    ///
    /// # Safety
    ///
    /// This is unsafe as the data_type may be uncorrect and
    /// is assumed to be correct in other unsafe code.
    pub unsafe fn unpack(&self, array: &Series) -> Result<&Self> {
        let array_trait = &**array;
        if self.data_type() == array.data_type() {
            let ca = &*(array_trait as *const dyn SeriesTrait as *const Self);
            Ok(ca)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "cannot unpack array {:?} into matching type {:?}",
                array,
                self.data_type()
            )))
        }
    }
}
#[derive(Debug, Clone)]
pub struct DFListArray {
    pub(crate) array: LargeListArray,
    pub data_type: DataType,
}

impl DFListArray {
    pub fn new(array: LargeListArray) -> Self {
        let data_type = array.data_type().into();

        Self { array, data_type }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        Self::new(
            array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap()
                .clone(),
        )
    }

    pub fn get_inner(&self) -> &LargeListArray {
        &self.array
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.
    pub unsafe fn try_get(&self, index: usize) -> Result<DataValue> {
        let v = match self.array.is_null(index) {
            true => None,
            false => {
                let netesed = self.array.value_unchecked(index);
                let netesed: ArrayRef = Arc::from(netesed);
                let netesed = netesed.into_series();
                let mut v = Vec::with_capacity(netesed.len());
                for i in 0..netesed.len() {
                    v.push(netesed.try_get(i)?);
                }
                Some(v)
            }
        };
        Ok(DataValue::List(v, self.sub_data_type().clone()))
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn null_count(&self) -> usize {
        self.array.null_count()
    }

    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        self.array.is_null(i)
    }

    /// Take a view of top n elements
    pub fn limit(&self, num_elements: usize) -> Self {
        self.slice(0, num_elements)
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let array = self.array.slice(offset, length);
        Self::new(array)
    }

    /// Unpack a array to the same physical type.
    ///
    /// # Safety
    ///
    /// This is unsafe as the data_type may be uncorrect and
    /// is assumed to be correct in other unsafe code.
    pub unsafe fn unpack(&self, array: &Series) -> Result<&Self> {
        let array_trait = &**array;
        if self.data_type() == array.data_type() {
            let ca = &*(array_trait as *const dyn SeriesTrait as *const Self);
            Ok(ca)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "cannot unpack array {:?} into matching type {:?}",
                array,
                self.data_type()
            )))
        }
    }

    pub fn sub_data_type(&self) -> &DataType {
        match self.data_type() {
            DataType::List(sub_types) => sub_types.data_type(),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DFStructArray {
    pub(crate) array: StructArray,
    pub data_type: DataType,
}

impl From<StructArray> for DFStructArray {
    fn from(array: StructArray) -> Self {
        Self::new(array)
    }
}

impl DFStructArray {
    pub fn new(array: StructArray) -> Self {
        let data_type = array.data_type().into();
        Self { array, data_type }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        Self::new(
            array
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .clone(),
        )
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.
    pub unsafe fn try_get(&self, index: usize) -> Result<DataValue> {
        let nested_array = self.array.values()[index].clone();
        let series = nested_array.into_series();
        let scalar_vec = (0..series.len())
            .map(|i| series.try_get(i))
            .collect::<Result<Vec<_>>>()?;
        Ok(DataValue::Struct(scalar_vec))
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn null_count(&self) -> usize {
        self.array.null_count()
    }

    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        self.array.is_null(i)
    }

    /// Take a view of top n elements
    pub fn limit(&self, num_elements: usize) -> Self {
        self.slice(0, num_elements)
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let array = self.array.slice(offset, length);
        Self::new(array)
    }

    /// Unpack a array to the same physical type.
    ///
    /// # Safety
    ///
    /// This is unsafe as the data_type may be uncorrect and
    /// is assumed to be correct in other unsafe code.
    pub unsafe fn unpack(&self, array: &Series) -> Result<&Self> {
        let array_trait = &**array;
        if self.data_type() == array.data_type() {
            let ca = &*(array_trait as *const dyn SeriesTrait as *const Self);
            Ok(ca)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "cannot unpack array {:?} into matching type {:?}",
                array,
                self.data_type()
            )))
        }
    }
}
