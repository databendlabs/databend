// Copyright 2021 Datafuse Labs.
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

use std::iter::Iterator;
use std::sync::Arc;

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;

use crate::prelude::*;

#[derive(Clone, Debug)]
pub enum DataColumn {
    // Array of values.
    Array(Series),
    // A Single value.
    Constant(DataValue, usize),
}

#[derive(Clone, Debug)]
pub enum DataColumnValidity {
    Array(Option<Bitmap>, usize),
    Constant(bool, usize),
}

impl DataColumnValidity {
    // Return whether the validity are all invalid, e.g., every value is null.
    #[inline(always)]
    pub fn all_null(&self) -> bool {
        match self {
            DataColumnValidity::Array(validity, _) => match validity {
                Some(bitmap) => bitmap.null_count() == bitmap.len(),
                None => false,
            },
            DataColumnValidity::Constant(valid, _) => !valid,
        }
    }

    // Return whether the validity are all valid, e.g., no null exists.
    #[inline(always)]
    pub fn all_valid(&self) -> bool {
        match self {
            DataColumnValidity::Array(validity, _) => match validity {
                Some(bitmap) => bitmap.null_count() == 0,
                None => true,
            },
            DataColumnValidity::Constant(valid, _) => *valid,
        }
    }

    // Keep the function here, mostly for error checking -- the validity's length should be the same as column length.
    #[allow(dead_code)]
    #[inline(always)]
    fn len(&self) -> usize {
        match self {
            DataColumnValidity::Array(_, size) => *size,
            DataColumnValidity::Constant(_, size) => *size,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DataColumnWithField {
    pub(crate) column: DataColumn,
    pub(crate) field: DataField,
}
impl DataColumnWithField {
    pub fn new(column: DataColumn, field: DataField) -> Self {
        Self { column, field }
    }
    pub fn column(&self) -> &DataColumn {
        &self.column
    }
    pub fn field(&self) -> &DataField {
        &self.field
    }
    pub fn data_type(&self) -> &DataType {
        self.field.data_type()
    }
}

pub type DataColumnsWithField = [DataColumnWithField];

impl DataColumn {
    #[inline]
    pub fn data_type(&self) -> DataType {
        match self {
            DataColumn::Array(array) => array.data_type().clone(),
            DataColumn::Constant(v, _) => v.data_type(),
        }
    }

    #[inline]
    pub fn physical_type(&self) -> PhysicalDataType {
        self.data_type().to_physical_type()
    }

    #[inline]
    pub fn to_array(&self) -> Result<Series> {
        match self {
            DataColumn::Array(array) => Ok(array.clone()),
            DataColumn::Constant(scalar, size) => scalar.to_series_with_size(*size),
        }
    }

    #[inline]
    pub fn to_values(&self) -> Result<Vec<DataValue>> {
        match self {
            DataColumn::Array(array) => array.to_values(),
            DataColumn::Constant(scalar, size) => scalar.to_values(*size),
        }
    }

    #[inline]
    pub fn get_array_ref(&self) -> Result<ArrayRef> {
        match self {
            DataColumn::Array(array) => Ok(array.get_array_ref()),
            DataColumn::Constant(scalar, size) => {
                Ok(scalar.to_series_with_size(*size)?.get_array_ref())
            }
        }
    }

    /// Return the minimal series, if it's constant value, it's size is 1.
    /// This could be useful when Constant <op> Constant
    /// Since our kernel is based on Array <op> Array
    /// 1. Constant -----> minimal Array; 2. Array <op> Array; 3. resize_constant
    #[inline]
    pub fn to_minimal_array(&self) -> Result<Series> {
        match self {
            DataColumn::Array(array) => Ok(array.clone()),
            DataColumn::Constant(scalar, _) => scalar.to_series_with_size(1),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            DataColumn::Array(array) => array.len(),
            DataColumn::Constant(_, size) => *size,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        match self {
            DataColumn::Array(array) => array.len() == 0,
            DataColumn::Constant(_, size) => *size == 0,
        }
    }

    #[inline]
    pub fn get_array_memory_size(&self) -> usize {
        match self {
            DataColumn::Array(array) => array.get_array_memory_size(),
            DataColumn::Constant(scalar, size) => scalar
                .to_series_with_size(*size)
                .map(|arr| arr.get_array_memory_size())
                .unwrap_or(0),
        }
    }

    #[inline]
    #[must_use]
    pub fn slice(&self, offset: usize, length: usize) -> DataColumn {
        match self {
            DataColumn::Array(array) => DataColumn::Array(array.slice(offset, length)),
            DataColumn::Constant(scalar, _) => DataColumn::Constant(scalar.clone(), length),
        }
    }

    #[inline]
    #[must_use]
    pub fn clone_empty(&self) -> DataColumn {
        match self {
            DataColumn::Array(array) => DataColumn::Array(array.slice(0, 0)),
            DataColumn::Constant(scalar, _) => DataColumn::Constant(scalar.clone(), 0),
        }
    }

    #[inline]
    pub fn cast_with_type(&self, data_type: &DataType) -> Result<DataColumn> {
        match self {
            DataColumn::Array(array) => Ok(DataColumn::Array(array.cast_with_type(data_type)?)),
            DataColumn::Constant(scalar, size) => {
                let array = scalar.to_series_with_size(1)?;
                let array = array.cast_with_type(data_type)?;

                let value = array.try_get(0)?;
                Ok(DataColumn::Constant(value, *size))
            }
        }
    }

    #[inline]
    #[must_use]
    pub fn resize_constant(&self, size: usize) -> Self {
        match self {
            DataColumn::Array(array) if array.len() == 1 => {
                let value = array.try_get(0).unwrap();
                DataColumn::Constant(value, size)
            }
            DataColumn::Constant(scalar, _) => DataColumn::Constant(scalar.clone(), size),
            _ => self.clone(),
        }
    }

    // Note: Don't call this inside a loop, it's slow.
    #[inline]
    pub fn try_get(&self, index: usize) -> Result<DataValue> {
        match self {
            DataColumn::Array(array) => Ok(array.try_get(index)?),
            DataColumn::Constant(scalar, _) => Ok(scalar.clone()),
        }
    }

    #[inline]
    pub fn serialize(&self, vec: &mut Vec<Vec<u8>>) -> Result<()> {
        let array = self.to_array()?;
        array.serialize(vec)
    }

    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub unsafe fn scatter_unchecked(
        &self,
        indices: &mut dyn Iterator<Item = u64>,
        scatter_size: usize,
    ) -> Result<Vec<DataColumn>> {
        match self {
            DataColumn::Array(array) => {
                let series = array.scatter_unchecked(indices, scatter_size)?;
                Ok(series.iter().map(|s| s.into()).collect())
            }
            DataColumn::Constant(scalar, _) => {
                let mut vs = vec![0; scatter_size];
                indices.for_each(|d| vs[d as usize] += 1);

                Ok(vs
                    .iter()
                    .map(|v| DataColumn::Constant(scalar.clone(), *v))
                    .collect())
            }
        }
    }

    #[inline]
    pub fn get_validity(&self) -> DataColumnValidity {
        let len = self.len();
        match self {
            DataColumn::Array(series) => match series.0.validity() {
                Some(bitmap) => DataColumnValidity::Array(Some(bitmap.clone()), len),
                None => DataColumnValidity::Array(None, len),
            },
            DataColumn::Constant(val, _) => {
                let valid = !val.is_null();
                DataColumnValidity::Constant(valid, len)
            }
        }
    }

    #[inline]
    pub fn apply_validities(
        self,
        validities_to_apply: &[DataColumnValidity],
    ) -> Result<DataColumn> {
        if validities_to_apply.is_empty() {
            return Ok(self);
        }

        // If the current state is all null, no need to do the masking.
        if self.get_validity().all_null() {
            return Ok(self);
        }

        let validity_to_apply = self.merge_validities(validities_to_apply)?;

        // 1. If the validity to apply is all valid, no need to do the masking.
        if validity_to_apply.all_valid() {
            return Ok(self);
        }

        // 2. If the validity to apply is all null, just need to return an constant type with null value.
        if validity_to_apply.all_null() {
            let data_type = self.data_type();
            let null_value = DataValue::new_from_data_type(&data_type, true);
            let column = DataColumn::Constant(null_value, self.len());
            return Ok(column);
        }

        // 3. Get the bitmap and do the masking.
        let bitmap_to_apply = match validity_to_apply {
            DataColumnValidity::Array(v, _) => v,

            // The validity is neither all-true nor all-false, it must NOT be a constant one.
            DataColumnValidity::Constant(_, _) => unreachable!(),
        };

        match self {
            DataColumn::Array(series) => {
                let current_bitmap: Option<&Bitmap> = series.validity();
                let bitmap_mask: Option<&Bitmap> = match &bitmap_to_apply {
                    Some(bitmap) => Some(bitmap),
                    None => None,
                };
                let new_bitmap = combine_validities(current_bitmap, bitmap_mask);
                let array = series.get_array_ref().with_validity(new_bitmap);
                let array_ref: ArrayRef = Arc::from(array);
                let col = DataColumn::Array(array_ref.into_series());
                Ok(col)
            }
            DataColumn::Constant(ref data_value, _) => {
                // If the current data column is all constant null, no need to apply any masking.
                if data_value.is_null() {
                    return Ok(self);
                }
                let array = self.get_array_ref()?.with_validity(bitmap_to_apply);
                let array_ref: ArrayRef = Arc::from(array);
                let col = DataColumn::Array(array_ref.into_series());
                Ok(col)
            }
        }
    }

    fn merge_validities(&self, validities: &[DataColumnValidity]) -> Result<DataColumnValidity> {
        let array_len = self.len();

        let mut bitmap_vec: Vec<&Option<Bitmap>> = vec![];

        for validity in validities.iter() {
            // 1. If we find one validity that is all null, the masked result must be all null.
            // Thus we just return a all-null validity.
            if validity.all_null() {
                return Ok(DataColumnValidity::Constant(false, array_len));
            }

            if let DataColumnValidity::Array(bitmap, _) = validity {
                bitmap_vec.push(bitmap)
            }
        }

        // 2. If we didn't find any array validity, that is, all of them are constant.
        // Also they must be all constant true after step 1.
        if bitmap_vec.is_empty() {
            return Ok(DataColumnValidity::Constant(true, array_len));
        }

        // 3. After excluding constants case, we merge all validity bitmap
        let mut bitmap = bitmap_vec[0].clone();
        for v in bitmap_vec.into_iter().skip(1) {
            bitmap = combine_validities_2(&bitmap, v);
        }

        Ok(DataColumnValidity::Array(bitmap, array_len))
    }
}

impl From<Series> for DataColumn {
    fn from(array: Series) -> Self {
        DataColumn::Array(array)
    }
}
impl From<&Series> for DataColumn {
    fn from(array: &Series) -> Self {
        DataColumn::Array(array.clone())
    }
}

impl<T> From<T> for DataColumn
where T: IntoSeries
{
    fn from(array: T) -> Self {
        DataColumn::Array(array.into_series())
    }
}
