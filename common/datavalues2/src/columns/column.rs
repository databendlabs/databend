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

use std::any::Any;
use std::sync::Arc;

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::Result;

use crate::prelude::*;
use crate::BooleanColumn;
use crate::DataTypePtr;
use crate::DataValue;
use crate::NullColumn;
use crate::TypeID;

pub type ColumnRef = Arc<dyn Column>;
pub trait Column: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    /// Type of data that column contains. It's an underlying physical type:
    /// UInt16 for Date, UInt32 for DateTime, so on.
    fn data_type_id(&self) -> TypeID {
        self.data_type().data_type_id()
    }
    fn data_type(&self) -> DataTypePtr;

    fn is_nullable(&self) -> bool {
        false
    }

    fn is_const(&self) -> bool {
        false
    }

    fn len(&self) -> usize;
    /// whether the array is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn null_at(&self, _row: usize) -> bool {
        false
    }

    /// If the only value column can contain is NULL.
    fn only_null(&self) -> bool {
        false
    }

    /// Returns (is_all_null,  Option bitmap)
    fn validity(&self) -> (bool, Option<&Bitmap>) {
        (false, None)
    }

    fn memory_size(&self) -> usize;
    fn as_arrow_array(&self) -> ArrayRef;
    fn slice(&self, offset: usize, length: usize) -> ColumnRef;

    // Copies each element according offsets parameter.
    // (i-th element should be copied offsets[i] - offsets[i - 1] times.)
    fn replicate(&self, offsets: &[usize]) -> ColumnRef;

    fn convert_full_column(&self) -> ColumnRef;

    /// # Safety
    /// Assumes that the `index` is smaller than size.
    fn get(&self, index: usize) -> DataValue;

    /// # Safety
    /// Assumes that the `index` is smaller than size.
    fn get_u64(&self, index: usize) -> Result<u64> {
        let value = self.get(index);
        DFTryFrom::try_from(&value)
    }

    /// # Safety
    /// Assumes that the `index` is smaller than size.
    fn get_i64(&self, index: usize) -> Result<i64> {
        let value = self.get(index);
        DFTryFrom::try_from(&value)
    }

    /// # Safety
    /// Assumes that the `index` is smaller than size.
    fn get_string(&self, index: usize) -> Result<Vec<u8>> {
        let value = self.get(index);
        DFTryFrom::try_from(value)
    }
}

pub trait IntoColumn {
    fn into_column(self) -> ColumnRef;
    fn into_nullable_column(self) -> ColumnRef;
}

// No nullable
// We should wrap the nullable by ourselves
impl IntoColumn for ArrayRef {
    fn into_column(self) -> ColumnRef {
        use TypeID::*;
        let data_type: DataTypePtr = from_arrow_type(self.data_type());
        match data_type.data_type_id() {
            // arrow type has no nullable type
            Nullable => unimplemented!(),
            Null => Arc::new(NullColumn::from_arrow_array(self.as_ref())),
            Boolean => Arc::new(BooleanColumn::from_arrow_array(self.as_ref())),
            UInt8 => Arc::new(UInt8Column::from_arrow_array(self.as_ref())),
            UInt16 | Date16 => Arc::new(UInt16Column::from_arrow_array(self.as_ref())),
            UInt32 | DateTime32 => Arc::new(UInt32Column::from_arrow_array(self.as_ref())),
            UInt64 | DateTime64 => Arc::new(UInt64Column::from_arrow_array(self.as_ref())),

            Int8 => Arc::new(Int8Column::from_arrow_array(self.as_ref())),
            Int16 => Arc::new(Int16Column::from_arrow_array(self.as_ref())),
            Int32 | Date32 => Arc::new(Int32Column::from_arrow_array(self.as_ref())),
            Int64 | Interval => Arc::new(Int64Column::from_arrow_array(self.as_ref())),

            Float32 => Arc::new(Float32Column::from_arrow_array(self.as_ref())),
            Float64 => Arc::new(Float64Column::from_arrow_array(self.as_ref())),

            Array => Arc::new(ArrayColumn::from_arrow_array(self.as_ref())),
            Struct => Arc::new(StructColumn::from_arrow_array(self.as_ref())),
            String => Arc::new(StringColumn::from_arrow_array(self.as_ref())),
        }
    }

    fn into_nullable_column(self) -> ColumnRef {
        let size = self.len();
        let validity = self.validity().cloned();
        let column = self.into_column();
        Arc::new(NullableColumn::new(
            column,
            validity.unwrap_or_else(|| {
                let mut bm = MutableBitmap::with_capacity(size);
                bm.extend_constant(size, false);
                Bitmap::from(bm)
            }),
        ))
    }
}

impl std::fmt::Debug for dyn Column + '_ {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let array = self.as_arrow_array();
        std::fmt::Debug::fmt(&array, f)
    }
}
