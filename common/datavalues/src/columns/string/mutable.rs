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

use std::sync::Arc;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;

pub struct MutableStringColumn {
    last_size: usize,
    offsets: Vec<i64>,
    values: Vec<u8>,
}

impl MutableStringColumn {
    pub fn from_data(values: Vec<u8>, offsets: Vec<i64>) -> Self {
        Self {
            last_size: *offsets.last().unwrap() as usize,
            offsets,
            values,
        }
    }

    #[inline]
    pub fn append_value(&mut self, v: impl AsRef<[u8]>) {
        let bytes = v.as_ref();
        self.add_offset(bytes.len());
        self.values.extend_from_slice(bytes);
    }

    pub fn pop_value(&mut self) -> Option<Vec<u8>> {
        (self.offsets.len() > 1).then(|| {
            let _ = self.offsets.pop();
            self.last_size = self.offsets.last().cloned().unwrap_or_default() as usize;
            self.values.split_off(self.last_size)
        })
    }

    pub fn with_values_capacity(values_capacity: usize, capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(0);

        Self {
            last_size: 0,
            offsets,
            values: Vec::with_capacity(values_capacity),
        }
    }

    pub fn values_mut(&mut self) -> &mut Vec<u8> {
        &mut self.values
    }

    pub fn offsets_mut(&mut self) -> &mut Vec<i64> {
        &mut self.offsets
    }

    #[inline]
    pub fn add_offset(&mut self, offset: usize) {
        self.last_size += offset;
        self.offsets.push(self.last_size as i64);
    }

    pub fn pop_offset(&mut self) -> Option<usize> {
        self.offsets.pop().map(|offset| offset as usize)
    }
}

impl Default for MutableStringColumn {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl MutableColumn for MutableStringColumn {
    fn data_type(&self) -> DataTypeImpl {
        StringType::new_impl()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn append_default(&mut self) {
        self.append_value("");
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        None
    }

    fn shrink_to_fit(&mut self) {
        self.offsets.shrink_to_fit();
        self.values.shrink_to_fit();
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn to_column(&mut self) -> ColumnRef {
        Arc::new(self.finish())
    }

    fn append_data_value(&mut self, value: DataValue) -> Result<()> {
        self.append_value(value.as_string()?);
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<DataValue> {
        self.pop_value().map(DataValue::from).ok_or_else(|| {
            ErrorCode::BadDataArrayLength("String column array is empty when pop data value")
        })
    }
}

impl ScalarColumnBuilder for MutableStringColumn {
    type ColumnType = StringColumn;

    fn with_capacity(capacity: usize) -> Self {
        Self::with_values_capacity(capacity * 3, capacity)
    }

    fn push(&mut self, value: &[u8]) {
        self.add_offset(value.len());
        self.values.extend_from_slice(value);
    }

    fn finish(&mut self) -> Self::ColumnType {
        self.shrink_to_fit();
        unsafe {
            let column = StringColumn::from_data_unchecked(
                std::mem::take(&mut self.offsets).into(),
                std::mem::take(&mut self.values).into(),
            );
            self.offsets.push(0);
            column
        }
    }
}
