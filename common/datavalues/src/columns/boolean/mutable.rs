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

use common_arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::columns::mutable::MutableColumn;
use crate::types::BooleanType;
use crate::types::DataTypeImpl;
use crate::BooleanColumn;
use crate::ColumnRef;
use crate::DataValue;
use crate::ScalarColumnBuilder;

pub struct MutableBooleanColumn {
    pub(crate) values: MutableBitmap,
    data_type: DataTypeImpl,
}

impl MutableColumn for MutableBooleanColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataTypeImpl {
        self.data_type.clone()
    }

    fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit()
    }

    fn append_default(&mut self) {
        self.append_value(false);
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn to_column(&mut self) -> ColumnRef {
        Arc::new(self.finish())
    }

    fn append_data_value(&mut self, value: DataValue) -> Result<()> {
        self.append_value(value.as_bool()?);
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<DataValue> {
        self.pop_value().map(DataValue::Boolean).ok_or_else(|| {
            ErrorCode::BadDataArrayLength("Bool column is empty when pop data value")
        })
    }
}

impl Default for MutableBooleanColumn {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl MutableBooleanColumn {
    pub fn from_data(values: MutableBitmap) -> Self {
        Self {
            values,
            data_type: BooleanType::arc(),
        }
    }

    #[inline]
    pub fn append_value(&mut self, value: bool) {
        self.values.push(value);
    }

    #[inline]
    pub fn pop_value(&mut self) -> Option<bool> {
        self.values.pop()
    }
}

impl ScalarColumnBuilder for MutableBooleanColumn {
    type ColumnType = BooleanColumn;

    fn with_capacity(capacity: usize) -> Self {
        Self {
            values: MutableBitmap::with_capacity(capacity),
            data_type: BooleanType::arc(),
        }
    }

    fn push(&mut self, value: bool) {
        self.values.push(value);
    }

    fn finish(&mut self) -> Self::ColumnType {
        self.shrink_to_fit();
        BooleanColumn {
            values: std::mem::take(&mut self.values).into(),
        }
    }
}
