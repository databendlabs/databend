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
use crate::types::DataTypeImpl;
use crate::ColumnRef;
use crate::DataValue;
use crate::NullColumn;
use crate::NullType;

#[derive(Debug, Default)]
pub struct MutableNullColumn {
    length: usize,
}

impl MutableColumn for MutableNullColumn {
    fn data_type(&self) -> DataTypeImpl {
        DataTypeImpl::Null(NullType {})
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn append_default(&mut self) {
        self.length += 1;
    }

    fn shrink_to_fit(&mut self) {}

    fn validity(&self) -> Option<&MutableBitmap> {
        None
    }

    fn len(&self) -> usize {
        self.length
    }

    fn to_column(&mut self) -> ColumnRef {
        let ret: ColumnRef = Arc::new(NullColumn {
            length: self.length,
        });
        self.length = 0;
        ret
    }

    fn append_data_value(&mut self, _value: crate::DataValue) -> Result<()> {
        self.length += 1;
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<DataValue> {
        (self.length > 0)
            .then(|| {
                self.length -= 1;
                DataValue::Null
            })
            .ok_or_else(|| {
                ErrorCode::BadDataArrayLength("Null column is empty when pop data value")
            })
    }
}
