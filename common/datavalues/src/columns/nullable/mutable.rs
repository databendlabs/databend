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

use common_arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::columns::mutable::MutableColumn;
use crate::types::DataTypeImpl;
use crate::ColumnRef;
use crate::DataValue;
use crate::NullableColumn;

pub struct MutableNullableColumn {
    values: MutableBitmap,
    inner: Box<dyn MutableColumn>,
    data_type: DataTypeImpl,
}

impl MutableColumn for MutableNullableColumn {
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
        self.inner.shrink_to_fit()
    }

    fn append_default(&mut self) {
        self.values.push(false);
        self.inner.append_default();
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn to_column(&mut self) -> ColumnRef {
        let col = self.inner.to_column();
        let validity = std::mem::take(&mut self.values);
        NullableColumn::wrap_inner(col, Some(validity.into()))
    }

    fn append_data_value(&mut self, value: DataValue) -> Result<()> {
        self.values.push(true);
        self.inner.append_data_value(value)
    }

    /// Note when the last value is null, this method will return DataValue::Null.
    fn pop_data_value(&mut self) -> Result<DataValue> {
        self.values
            .pop()
            .ok_or_else(|| {
                ErrorCode::BadDataArrayLength("Nullable column array is empty when pop data value")
            })
            .and_then(|v| {
                let value = self.inner.pop_data_value();
                v.then_some(value).unwrap_or(Ok(DataValue::Null))
            })
    }
}

impl MutableNullableColumn {
    pub fn new(inner: Box<dyn MutableColumn>, data_type: DataTypeImpl) -> Self {
        Self {
            inner,
            values: MutableBitmap::with_capacity(0),
            data_type,
        }
    }

    #[inline]
    pub fn append_value(&mut self, value: bool) {
        self.values.push(value);
    }

    pub fn inner_mut(&mut self) -> &mut Box<dyn MutableColumn> {
        &mut self.inner
    }

    pub fn validity_mut(&mut self) -> &mut MutableBitmap {
        &mut self.values
    }
}
