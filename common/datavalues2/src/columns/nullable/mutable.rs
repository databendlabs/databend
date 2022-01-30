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
use common_exception::Result;

use crate::columns::mutable::MutableColumn;
use crate::types::DataTypePtr;
use crate::ColumnRef;
use crate::NullableColumn;

pub struct MutableNullableColumn {
    values: MutableBitmap,
    inner: Box<dyn MutableColumn>,
    data_type: DataTypePtr,
}

impl MutableColumn for MutableNullableColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataTypePtr {
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
        Arc::new(NullableColumn::new(col, validity.into()))
    }

    fn append_data_value(&mut self, value: crate::DataValue) -> Result<()> {
        self.values.push(true);
        self.inner.append_data_value(value)
    }
}

impl MutableNullableColumn {
    pub fn new(inner: Box<dyn MutableColumn>, data_type: DataTypePtr) -> Self {
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
