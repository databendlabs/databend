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

use common_arrow::arrow::bitmap::MutableBitmap;

use crate::ColumnRef;
use crate::DataTypeNullable;
use crate::DataTypePtr;
use crate::MutableColumn;
use crate::NullableColumn;

pub struct MutableNullableColumn {
    bitmap: MutableBitmap,
    values: Box<dyn MutableColumn>,
    data_type: DataTypePtr,
}

impl MutableNullableColumn {
    pub fn new(values: Box<dyn MutableColumn>) -> Self {
        let inner_type = values.data_type();
        Self {
            bitmap: MutableBitmap::with_capacity(0),
            values,
            data_type: Arc::new(DataTypeNullable::create(inner_type)),
        }
    }
}

impl MutableColumn for MutableNullableColumn {
    fn data_type(&self) -> DataTypePtr {
        self.data_type.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn as_column(&mut self) -> ColumnRef {
        let column = self.values.as_column();
        let bitmap = std::mem::take(&mut self.bitmap);
        Arc::new(NullableColumn::new(column, bitmap.into()))
    }

    #[inline]
    fn append_default(&mut self) {
        self.bitmap.push(true);
        self.values.append_default();
    }

    #[inline]
    fn append_null(&mut self) -> bool {
        self.append_default();
        true
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        Some(&self.bitmap)
    }

    fn shrink_to_fit(&mut self) {
        self.bitmap.shrink_to_fit();
    }
}
