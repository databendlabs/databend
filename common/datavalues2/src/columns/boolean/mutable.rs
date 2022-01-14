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

use common_arrow::arrow::bitmap::MutableBitmap;

use crate::columns::mutable::MutableColumn;
use crate::types::DataTypeBoolean;
use crate::types::DataTypePtr;
use crate::BooleanColumn;
use crate::ColumnRef;
use crate::NewColumn;

pub struct MutableBooleanColumn {
    values: MutableBitmap,
    data_type: DataTypePtr,
}

impl MutableColumn for MutableBooleanColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_column(&mut self) -> ColumnRef {
        todo!()
    }

    fn data_type(&self) -> DataTypePtr {
        self.data_type.clone()
    }

    fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit()
    }

    fn append_default(&mut self) {
        self.append_value(false);
    }
}

impl Default for MutableBooleanColumn {
    fn default() -> Self {
        Self::new()
    }
}

impl MutableBooleanColumn {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: MutableBitmap::with_capacity(capacity),
            data_type: DataTypeBoolean::arc(),
        }
    }

    pub fn from_data(values: MutableBitmap) -> Self {
        Self {
            values,
            data_type: DataTypeBoolean::arc(),
        }
    }

    #[inline]
    pub fn append_value(&mut self, value: bool) {
        self.values.push(value);
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    pub fn finish(&mut self) -> BooleanColumn {
        self.shrink_to_fit();
        BooleanColumn {
            values: std::mem::take(&mut self.values).into(),
        }
    }
}

impl NewColumn<bool> for BooleanColumn {
    fn new_from_slice<P: AsRef<[bool]>>(slice: P) -> Self {
        let bitmap = MutableBitmap::from_iter(slice.as_ref().iter().cloned());
        BooleanColumn {
            values: bitmap.into(),
        }
    }

    fn new_from_iter(it: impl Iterator<Item = bool>) -> Self {
        let bitmap = MutableBitmap::from_iter(it);
        BooleanColumn {
            values: bitmap.into(),
        }
    }
}
