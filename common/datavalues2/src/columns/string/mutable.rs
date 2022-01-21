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

use crate::prelude::*;

pub struct MutableStringColumn {
    last_size: usize,
    offsets: Vec<i64>,
    values: Vec<u8>,
}

impl MutableStringColumn {
    #[inline]
    pub fn append_value(&mut self, v: impl AsRef<[u8]>) {
        let bytes = v.as_ref();
        self.last_size += bytes.len();
        self.offsets.push(self.last_size as i64);
        self.values.extend_from_slice(bytes);
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
}

impl Default for MutableStringColumn {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl<'a> MutableColumn<&'a [u8], StringColumn> for MutableStringColumn {
    fn data_type(&self) -> DataTypePtr {
        StringType::arc()
    }

    fn with_capacity(capacity: usize) -> Self {
        Self::with_values_capacity(capacity * 3, capacity)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn finish(&mut self) -> StringColumn {
        self.last_size = 0;
        unsafe {
            StringColumn::from_data_unchecked(
                std::mem::take(&mut self.offsets).into(),
                std::mem::take(&mut self.values).into(),
            )
        }
    }

    fn append_default(&mut self) {
        self.append_value("");
    }

    fn validity(&self) -> Option<&common_arrow::arrow::bitmap::MutableBitmap> {
        None
    }

    fn shrink_to_fit(&mut self) {
        self.offsets.shrink_to_fit();
        self.values.shrink_to_fit();
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn append(&mut self, item: &'a [u8]) {
        self.append_value(item);
    }
}
