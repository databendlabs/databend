// Copyright 2020 Datafuse Labs.
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

use common_arrow::arrow::array::*;
use common_exception::Result;
use common_io::prelude::*;

use crate::prelude::*;
use crate::utils::get_iter_capacity;

pub struct BooleanArrayBuilder {
    builder: MutableBooleanArray,
}

impl ArrayBuilder<bool, DFBooleanArray> for BooleanArrayBuilder {
    /// Appends a value of type `T` into the builder
    #[inline]
    fn append_value(&mut self, v: bool) {
        self.builder.push(Some(v))
    }

    /// Appends a null slot into the builder
    #[inline]
    fn append_null(&mut self) {
        self.builder.push_null();
    }

    fn finish(&mut self) -> DFBooleanArray {
        let array = self.builder.as_arc();
        DFBooleanArray::from_arrow_array(array.as_ref())
    }
}

impl ArrayDeserializer for BooleanArrayBuilder {
    fn de(&mut self, reader: &mut &[u8]) -> Result<()> {
        let value: bool = reader.read_scalar()?;
        self.append_value(value);
        Ok(())
    }

    fn de_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let value: bool = reader.read_scalar()?;
            self.append_value(value);
        }

        Ok(())
    }

    fn finish_to_series(&mut self) -> Series {
        self.finish().into_series()
    }

    fn de_text(&mut self, reader: &[u8]) {
        let v = if reader.eq_ignore_ascii_case(b"false") {
            Some(false)
        } else if reader.eq_ignore_ascii_case(b"true") {
            Some(true)
        } else {
            None
        };
        self.append_option(v);
    }

    fn de_null(&mut self) {
        self.append_null()
    }
}

impl BooleanArrayBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        BooleanArrayBuilder {
            builder: MutableBooleanArray::with_capacity(capacity),
        }
    }
}

impl NewDataArray<bool> for DFBooleanArray {
    fn new_from_slice(v: &[bool]) -> Self {
        Self::new_from_iter(v.iter().copied())
    }

    fn new_from_opt_slice(opt_v: &[Option<bool>]) -> Self {
        Self::new_from_opt_iter(opt_v.iter().copied())
    }

    fn new_from_opt_iter(it: impl Iterator<Item = Option<bool>>) -> DFBooleanArray {
        let mut builder = BooleanArrayBuilder::with_capacity(get_iter_capacity(&it));
        it.for_each(|opt| builder.append_option(opt));
        builder.finish()
    }

    /// Create a new DataArray from an iterator.
    fn new_from_iter(it: impl Iterator<Item = bool>) -> DFBooleanArray {
        it.collect()
    }
}
