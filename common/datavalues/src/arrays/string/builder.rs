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

use std::io::Read;

use common_arrow::arrow::array::*;
use common_exception::Result;
use common_io::prelude::BinaryRead;

use crate::prelude::*;
use crate::utils::get_iter_capacity;

pub struct StringArrayBuilder {
    builder: MutableBinaryArray<i64>,
}

impl StringArrayBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: MutableBinaryArray::<i64>::with_capacity(capacity),
        }
    }

    pub fn append_value(&mut self, value: impl AsRef<[u8]>) {
        self.builder.push(Some(value))
    }

    #[inline]
    pub fn append_null(&mut self) {
        self.builder.push_null();
    }

    #[inline]
    pub fn append_option<S: AsRef<[u8]>>(&mut self, opt: Option<S>) {
        match opt {
            Some(s) => self.append_value(s),
            None => self.append_null(),
        }
    }

    pub fn finish(&mut self) -> DFStringArray {
        let array = self.builder.as_arc();
        DFStringArray::from_arrow_array(array.as_ref())
    }
}

impl ArrayDeserializer for StringArrayBuilder {
    fn de(&mut self, reader: &mut &[u8]) -> Result<()> {
        let offset: u64 = reader.read_uvarint()?;
        let mut values: Vec<u8> = Vec::with_capacity(offset as usize);
        reader.read_exact(&mut values)?;
        self.append_value(reader);
        Ok(())
    }

    fn de_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()> {
        for row in 0..rows {
            let reader = &reader[step * row..];
            self.append_value(reader);
        }
        Ok(())
    }

    fn finish_to_series(&mut self) -> Series {
        self.finish().into_series()
    }

    fn de_text(&mut self, reader: &[u8]) {
        self.append_value(reader)
    }

    fn de_null(&mut self) {
        self.append_null()
    }
}

impl<S> NewDataArray<S> for DFStringArray
where S: AsRef<[u8]>
{
    fn new_from_slice(v: &[S]) -> Self {
        let values_size = v.iter().fold(0, |acc, s| acc + s.as_ref().len());
        let mut builder = StringArrayBuilder::with_capacity(values_size);
        v.iter().for_each(|val| {
            builder.append_value(val.as_ref());
        });

        builder.finish()
    }

    fn new_from_opt_slice(opt_v: &[Option<S>]) -> Self {
        let values_size = opt_v.iter().fold(0, |acc, s| match s {
            Some(s) => acc + s.as_ref().len(),
            None => acc,
        });
        let mut builder = StringArrayBuilder::with_capacity(values_size);
        opt_v.iter().for_each(|opt| match opt {
            Some(v) => builder.append_value(v.as_ref()),
            None => builder.append_null(),
        });
        builder.finish()
    }

    fn new_from_opt_iter(it: impl Iterator<Item = Option<S>>) -> Self {
        let cap = get_iter_capacity(&it);
        let mut builder = StringArrayBuilder::with_capacity(cap * 5);
        it.for_each(|opt| builder.append_option(opt));
        builder.finish()
    }

    /// Create a new DataArray from an iterator.
    fn new_from_iter(it: impl Iterator<Item = S>) -> Self {
        let cap = get_iter_capacity(&it);
        let mut builder = StringArrayBuilder::with_capacity(cap * 5);
        it.for_each(|v| builder.append_value(v));
        builder.finish()
    }
}
