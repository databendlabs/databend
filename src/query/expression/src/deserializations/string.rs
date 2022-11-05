// Copyright 2022 Datafuse Labs.
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

use common_io::prelude::*;

use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;
use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;

pub struct StringDeserializer {
    pub buffer: Vec<u8>,
    pub builder: StringColumnBuilder,
}

impl StringDeserializer {
    pub fn create() -> Self {
        Self {
            buffer: Vec::new(),
            builder: StringColumnBuilder::with_capacity(0, 0),
        }
    }
}

impl TypeDeserializer for StringDeserializer {
    fn memory_size(&self) -> usize {
        self.builder.data.len() * std::mem::size_of::<u8>()
            + self.builder.offsets.len() * std::mem::size_of::<u64>()
    }

    fn de_default(&mut self, _format: &FormatSettings) {
        self.builder.put_str("");
        self.builder.commit_row();
    }

    fn append_data_value(&mut self, value: Scalar, _format: &FormatSettings) -> Result<(), String> {
        let v = value
            .as_string()
            .ok_or_else(|| "Unable to get string value".to_string())?;
        self.builder.put(v.as_slice());
        self.builder.commit_row();
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<Scalar, String> {
        match self.builder.pop() {
            Some(v) => Ok(Scalar::String(v)),
            None => Err("String column is empty when pop data value".to_string()),
        }
    }

    fn finish_to_column(&mut self) -> Column {
        let col = StringColumn {
            data: std::mem::take(&mut self.builder.data).into(),
            offsets: std::mem::take(&mut self.builder.offsets).into(),
        };
        Column::String(col)
    }
}
