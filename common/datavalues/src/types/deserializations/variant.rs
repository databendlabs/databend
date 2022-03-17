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

use std::io::Read;

use common_exception::Result;
use common_io::prelude::BinaryRead;
use serde_json::Value as JsonValue;

use crate::prelude::*;

pub struct VariantDeserializer {
    pub buffer: Vec<u8>,
    pub builder: MutableObjectColumn<JsonValue>,
}

impl VariantDeserializer {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::new(),
            builder: MutableObjectColumn::<JsonValue>::with_capacity(capacity),
        }
    }
}

impl TypeDeserializer for VariantDeserializer {
    #[allow(clippy::uninit_vec)]
    fn de_binary(&mut self, reader: &mut &[u8]) -> Result<()> {
        let offset: u64 = reader.read_uvarint()?;

        self.buffer.clear();
        self.buffer.reserve(offset as usize);
        unsafe {
            self.buffer.set_len(offset as usize);
        }

        reader.read_exact(&mut self.buffer)?;
        let val = serde_json::from_slice(self.buffer.as_slice())?;
        self.builder.append_value(val);
        Ok(())
    }

    fn de_default(&mut self) {
        self.builder.append_value(JsonValue::Null);
    }

    fn de_fixed_binary_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()> {
        for row in 0..rows {
            let reader = &reader[step * row..];
            let val = serde_json::from_slice(reader)?;
            self.builder.append_value(val);
        }
        Ok(())
    }

    fn de_text(&mut self, reader: &[u8]) -> Result<()> {
        let val = serde_json::from_slice(reader)?;
        self.builder.append_value(val);
        Ok(())
    }

    fn finish_to_column(&mut self) -> ColumnRef {
        self.builder.to_column()
    }
}
