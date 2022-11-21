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

use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;

use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::variant::JSONB_NULL;
use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;

pub struct VariantDeserializer {
    pub builder: StringColumnBuilder,
}

impl VariantDeserializer {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: StringColumnBuilder::with_capacity(capacity, capacity * 4),
        }
    }
}

impl TypeDeserializer for VariantDeserializer {
    fn memory_size(&self) -> usize {
        self.builder.data.len() * std::mem::size_of::<u8>()
            + self.builder.offsets.len() * std::mem::size_of::<u64>()
    }

    fn de_default(&mut self) {
        self.builder.put(JSONB_NULL);
        self.builder.commit_row();
    }

    fn append_data_value(&mut self, value: Scalar, _format: &FormatSettings) -> Result<()> {
        let v = value
            .as_variant()
            .ok_or_else(|| ErrorCode::from("Unable to get variant value"))?;
        self.builder.put(v.as_slice());
        self.builder.commit_row();
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<()> {
        match self.builder.pop() {
            Some(_) => Ok(()),
            None => Err(ErrorCode::from(
                "Variant column is empty when pop data value",
            )),
        }
    }

    fn finish_to_column(&mut self) -> Column {
        let col = StringColumn {
            data: std::mem::take(&mut self.builder.data).into(),
            offsets: std::mem::take(&mut self.builder.offsets).into(),
        };
        Column::Variant(col)
    }

    fn de_binary(&mut self, reader: &mut &[u8], _format: &FormatSettings) -> Result<()> {
        let offset: u64 = reader.read_uvarint()?;

        self.builder
            .data
            .resize(offset as usize + self.builder.data.len(), 0);
        let last = *self.builder.offsets.last().unwrap() as usize;
        reader.read_exact(&mut self.builder.data[last..last + offset as usize])?;

        self.builder.commit_row();
        Ok(())
    }

    fn de_fixed_binary_batch(
        &mut self,
        reader: &[u8],
        step: usize,
        rows: usize,
        _format: &FormatSettings,
    ) -> Result<()> {
        for row in 0..rows {
            let val = &reader[step * row..];
            self.builder.put_slice(val);
            self.builder.commit_row();
        }
        Ok(())
    }

    fn de_json(&mut self, value: &serde_json::Value, _format: &FormatSettings) -> Result<()> {
        let v = common_jsonb::Value::from(value);
        v.to_vec(&mut self.builder.data);
        self.builder.commit_row();
        Ok(())
    }
}
