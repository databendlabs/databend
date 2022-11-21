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
use common_io::prelude::BinaryRead;
use common_io::prelude::FormatSettings;

use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;
use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;

impl TypeDeserializer for StringColumnBuilder {
    fn memory_size(&self) -> usize {
        self.data.len() * std::mem::size_of::<u8>()
            + self.offsets.len() * std::mem::size_of::<u64>()
    }

    // See GroupHash.rs for StringColumn
    #[allow(clippy::uninit_vec)]
    fn de_binary(&mut self, reader: &mut &[u8], _format: &FormatSettings) -> Result<()> {
        let offset: u64 = reader.read_uvarint()?;

        self.data.resize(offset as usize + self.data.len(), 0);
        let last = *self.offsets.last().unwrap() as usize;
        reader.read_exact(&mut self.data[last..last + offset as usize])?;

        self.commit_row();
        Ok(())
    }

    fn de_default(&mut self) {
        self.put_str("");
        self.commit_row();
    }

    fn de_fixed_binary_batch(
        &mut self,
        reader: &[u8],
        step: usize,
        rows: usize,
        _format: &FormatSettings,
    ) -> Result<()> {
        for row in 0..rows {
            let reader = &reader[step * row..];
            self.put_slice(reader);
            self.commit_row();
        }
        Ok(())
    }

    fn de_json(&mut self, value: &serde_json::Value, _format: &FormatSettings) -> Result<()> {
        match value {
            serde_json::Value::String(s) => {
                self.put_str(s.as_str());
                self.commit_row();
                Ok(())
            }
            _ => Err(ErrorCode::from("Incorrect json value, must be string")),
        }
    }

    fn append_data_value(&mut self, value: Scalar, _format: &FormatSettings) -> Result<()> {
        let v = value
            .as_string()
            .ok_or_else(|| ErrorCode::from("Unable to get string value"))?;
        self.put(v.as_slice());
        self.commit_row();
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<()> {
        match self.pop() {
            Some(_) => Ok(()),
            None => Err(ErrorCode::from(
                "String column is empty when pop data value",
            )),
        }
    }

    fn finish_to_column(&mut self) -> Column {
        let col = StringColumn {
            data: std::mem::take(&mut self.data).into(),
            offsets: std::mem::take(&mut self.offsets).into(),
        };
        Column::String(col)
    }
}
