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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_io::prelude::BinaryRead;
use common_io::prelude::FormatSettings;
use common_io::prelude::*;

use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;

impl TypeDeserializer for MutableBitmap {
    fn memory_size(&self) -> usize {
        self.len()
    }

    fn de_binary(&mut self, reader: &mut &[u8], format: &FormatSettings) -> Result<(), String> {
        let value: bool = reader.read_scalar()?;
        self.push(value);
        Ok(())
    }

    fn de_default(&mut self, _format: &FormatSettings) {
        self.push(false);
    }

    fn de_fixed_binary_batch(
        &mut self,
        reader: &[u8],
        step: usize,
        rows: usize,
        format: &FormatSettings,
    ) -> Result<(), String> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let value: bool = reader.read_scalar()?;
            self.push(value);
        }
        Ok(())
    }

    fn de_json(
        &mut self,
        reader: &serde_json::Value,
        format: &FormatSettings,
    ) -> Result<(), String> {
        match value {
            serde_json::Value::Bool(v) => self.push(*v),
            _ => return Err("Incorrect boolean value".to_string()),
        }
        Ok(())
    }

    fn append_data_value(&mut self, value: Scalar, _format: &FormatSettings) -> Result<(), String> {
        let v = value
            .as_boolean()
            .ok_or_else(|| "Unable to get boolean value".to_string())?;
        self.push(*v);
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<()> {
        match self.pop() {
            Some(v) => Ok(()),
            None => Err("Boolean column is empty when pop data value".to_string()),
        }
    }

    fn finish_to_column(&mut self) -> Column {
        self.shrink_to_fit();
        Column::Boolean(std::mem::take(&mut self).into())
    }
}
