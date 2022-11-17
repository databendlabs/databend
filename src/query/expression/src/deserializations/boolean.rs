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
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::BinaryRead;
use common_io::prelude::FormatSettings;

use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;

impl TypeDeserializer for MutableBitmap {
    fn memory_size(&self) -> usize {
        self.len()
    }

    fn de_binary(&mut self, reader: &mut &[u8], _format: &FormatSettings) -> Result<()> {
        let value: bool = reader.read_scalar()?;
        self.push(value);
        Ok(())
    }

    fn de_default(&mut self) {
        self.push(false);
    }

    fn de_fixed_binary_batch(
        &mut self,
        reader: &[u8],
        step: usize,
        rows: usize,
        _format: &FormatSettings,
    ) -> Result<()> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let value: bool = reader.read_scalar()?;
            self.push(value);
        }
        Ok(())
    }

    fn de_json(&mut self, value: &serde_json::Value, _format: &FormatSettings) -> Result<()> {
        match value {
            serde_json::Value::Bool(v) => self.push(*v),
            _ => return Err(ErrorCode::from("Incorrect boolean value")),
        }
        Ok(())
    }

    fn append_data_value(&mut self, value: Scalar, _format: &FormatSettings) -> Result<()> {
        let v = value
            .as_boolean()
            .ok_or_else(|| ErrorCode::from("Unable to get boolean value"))?;
        self.push(*v);
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<()> {
        match self.pop() {
            Some(_) => Ok(()),
            None => Err(ErrorCode::from(
                "Boolean column is empty when pop data value",
            )),
        }
    }

    fn finish_to_column(&mut self) -> Column {
        self.shrink_to_fit();
        let bitmap = std::mem::replace(self, Self::with_capacity(0));
        Column::Boolean(bitmap.into())
    }
}
