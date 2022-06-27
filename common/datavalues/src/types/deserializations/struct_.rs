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

use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;

use crate::prelude::*;

pub struct StructDeserializer {
    pub builder: MutableStructColumn,
    pub inners: Vec<TypeDeserializerImpl>,
}

impl TypeDeserializer for StructDeserializer {
    #[allow(clippy::uninit_vec)]
    fn de_binary(&mut self, reader: &mut &[u8], format: &FormatSettings) -> Result<()> {
        let mut values = Vec::with_capacity(self.inners.len());
        for inner in self.inners.iter_mut() {
            inner.de_binary(reader, format)?;
            values.push(inner.pop_data_value().unwrap());
        }
        self.builder.append_data_value(DataValue::Struct(values))
    }

    fn de_default(&mut self, _format: &FormatSettings) {
        self.builder.append_default();
    }

    fn de_fixed_binary_batch(
        &mut self,
        reader: &[u8],
        step: usize,
        rows: usize,
        format: &FormatSettings,
    ) -> Result<()> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let mut values = Vec::with_capacity(self.inners.len());

            for inner in self.inners.iter_mut() {
                inner.de_binary(&mut reader, format)?;
                values.push(inner.pop_data_value().unwrap());
            }
            self.builder.append_data_value(DataValue::Struct(values))?;
        }
        Ok(())
    }

    fn de_json(&mut self, value: &serde_json::Value, format: &FormatSettings) -> Result<()> {
        match value {
            serde_json::Value::Object(obj) => {
                if self.inners.len() != obj.len() {
                    return Err(ErrorCode::BadBytes(format!(
                        "Incorrect json value, expect {} values, but get {} values",
                        self.inners.len(),
                        obj.len()
                    )));
                }
                let mut values = Vec::with_capacity(self.inners.len());
                for (inner, item) in self.inners.iter_mut().zip(obj.iter()) {
                    let (_, val) = item;
                    inner.de_json(val, format)?;
                    values.push(inner.pop_data_value()?);
                }
                self.builder.append_value(StructValue::new(values));
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be object")),
        }
    }

    fn de_text<R: BufferRead>(&mut self, reader: &mut R, format: &FormatSettings) -> Result<()> {
        reader.must_ignore_byte(b'(')?;
        let mut values = Vec::with_capacity(self.inners.len());
        for (idx, inner) in self.inners.iter_mut().enumerate() {
            let _ = reader.ignore_white_spaces()?;
            if idx != 0 {
                reader.must_ignore_byte(b',')?;
            }
            let _ = reader.ignore_white_spaces()?;
            inner.de_text_quoted(reader, format)?;
            values.push(inner.pop_data_value()?);
        }
        reader.must_ignore_byte(b')')?;
        self.builder.append_value(StructValue::new(values));
        Ok(())
    }

    fn de_text_csv<R: BufferRead>(
        &mut self,
        _reader: &mut R,
        _format: &FormatSettings,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement("Unimplement error"))
    }

    fn de_whole_text(&mut self, _reader: &[u8], _format: &FormatSettings) -> Result<()> {
        Err(ErrorCode::UnImplement("Unimplement error"))
    }

    fn append_data_value(&mut self, value: DataValue, _format: &FormatSettings) -> Result<()> {
        self.builder.append_data_value(value)
    }

    fn pop_data_value(&mut self) -> Result<DataValue> {
        self.builder.pop_data_value()
    }

    fn finish_to_column(&mut self) -> ColumnRef {
        self.builder.to_column()
    }
}
