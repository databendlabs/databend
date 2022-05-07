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

use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use lexical_core::FromLexical;

use crate::prelude::*;

pub struct NumberDeserializer<T: PrimitiveType> {
    pub builder: MutablePrimitiveColumn<T>,
}

impl<T> TypeDeserializer for NumberDeserializer<T>
    where
        T: PrimitiveType,
        T: Unmarshal<T> + StatBuffer + FromLexical,
{
    fn de_binary(&mut self, reader: &mut &[u8]) -> Result<()> {
        let value: T = reader.read_scalar()?;
        self.builder.append_value(value);
        Ok(())
    }

    fn de_default(&mut self) {
        self.builder.append_value(T::default());
    }

    fn de_fixed_binary_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let value: T = reader.read_scalar()?;
            self.builder.append_value(value);
        }
        Ok(())
    }

    fn de_json(&mut self, value: &serde_json::Value) -> Result<()> {
        match value {
            serde_json::Value::Number(v) => {
                let v = v.to_string();
                let mut reader = BufferReader::new(v.as_bytes());
                let v: T = if !T::FLOATING {
                    reader.read_int_text()
                } else {
                    reader.read_float_text()
                }?;

                self.builder.append_value(v);
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be number")),
        }
    }

    fn de_null(&mut self) -> bool {
        false
    }

    fn de_whole_text(&mut self, reader: &[u8]) -> Result<()> {
        let mut reader = BufferReader::new(reader);
        let v: T = if !T::FLOATING {
            reader.read_int_text()
        } else {
            reader.read_float_text()
        }?;
        reader.must_eof()?;

        self.builder.append_value(v);
        Ok(())
    }

    fn de_text<R: BufferRead>(&mut self, reader: &mut CheckpointReader<R>) -> Result<()> {
        let v: T = if !T::FLOATING {
            reader.read_int_text()
        } else {
            reader.read_float_text()
        }?;
        self.builder.append_value(v);
        Ok(())
    }

    fn de_text_csv<R: BufferRead>(&mut self, reader: &mut CheckpointReader<R>) -> Result<()> {
        let maybe_quote = reader.ignore(|f| f == b'\'' || f == b'"')?;
        let v: T = if !T::FLOATING {
            reader.read_int_text()
        } else {
            reader.read_float_text()
        }?;

        if maybe_quote {
            reader.must_ignore(|f| f == b'\'' || f == b'"')?;
        }

        self.builder.append_value(v);
        Ok(())
    }

    fn append_data_value(&mut self, value: DataValue) -> Result<()> {
        self.builder.append_data_value(value)
    }

    fn pop_data_value(&mut self) -> Result<DataValue> {
        self.builder.pop_data_value()
    }

    fn finish_to_column(&mut self) -> ColumnRef {
        self.builder.to_column()
    }
}
