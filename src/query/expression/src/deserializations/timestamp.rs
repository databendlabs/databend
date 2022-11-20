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

use std::io::Cursor;

use common_exception::ErrorCode;
use common_exception::Result;
use common_io::cursor_ext::*;
use common_io::prelude::BinaryRead;
use common_io::prelude::FormatSettings;

use crate::types::timestamp::check_timestamp;
use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;

pub struct TimestampDeserializer {
    pub builder: Vec<i64>,
}

impl TimestampDeserializer {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: Vec::with_capacity(capacity),
        }
    }
}

impl TypeDeserializer for TimestampDeserializer {
    fn memory_size(&self) -> usize {
        self.builder.len() * std::mem::size_of::<i64>()
    }

    fn de_binary(&mut self, reader: &mut &[u8], _format: &FormatSettings) -> Result<()> {
        let value: i64 = reader.read_scalar()?;
        check_timestamp(value)?;
        self.builder.push(value);
        Ok(())
    }

    fn de_default(&mut self) {
        self.builder.push(i64::default());
    }

    fn de_json(&mut self, value: &serde_json::Value, format: &FormatSettings) -> Result<()> {
        match value {
            serde_json::Value::String(v) => {
                let v = v.clone();
                let mut reader = Cursor::new(v.as_bytes());
                let ts = reader.read_timestamp_text(&format.timezone)?;

                let micros = ts.timestamp_micros();
                check_timestamp(micros)?;
                self.builder.push(micros);
                Ok(())
            }
            _ => Err(ErrorCode::from("Incorrect timestamp value")),
        }
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
            let value: i64 = reader.read_scalar()?;
            self.builder.push(value);
        }
        Ok(())
    }

    fn append_data_value(&mut self, value: Scalar, _format: &FormatSettings) -> Result<()> {
        let v = value
            .as_timestamp()
            .ok_or_else(|| ErrorCode::from("Unable to get timestamp value"))?;
        check_timestamp(*v)?;
        self.builder.push(*v);
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<()> {
        match self.builder.pop() {
            Some(_) => Ok(()),
            None => Err(ErrorCode::from(
                "Timestamp column is empty when pop data value",
            )),
        }
    }

    fn finish_to_column(&mut self) -> Column {
        self.builder.shrink_to_fit();
        Column::Timestamp(std::mem::take(&mut self.builder).into())
    }
}
