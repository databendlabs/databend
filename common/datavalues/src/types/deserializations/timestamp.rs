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

use chrono_tz::Tz;
use common_exception::*;
use common_io::prelude::*;
use lexical_core::FromLexical;
use num::cast::AsPrimitive;

use crate::columns::MutableColumn;
use crate::prelude::*;

pub struct TimestampDeserializer<T: PrimitiveType> {
    pub builder: MutablePrimitiveColumn<T>,
    pub tz: Tz,
    pub precision: usize,
}

impl<T> TypeDeserializer for TimestampDeserializer<T>
where
    i64: AsPrimitive<T>,
    T: PrimitiveType,
    T: Unmarshal<T> + StatBuffer + FromLexical,
    for<'a> T:
        opensrv_clickhouse::types::column::iter::Iterable<'a, opensrv_clickhouse::types::Simple>,
{
    fn de_binary(&mut self, reader: &mut &[u8]) -> Result<()> {
        let value: T = reader.read_scalar()?;
        let _ = check_timestamp(value.as_i64())?;
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
            let _ = check_timestamp(value.as_i64())?;
            self.builder.append_value(value);
        }
        Ok(())
    }

    fn de_json(&mut self, value: &serde_json::Value) -> Result<()> {
        match value {
            serde_json::Value::String(v) => {
                let v = v.clone();
                let mut reader = BufferReader::new(v.as_bytes());
                let ts = reader.read_timestamp_text(&self.tz)?;
                let micros = uniform(ts.timestamp_micros(), self.precision);
                let _ = check_timestamp(micros)?;
                self.builder
                    .append_value(micros.as_());
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect boolean value")),
        }
    }

    fn de_text_quoted(&mut self, reader: &mut CpBufferReader) -> Result<()> {
        reader.must_ignore_byte(b'\'')?;
        let ts = reader.read_timestamp_text(&self.tz)?;
        let micros = uniform(ts.timestamp_micros(), self.precision);
        let _ = check_timestamp(micros)?;
        reader.must_ignore_byte(b'\'')?;
        self.builder
            .append_value(micros.as_());
        Ok(())
    }

    fn de_whole_text(&mut self, reader: &[u8]) -> Result<()> {
        let mut reader = BufferReader::new(reader);
        let ts = reader.read_timestamp_text(&self.tz)?;
        let micros = uniform(ts.timestamp_micros(), self.precision);
        let _ = check_timestamp(micros)?;
        reader.must_eof()?;
        self.builder
            .append_value(micros.as_());
        Ok(())
    }

    fn de_text(&mut self, reader: &mut CpBufferReader) -> Result<()> {
        let ts = reader.read_timestamp_text(&self.tz)?;
        let micros = uniform(ts.timestamp_micros(), self.precision);
        let _ = check_timestamp(micros)?;
        self.builder
            .append_value(micros.as_());
        Ok(())
    }

    fn de_text_csv(&mut self, reader: &mut CpBufferReader) -> Result<()> {
        let maybe_quote = reader.ignore(|f| f == b'\'' || f == b'"')?;
        let ts = reader.read_timestamp_text(&self.tz)?;
        let micros = uniform(ts.timestamp_micros(), self.precision);
        let _ = check_timestamp(micros)?;
        if maybe_quote {
            reader.must_ignore(|f| f == b'\'' || f == b'"')?;
        }
        self.builder
            .append_value(micros.as_());
        Ok(())
    }

    fn de_text_json(&mut self, reader: &mut CpBufferReader) -> Result<()> {
        reader.must_ignore_byte(b'"')?;
        let ts = reader.read_timestamp_text(&self.tz)?;
        let micros = uniform(ts.timestamp_micros(), self.precision);
        let _ = check_timestamp(micros)?;
        reader.must_ignore_byte(b'"')?;

        self.builder
            .append_value(micros.as_());
        Ok(())
    }

    fn append_data_value(&mut self, value: DataValue) -> Result<()> {
        let v = value.as_i64()?;
        let _ = check_timestamp(v)?;
        self.builder.append_value(v.as_());
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<DataValue> {
        self.builder.pop_data_value()
    }

    fn finish_to_column(&mut self) -> ColumnRef {
        self.builder.to_column()
    }
}

#[inline]
fn uniform(micros: i64, precision: usize) -> i64 {
    micros / 10_i64.pow(6 - precision as u32)
}
