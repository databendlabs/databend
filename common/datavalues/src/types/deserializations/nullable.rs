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

use common_arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;

use crate::ColumnRef;
use crate::DataValue;
use crate::NullableColumn;
use crate::TypeDeserializer;
use crate::TypeDeserializerImpl;

pub struct NullableDeserializer {
    pub inner: Box<TypeDeserializerImpl>,
    pub bitmap: MutableBitmap,
}

impl TypeDeserializer for NullableDeserializer {
    fn de_binary(&mut self, reader: &mut &[u8]) -> Result<()> {
        let valid: bool = reader.read_scalar()?;
        if valid {
            self.inner.de_binary(reader)?;
        } else {
            self.inner.de_default();
        }
        self.bitmap.push(valid);
        Ok(())
    }

    fn de_default(&mut self) {
        self.inner.de_default();
        self.bitmap.push(false);
    }

    fn de_fixed_binary_batch(&mut self, _reader: &[u8], _step: usize, _rows: usize) -> Result<()> {
        // it's covered outside
        unreachable!()
    }

    fn de_json(&mut self, value: &serde_json::Value) -> Result<()> {
        match value {
            serde_json::Value::Null => {
                self.de_null();
                Ok(())
            }
            other => {
                self.bitmap.push(true);
                self.inner.de_json(other)
            }
        }
    }

    // TODO: support null text setting
    fn de_text<R: BufferRead>(&mut self, reader: &mut CheckpointReader<R>) -> Result<()> {
        if reader.ignore_insensitive_bytes(b"null")? {
            self.de_default();
            return Ok(());
        }
        self.inner.de_text(reader)?;
        self.bitmap.push(true);
        Ok(())
    }

    fn de_text_quoted<R: BufferRead>(&mut self, reader: &mut CheckpointReader<R>) -> Result<()> {
        if reader.ignore_insensitive_bytes(b"null")? {
            self.de_default();
            return Ok(());
        }
        self.inner.de_text_quoted(reader)?;
        self.bitmap.push(true);
        Ok(())
    }

    fn de_whole_text(&mut self, reader: &[u8]) -> Result<()> {
        if reader.eq_ignore_ascii_case(b"null") {
            self.de_default();
            return Ok(());
        }

        self.inner.de_whole_text(reader)?;
        self.bitmap.push(true);
        Ok(())
    }

    fn de_null(&mut self) -> bool {
        self.inner.de_default();
        self.bitmap.push(false);
        true
    }

    fn append_data_value(&mut self, value: DataValue) -> Result<()> {
        if value.is_null() {
            self.inner.de_default();
            self.bitmap.push(false);
        } else {
            self.inner.append_data_value(value)?;
            self.bitmap.push(true);
        }
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<DataValue> {
        self.bitmap
            .pop()
            .ok_or_else(|| {
                ErrorCode::BadDataArrayLength("Nullable deserializer is empty when pop data value")
            })
            .and_then(|v| {
                let inner_value = self.inner.pop_data_value();
                v.then(|| inner_value).unwrap_or(Ok(DataValue::Null))
            })
    }

    fn finish_to_column(&mut self) -> ColumnRef {
        let inner_column = self.inner.finish_to_column();
        let bitmap = std::mem::take(&mut self.bitmap);
        NullableColumn::wrap_inner(inner_column, Some(bitmap.into()))
    }
}
