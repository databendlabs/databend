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
use common_io::prelude::*;

use crate::types::nullable::NullableColumn;
use crate::types::DataType;
use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;

pub struct NullableDeserializer {
    pub validity: MutableBitmap,
    pub inner: Box<dyn TypeDeserializer>,
}

impl NullableDeserializer {
    pub fn with_capacity(capacity: usize, inner_ty: &DataType) -> Self {
        Self {
            validity: MutableBitmap::new(),
            inner: inner_ty.create_deserializer(capacity),
        }
    }
}

impl TypeDeserializer for NullableDeserializer {
    fn memory_size(&self) -> usize {
        self.inner.memory_size() + self.validity.as_slice().len()
    }

    fn de_binary(&mut self, reader: &mut &[u8], format: &FormatSettings) -> Result<()> {
        let valid: bool = reader.read_scalar()?;
        if valid {
            self.inner.de_binary(reader, format)?;
        } else {
            self.inner.de_default();
        }
        self.validity.push(valid);
        Ok(())
    }

    fn de_default(&mut self) {
        self.inner.de_default();
        self.validity.push(false);
    }

    fn de_fixed_binary_batch(
        &mut self,
        _reader: &[u8],
        _step: usize,
        _rows: usize,
        _format: &FormatSettings,
    ) -> Result<()> {
        Err(ErrorCode::from("unreachable"))
    }

    fn de_json(&mut self, value: &serde_json::Value, format: &FormatSettings) -> Result<()> {
        match value {
            serde_json::Value::Null => {
                self.de_null(format);
                Ok(())
            }
            other => {
                self.validity.push(true);
                self.inner.de_json(other, format)
            }
        }
    }

    fn de_null(&mut self, _format: &FormatSettings) -> bool {
        self.inner.de_default();
        self.validity.push(false);
        true
    }

    fn append_data_value(&mut self, value: Scalar, format: &FormatSettings) -> Result<()> {
        match value {
            Scalar::Null => {
                self.validity.push(false);
                self.inner.de_default();
            }
            _ => {
                self.validity.push(true);
                self.inner.append_data_value(value, format)?;
            }
        }
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<()> {
        match self.validity.pop() {
            Some(_) => Ok(()),
            None => Err(ErrorCode::from(
                "Nullable column is empty when pop data value",
            )),
        }
    }

    fn finish_to_column(&mut self) -> Column {
        let col = NullableColumn {
            column: self.inner.finish_to_column(),
            validity: std::mem::take(&mut self.validity).into(),
        };
        Column::Nullable(Box::new(col))
    }
}
