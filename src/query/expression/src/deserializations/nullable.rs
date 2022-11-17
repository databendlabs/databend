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
    pub fn create(inner_ty: &DataType) -> Self {
        Self {
            validity: MutableBitmap::new(),
            inner: inner_ty.create_deserializer(),
        }
    }
}

impl TypeDeserializer for NullableDeserializer {
    fn memory_size(&self) -> usize {
        self.inner.memory_size() + self.validity.as_slice().len()
    }

    fn de_binary(&mut self, reader: &mut &[u8], format: &FormatSettings) -> Result<(), String> {
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
    ) -> Result<(), String> {
        Err("unreachable".to_string())
    }

    fn de_json(
        &mut self,
        value: &serde_json::Value,
        format: &FormatSettings,
    ) -> Result<(), String> {
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

    fn append_data_value(&mut self, value: Scalar, format: &FormatSettings) -> Result<(), String> {
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

    fn pop_data_value(&mut self) -> Result<(), String> {
        match self.validity.pop() {
            Some(_) => Ok(()),
            None => Err("Nullable column is empty when pop data value".to_string()),
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
