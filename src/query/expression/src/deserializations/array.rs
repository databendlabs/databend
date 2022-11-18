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
use common_io::prelude::BinaryRead;
use common_io::prelude::FormatSettings;

use crate::types::array::ArrayColumn;
use crate::types::AnyType;
use crate::types::ValueType;
use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;

pub struct ArrayDeserializer {
    pub inner: Box<dyn TypeDeserializer>,
    pub offsets: Vec<u64>,
}

impl ArrayDeserializer {
    fn add_offset(&mut self, size: usize) {
        if self.offsets.is_empty() {
            self.offsets.push(0);
        }
        self.offsets
            .push(*self.offsets.last().unwrap() + size as u64);
    }

    fn pop_offset(&mut self) -> Result<usize> {
        if self.offsets.len() <= 1 {
            return Err(ErrorCode::BadDataValueType("Array is empty".to_string()));
        }
        let total = self.offsets.pop().unwrap();
        Ok((total - *self.offsets.last().unwrap()) as usize)
    }
}

impl TypeDeserializer for ArrayDeserializer {
    fn memory_size(&self) -> usize {
        self.inner.memory_size() + self.offsets.len() * std::mem::size_of::<u64>()
    }

    fn de_binary(&mut self, reader: &mut &[u8], format: &FormatSettings) -> Result<()> {
        let size = reader.read_uvarint()?;
        for _i in 0..size {
            self.inner.de_binary(reader, format)?;
        }
        self.add_offset(size as usize);
        Ok(())
    }

    fn de_default(&mut self) {
        self.add_offset(0);
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
            let size = reader.read_uvarint()?;
            for _i in 0..size {
                self.inner.de_binary(&mut reader, format)?;
            }
            self.add_offset(size as usize);
        }
        Ok(())
    }

    fn de_json(&mut self, value: &serde_json::Value, format: &FormatSettings) -> Result<()> {
        match value {
            serde_json::Value::Array(vals) => {
                for val in vals {
                    self.inner.de_json(val, format)?;
                }
                self.add_offset(vals.len());
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be array")),
        }
    }

    fn append_data_value(&mut self, value: Scalar, format: &FormatSettings) -> Result<()> {
        let value = value.as_array().unwrap();
        for val in AnyType::iter_column(value) {
            self.inner.append_data_value(val.to_owned(), format)?;
        }
        self.add_offset(value.len());
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<()> {
        let size = self.pop_offset()?;
        for _ in 0..size {
            self.inner.pop_data_value()?;
        }
        Ok(())
    }

    fn finish_to_column(&mut self) -> Column {
        let values = self.inner.finish_to_column();
        let offsets = std::mem::take(&mut self.offsets);
        Column::Array(Box::new(ArrayColumn {
            values,
            offsets: offsets.into(),
        }))
    }
}
