// Copyright 2023 Datafuse Labs.
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
use crate::types::map::KvPair;
use crate::types::AnyType;
use crate::types::DataType;
use crate::types::ValueType;
use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;
use crate::TypeDeserializerImpl;

pub struct MapDeserializer {
    pub key: Box<TypeDeserializerImpl>,
    pub value: Box<TypeDeserializerImpl>,
    offsets: Vec<u64>,
}

impl MapDeserializer {
    pub fn with_capacity(capacity: usize, inner_ty: &DataType) -> Self {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(0);
        match inner_ty {
            DataType::Tuple(typs) => {
                let key_ty = &typs[0];
                let value_ty = &typs[1];
                Self {
                    key: Box::new(key_ty.create_deserializer(capacity)),
                    value: Box::new(value_ty.create_deserializer(capacity)),
                    offsets,
                }
            }
            _ => unreachable!(),
        }
    }

    pub fn add_offset(&mut self, size: usize) {
        self.offsets
            .push(*self.offsets.last().unwrap() + size as u64);
    }

    pub fn pop_offset(&mut self) -> Result<usize> {
        if self.offsets.len() <= 1 {
            return Err(ErrorCode::BadDataValueType("Map is empty".to_string()));
        }
        let total = self.offsets.pop().unwrap();
        Ok((total - *self.offsets.last().unwrap()) as usize)
    }
}

impl TypeDeserializer for MapDeserializer {
    fn memory_size(&self) -> usize {
        self.key.memory_size()
            + self.value.memory_size()
            + self.offsets.len() * std::mem::size_of::<u64>()
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn de_binary(&mut self, reader: &mut &[u8], format: &FormatSettings) -> Result<()> {
        let size = reader.read_uvarint()?;
        for _i in 0..size {
            self.key.de_binary(reader, format)?;
            self.value.de_binary(reader, format)?;
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
                self.key.de_binary(&mut reader, format)?;
                self.value.de_binary(&mut reader, format)?;
            }
            self.add_offset(size as usize);
        }
        Ok(())
    }

    fn de_json(&mut self, value: &serde_json::Value, format: &FormatSettings) -> Result<()> {
        match value {
            serde_json::Value::Object(obj) => {
                for (key, val) in obj.iter() {
                    let key = serde_json::Value::String(key.to_string());
                    self.key.de_json(&key, format)?;
                    self.value.de_json(val, format)?;
                }
                self.add_offset(obj.len());
                Ok(())
            }
            _ => Err(ErrorCode::BadBytes("Incorrect json value, must be object")),
        }
    }

    fn append_data_value(&mut self, value: Scalar, format: &FormatSettings) -> Result<()> {
        let col = value.as_map().unwrap();
        let kv_col = KvPair::<AnyType, AnyType>::try_downcast_column(col).unwrap();
        for (key, val) in kv_col.iter() {
            self.key.append_data_value(key.to_owned(), format)?;
            self.value.append_data_value(val.to_owned(), format)?;
        }
        self.add_offset(col.len());
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<()> {
        let size = self.pop_offset()?;
        for _ in 0..size {
            self.key.pop_data_value()?;
            self.value.pop_data_value()?;
        }
        Ok(())
    }

    fn finish_to_column(&mut self) -> Column {
        let key_col = self.key.finish_to_column();
        let value_col = self.value.finish_to_column();
        let len = key_col.len();
        let values = Column::Tuple {
            fields: vec![key_col, value_col],
            len,
        };
        let offsets = std::mem::take(&mut self.offsets);
        Column::Map(Box::new(ArrayColumn {
            values,
            offsets: offsets.into(),
        }))
    }
}
