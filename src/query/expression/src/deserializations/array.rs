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
use crate::types::DataType;
use crate::types::ValueType;
use crate::Column;
use crate::ColumnBuilder;
use crate::Scalar;
use crate::TypeDeserializer;
use crate::TypeDeserializerImpl;

pub struct ArrayDeserializer {
    pub inner: Box<TypeDeserializerImpl>,
    inner_ty: DataType,
    offsets: Vec<u64>,
}

impl ArrayDeserializer {
    pub fn with_capacity(capacity: usize, inner_ty: &DataType) -> Self {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(0);
        Self {
            inner: Box::new(inner_ty.create_deserializer(capacity)),
            inner_ty: inner_ty.clone(),
            offsets,
        }
    }

    pub fn add_offset(&mut self, size: usize) {
        self.offsets
            .push(*self.offsets.last().unwrap() + size as u64);
    }

    pub fn pop_offset(&mut self) -> Result<usize> {
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

    fn len(&self) -> usize {
        self.offsets.len() - 1
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

    fn pop_data_value(&mut self) -> Result<Scalar> {
        let size = self.pop_offset()?;
        let mut vals = Vec::with_capacity(size);
        for _ in 0..size {
            let val = self.inner.pop_data_value()?;
            vals.push(val);
        }
        let mut builder = ColumnBuilder::with_capacity(&self.inner_ty, size);
        while !vals.is_empty() {
            builder.push(vals.pop().unwrap().as_ref());
        }
        Ok(Scalar::Array(builder.build()))
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
