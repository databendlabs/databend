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
use common_io::prelude::FormatSettings;

use crate::types::DataType;
use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;
use crate::TypeDeserializerImpl;

pub struct TupleDeserializer {
    pub inners: Vec<TypeDeserializerImpl>,
}

pub type StructDeserializer = TupleDeserializer;

impl TupleDeserializer {
    pub fn with_capacity(capacity: usize, inners: &[DataType]) -> Self {
        let inners = inners
            .iter()
            .map(|ty| TypeDeserializerImpl::with_capacity(ty, capacity))
            .collect();
        Self { inners }
    }
}

impl TypeDeserializer for TupleDeserializer {
    fn memory_size(&self) -> usize {
        self.inners.iter().map(|d| d.memory_size()).sum()
    }

    fn len(&self) -> usize {
        self.inners.first().map(|c| c.len()).unwrap_or_default()
    }

    fn de_binary(&mut self, reader: &mut &[u8], format: &FormatSettings) -> Result<()> {
        for inner in self.inners.iter_mut() {
            inner.de_binary(reader, format)?;
        }
        Ok(())
    }

    fn de_default(&mut self) {
        for inner in self.inners.iter_mut() {
            inner.de_default();
        }
    }

    fn de_json(&mut self, value: &serde_json::Value, format: &FormatSettings) -> Result<()> {
        match value {
            serde_json::Value::Array(obj) => {
                if self.inners.len() != obj.len() {
                    return Err(ErrorCode::from_string(format!(
                        "Incorrect json value, expect {} values, but get {} values",
                        self.inners.len(),
                        obj.len()
                    )));
                }
                for (inner, val) in self.inners.iter_mut().zip(obj.iter()) {
                    inner.de_json(val, format)?;
                }
                Ok(())
            }
            _ => Err(ErrorCode::from("Incorrect tuple value")),
        }
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
            for inner in self.inners.iter_mut() {
                inner.de_binary(&mut reader, format)?;
            }
        }
        Ok(())
    }

    fn append_data_value(&mut self, value: Scalar, format: &FormatSettings) -> Result<()> {
        let v = value
            .as_tuple()
            .ok_or_else(|| ErrorCode::from("Unable to get tuple value"))?;

        for (v, inner) in v.iter().zip(self.inners.iter_mut()) {
            inner.append_data_value(v.clone(), format)?;
        }
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<Scalar> {
        let mut vals = Vec::with_capacity(self.inners.len());
        for inner in self.inners.iter_mut() {
            let val = inner.pop_data_value()?;
            vals.push(val);
        }
        Ok(Scalar::Tuple(vals))
    }

    fn finish_to_column(&mut self) -> Column {
        let fields: Vec<Column> = self
            .inners
            .iter_mut()
            .map(|f| f.finish_to_column())
            .collect();
        let len = fields.iter().map(|f| f.len()).next().unwrap_or(0);
        Column::Tuple { fields, len }
    }
}
