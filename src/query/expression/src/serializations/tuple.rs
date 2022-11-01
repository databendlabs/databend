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

use common_io::prelude::FormatSettings;
use serde_json::Value;

use crate::types::DataType;
use crate::Column;
use crate::TypeSerializer;
use crate::TypeSerializerImpl;

#[derive(Clone)]
pub struct TupleSerializer {
    pub(crate) inners: Vec<TypeSerializerImpl>,
    pub(crate) len: usize,
}

impl TupleSerializer {
    pub fn try_create(col: Column, inner_tys: &[DataType]) -> Result<Self, String> {
        let (columns, len) = col
            .into_tuple()
            .map_err(|_| "unable to get tuple column".to_string())?;

        let mut inners = Vec::with_capacity(columns.len());
        for (column, inner_ty) in columns.iter().zip(inner_tys.iter()) {
            let inner = inner_ty.create_serializer(column.clone())?;
            inners.push(inner);
        }

        Ok(Self { inners, len })
    }
}

impl TypeSerializer for TupleSerializer {
    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        buf.push(b'(');
        for (i, inner) in self.inners.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            inner.write_field_quoted(row_index, buf, format, b'\'');
        }
        buf.push(b')');
    }

    fn serialize_json_values(&self, format: &FormatSettings) -> Result<Vec<Value>, String> {
        let mut values = Vec::with_capacity(self.len);
        for _ in 0..self.len {
            let value = Vec::with_capacity(self.inners.len());
            values.push(value);
        }
        for inner in &self.inners {
            let mut inner_value = inner.serialize_json_values(format)?;
            for i in (0..self.len).rev() {
                let value = values.get_mut(i).unwrap();
                value.push(inner_value.pop().unwrap());
            }
        }
        let mut result = Vec::with_capacity(self.len);
        for value in values {
            result.push(Value::Array(value));
        }

        Ok(result)
    }
}
