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

use std::sync::Arc;

use common_exception::Result;
use common_io::prelude::FormatSettings;
use opensrv_clickhouse::types::column::ArrayColumnData;
use serde_json::Value;

use crate::prelude::*;

#[derive(Clone)]
pub struct ArraySerializer<'a> {
    offsets: &'a [i64],
    inner: Box<TypeSerializerImpl<'a>>,
}

impl<'a> ArraySerializer<'a> {
    pub fn try_create(column: &'a ColumnRef, inner_type: &DataTypeImpl) -> Result<Self> {
        let column: &ArrayColumn = Series::check_get(column)?;
        let inner = Box::new(inner_type.create_serializer(column.values())?);
        Ok(Self {
            offsets: column.offsets(),
            inner,
        })
    }
}

impl<'a> TypeSerializer<'a> for ArraySerializer<'a> {
    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        let start = self.offsets[row_index] as usize;
        let end = self.offsets[row_index + 1] as usize;
        buf.push(b'[');
        let inner = &self.inner;
        for i in start..end {
            if i != start {
                buf.extend_from_slice(b", ");
            }
            inner.write_field_quoted(i, buf, format, b'\'');
        }
        buf.push(b']');
    }

    fn serialize_clickhouse_const(
        &self,
        format: &FormatSettings,
        size: usize,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let len = self.offsets.len() - 1;
        let mut offsets = opensrv_clickhouse::types::column::List::with_capacity(size * len);
        let total = self.offsets[len];
        let mut base = 0;
        for _ in 0..size {
            for offset in self.offsets.iter().skip(1) {
                offsets.push(((*offset) + base) as u64);
            }
            base += total;
        }

        let inner_data = self.inner.serialize_clickhouse_const(format, size)?;
        Ok(Arc::new(ArrayColumnData::create(inner_data, offsets)))
    }

    fn serialize_clickhouse_column(
        &self,
        format: &FormatSettings,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let mut offsets =
            opensrv_clickhouse::types::column::List::with_capacity(self.offsets.len() - 1);
        for offset in self.offsets.iter().skip(1) {
            offsets.push(*offset as u64);
        }

        let inner_data = self.inner.serialize_clickhouse_column(format)?;
        Ok(Arc::new(ArrayColumnData::create(inner_data, offsets)))
    }

    fn serialize_json_values(&self, format: &FormatSettings) -> Result<Vec<Value>> {
        let size = self.offsets.len() - 1;
        let mut result = Vec::with_capacity(size);
        let inner = self.inner.serialize_json_values(format)?;
        let mut iter = inner.into_iter();
        for i in 0..size {
            let len = (self.offsets[i + 1] - self.offsets[i]) as usize;
            let chunk = iter.by_ref().take(len).collect();
            result.push(Value::Array(chunk))
        }
        Ok(result)
    }
}
