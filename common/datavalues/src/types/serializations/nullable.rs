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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use opensrv_clickhouse::types::column::NullableColumnData;
use serde_json::Value;

use crate::serializations::TypeSerializer;
use crate::serializations::TypeSerializerImpl;

#[derive(Clone)]
pub struct NullableSerializer<'a> {
    pub validity: &'a Bitmap,
    pub inner: Box<TypeSerializerImpl<'a>>,
}

impl<'a> TypeSerializer<'a> for NullableSerializer<'a> {
    fn need_quote(&self) -> bool {
        self.inner.need_quote()
    }

    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        if !self.validity.get_bit(row_index) {
            buf.extend_from_slice(&format.null_bytes);
        } else {
            self.inner.write_field(row_index, buf, format)
        }
    }

    fn serialize_json(&self, format: &FormatSettings) -> Result<Vec<Value>> {
        let mut res = self.inner.serialize_json(format)?;
        let validity = self.validity;

        (0..validity.len()).for_each(|row| {
            if !validity.get_bit(row) {
                res[row] = Value::Null;
            }
        });
        Ok(res)
    }

    fn serialize_clickhouse_const(
        &self,
        format: &FormatSettings,
        size: usize,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let inner = self.inner.serialize_clickhouse_const(format, size)?;
        let nulls: Vec<_> = self.validity.iter().map(|v| !v as u8).collect();
        let nulls = nulls.repeat(size);
        let data = NullableColumnData { nulls, inner };

        Ok(Arc::new(data))
    }

    fn serialize_clickhouse_column(
        &self,
        format: &FormatSettings,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let inner = self.inner.serialize_clickhouse_column(format)?;
        let nulls = self.validity.iter().map(|v| !v as u8).collect();
        let data = NullableColumnData { nulls, inner };

        Ok(Arc::new(data))
    }
}
