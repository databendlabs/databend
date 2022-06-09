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
use opensrv_clickhouse::types::column::ArcColumnWrapper;
use opensrv_clickhouse::types::column::ColumnFrom;
use opensrv_clickhouse::types::column::NullableColumnData;
use serde_json::Value;

use crate::serializations::TypeSerializer;

#[derive(Clone, Debug, Default)]
pub struct NullSerializer {
    pub size: usize,
}

const NULL_STR: &str = "NULL";
const NULL_BYTES: &[u8] = b"NULL";

impl<'a> TypeSerializer<'a> for NullSerializer {
    fn need_quote(&self) -> bool {
        false
    }

    fn write_field(&self, _row_index: usize, buf: &mut Vec<u8>, _format: &FormatSettings) {
        buf.extend_from_slice(NULL_BYTES);
    }

    fn serialize_field(&self, _row_index: usize, _format: &FormatSettings) -> Result<String> {
        Ok(NULL_STR.to_owned())
    }

    fn serialize_json(&self, _format: &FormatSettings) -> Result<Vec<Value>> {
        let null = Value::Null;
        let result: Vec<Value> = vec![null; self.size];
        Ok(result)
    }

    fn serialize_clickhouse_const(
        &self,
        _format: &FormatSettings,
        size: usize,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let n = size * self.size;
        let nulls = vec![1u8; n];
        let inner = Vec::column_from::<ArcColumnWrapper>(vec![1u8; n]);
        let data = NullableColumnData { nulls, inner };
        Ok(Arc::new(data))
    }

    fn serialize_clickhouse_column(
        &self,
        _format: &FormatSettings,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let nulls = vec![1u8; self.size];
        let inner = Vec::column_from::<ArcColumnWrapper>(vec![1u8; self.size]);
        let data = NullableColumnData { nulls, inner };
        Ok(Arc::new(data))
    }
}
