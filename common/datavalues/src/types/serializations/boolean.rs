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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use opensrv_clickhouse::types::column::ArcColumnWrapper;
use opensrv_clickhouse::types::column::ColumnFrom;
use serde_json::Value;

use crate::prelude::*;

const TRUE_BYTES: &[u8] = &[b'1'];
const FALSE_BYTES: &[u8] = &[b'0'];

#[derive(Clone)]
pub struct BooleanSerializer {
    pub(crate) values: Bitmap,
}

impl BooleanSerializer {
    pub fn try_create(col: &ColumnRef) -> Result<Self> {
        let col: &BooleanColumn = Series::check_get(col)?;
        let values = col.values().clone();
        Ok(Self { values })
    }
}

impl<'a> TypeSerializer<'a> for BooleanSerializer {
    fn need_quote(&self) -> bool {
        false
    }

    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, _format: &FormatSettings) {
        let v = if self.values.get_bit(row_index) {
            TRUE_BYTES
        } else {
            FALSE_BYTES
        };
        buf.extend_from_slice(v);
    }

    fn serialize_json(&self, _format: &FormatSettings) -> Result<Vec<Value>> {
        let result: Vec<Value> = self
            .values
            .iter()
            .map(|v| serde_json::to_value(v).unwrap())
            .collect();
        Ok(result)
    }

    fn serialize_clickhouse_const(
        &self,
        _format: &FormatSettings,
        size: usize,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let mut values: Vec<u8> = Vec::with_capacity(self.values.len() * size);
        for _ in 0..size {
            for v in self.values.iter() {
                values.push(v as u8)
            }
        }
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }

    fn serialize_clickhouse_column(
        &self,
        _format: &FormatSettings,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let values: Vec<u8> = self.values.iter().map(|c| c as u8).collect();
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }

    fn serialize_json_object(
        &self,
        _valids: Option<&Bitmap>,
        format: &FormatSettings,
    ) -> Result<Vec<Value>> {
        self.serialize_json(format)
    }

    fn serialize_json_object_suppress_error(
        &self,
        _format: &FormatSettings,
    ) -> Result<Vec<Option<Value>>> {
        let result: Vec<Option<Value>> = self
            .values
            .iter()
            .map(|x| match serde_json::to_value(x) {
                Ok(v) => Some(v),
                Err(_) => None,
            })
            .collect();
        Ok(result)
    }
}
