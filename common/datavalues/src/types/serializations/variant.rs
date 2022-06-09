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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use opensrv_clickhouse::types::column::ArcColumnWrapper;
use opensrv_clickhouse::types::column::ColumnFrom;
use serde_json;
use serde_json::Value;

use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct VariantSerializer<'a> {
    values: &'a [VariantValue],
}

impl<'a> VariantSerializer<'a> {
    pub fn try_create(col: &'a ColumnRef) -> Result<Self> {
        let column: &VariantColumn = Series::check_get(col)?;
        let values = column.values();
        Ok(Self { values })
    }
}

impl<'a> TypeSerializer<'a> for VariantSerializer<'a> {
    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, _format: &FormatSettings) {
        buf.extend_from_slice(self.values[row_index].to_string().as_bytes());
    }

    fn serialize_field(&self, row_index: usize, _format: &FormatSettings) -> Result<String> {
        Ok(self.values[row_index].to_string())
    }

    fn serialize_json(&self, _format: &FormatSettings) -> Result<Vec<Value>> {
        let result: Vec<Value> = self.values.iter().map(|v| v.as_ref().to_owned()).collect();
        Ok(result)
    }

    fn serialize_clickhouse_const(
        &self,
        _format: &FormatSettings,
        size: usize,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let strings: Vec<String> = self.values.iter().map(|v| v.to_string()).collect();
        let mut values: Vec<String> = Vec::with_capacity(self.values.len() * size);
        for _ in 0..size {
            for v in strings.iter() {
                values.push(v.clone())
            }
        }
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }

    fn serialize_clickhouse_column(
        &self,
        _format: &FormatSettings,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let values: Vec<String> = self.values.iter().map(|v| v.to_string()).collect();
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }

    fn serialize_json_object(
        &self,
        valids: Option<&Bitmap>,
        _format: &FormatSettings,
    ) -> Result<Vec<Value>> {
        let mut result: Vec<Value> = Vec::new();
        for (i, v) in self.values.iter().enumerate() {
            if let Some(valids) = valids {
                if !valids.get_bit(i) {
                    result.push(Value::Null);
                    continue;
                }
            }
            match v.as_ref() {
                Value::String(v) => match serde_json::from_str::<Value>(v.as_str()) {
                    Ok(v) => result.push(v),
                    Err(e) => {
                        return Err(ErrorCode::BadDataValueType(format!(
                            "Error parsing JSON: {}",
                            e
                        )))
                    }
                },
                _ => result.push(v.as_ref().to_owned()),
            }
        }
        Ok(result)
    }

    fn serialize_json_object_suppress_error(
        &self,
        _format: &FormatSettings,
    ) -> Result<Vec<Option<Value>>> {
        let result: Vec<Option<Value>> = self
            .values
            .iter()
            .map(|v| match v.as_ref() {
                Value::String(v) => match serde_json::from_str::<Value>(v.as_str()) {
                    Ok(v) => Some(v),
                    Err(_) => None,
                },
                _ => Some(v.as_ref().to_owned()),
            })
            .collect();
        Ok(result)
    }
}
