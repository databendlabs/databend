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
use opensrv_clickhouse::types::column::ArcColumnWrapper;
use opensrv_clickhouse::types::column::ColumnFrom;
use serde_json;
use serde_json::Value;

use crate::prelude::*;

pub struct VariantSerializer {}

impl TypeSerializer for VariantSerializer {
    fn serialize_value(&self, value: &DataValue) -> Result<String> {
        if let DataValue::Variant(v) = value {
            Ok(v.to_string())
        } else {
            Err(ErrorCode::BadBytes("Incorrect Variant value"))
        }
    }

    fn serialize_column(&self, column: &ColumnRef) -> Result<Vec<String>> {
        let column: &VariantColumn = Series::check_get(column)?;
        let result: Vec<String> = column.iter().map(|v| v.to_string()).collect();
        Ok(result)
    }

    fn serialize_json(&self, column: &ColumnRef) -> Result<Vec<Value>> {
        let column: &VariantColumn = Series::check_get(column)?;
        let result: Vec<Value> = column.iter().map(|v| v.as_ref().to_owned()).collect();
        Ok(result)
    }

    fn serialize_clickhouse_format(
        &self,
        column: &ColumnRef,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let column: &VariantColumn = Series::check_get(column)?;
        let values: Vec<String> = column.iter().map(|v| v.to_string()).collect();

        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }

    fn serialize_json_object(
        &self,
        column: &ColumnRef,
        valids: Option<&Bitmap>,
    ) -> Result<Vec<Value>> {
        let column: &VariantColumn = Series::check_get(column)?;
        let mut result: Vec<Value> = Vec::new();
        for (i, v) in column.iter().enumerate() {
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
        column: &ColumnRef,
    ) -> Result<Vec<Option<Value>>> {
        let column: &VariantColumn = Series::check_get(column)?;
        let result: Vec<Option<Value>> = column
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
