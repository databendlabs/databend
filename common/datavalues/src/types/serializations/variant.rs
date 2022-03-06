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
use opensrv_clickhouse::types::column::ArcColumnWrapper;
use opensrv_clickhouse::types::column::ColumnFrom;
use serde_json;
use serde_json::Value as JsonValue;

use crate::prelude::*;

pub struct VariantSerializer {}

impl TypeSerializer for VariantSerializer {
    fn serialize_value(&self, value: &DataValue) -> Result<String> {
        if let DataValue::Json(v) = value {
            Ok(format!("{:#}", v))
        } else {
            Err(ErrorCode::BadBytes("Incorrect Variant value"))
        }
    }

    fn serialize_column(&self, column: &ColumnRef) -> Result<Vec<String>> {
        let column: &JsonColumn = Series::check_get(column)?;
        let result: Vec<String> = column.iter().map(|v| format!("{:#}", v)).collect();
        Ok(result)
    }

    fn serialize_json(&self, column: &ColumnRef) -> Result<Vec<JsonValue>> {
        let column: &JsonColumn = Series::check_get(column)?;
        let result: Vec<JsonValue> = column.iter().map(|v| v.to_owned()).collect();
        Ok(result)
    }

    fn serialize_clickhouse_format(
        &self,
        column: &ColumnRef,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let column: &JsonColumn = Series::check_get(column)?;
        let values: Vec<String> = column.iter().map(|v| format!("{:#}", v)).collect();

        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }
}
