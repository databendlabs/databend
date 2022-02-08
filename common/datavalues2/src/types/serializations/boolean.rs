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

use common_clickhouse_srv::types::column::ArcColumnWrapper;
use common_clickhouse_srv::types::column::ColumnFrom;
use common_exception::ErrorCode;
use common_exception::Result;
use serde_json::Value;

use crate::prelude::*;

pub struct BooleanSerializer {}

const TRUE_STR: &str = "1";
const FALSE_STR: &str = "0";

impl TypeSerializer for BooleanSerializer {
    fn serialize_value(&self, value: &DataValue) -> Result<String> {
        if let DataValue::Boolean(x) = value {
            if *x {
                Ok(TRUE_STR.to_owned())
            } else {
                Ok(FALSE_STR.to_owned())
            }
        } else {
            Err(ErrorCode::BadBytes("Incorrect boolean value"))
        }
    }

    fn serialize_column(&self, column: &ColumnRef) -> Result<Vec<String>> {
        let array: &BooleanColumn = Series::check_get(column)?;

        let result: Vec<String> = array
            .iter()
            .map(|v| {
                if v {
                    TRUE_STR.to_owned()
                } else {
                    FALSE_STR.to_owned()
                }
            })
            .collect();
        Ok(result)
    }

    fn serialize_json(&self, column: &ColumnRef) -> Result<Vec<Value>> {
        let array: &BooleanColumn = Series::check_get(column)?;
        let result: Vec<Value> = array
            .iter()
            .map(|v| serde_json::to_value(v).unwrap())
            .collect();
        Ok(result)
    }

    fn serialize_clickhouse_format(
        &self,
        column: &ColumnRef,
    ) -> Result<common_clickhouse_srv::types::column::ArcColumnData> {
        let col: &BooleanColumn = Series::check_get(column)?;
        let values: Vec<u8> = col.iter().map(|c| c as u8).collect();
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }
}
