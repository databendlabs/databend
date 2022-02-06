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

use common_clickhouse_srv::types::column::ArcColumnWrapper;
use common_clickhouse_srv::types::column::ColumnFrom;
use common_clickhouse_srv::types::column::NullableColumnData;
use common_exception::Result;
use serde_json::Value;

use crate::prelude::DataValue;
use crate::ColumnRef;
use crate::TypeSerializer;

#[derive(Clone, Debug, Default)]
pub struct NullSerializer {}

const NULL_STR: &str = "NULL";

impl TypeSerializer for NullSerializer {
    fn serialize_value(&self, _value: &DataValue) -> Result<String> {
        Ok(NULL_STR.to_owned())
    }

    fn serialize_column(&self, column: &ColumnRef) -> Result<Vec<String>> {
        let result: Vec<String> = vec![NULL_STR.to_owned(); column.len()];
        Ok(result)
    }

    fn serialize_json(&self, column: &ColumnRef) -> Result<Vec<Value>> {
        let null = Value::Null;
        let result: Vec<Value> = vec![null; column.len()];
        Ok(result)
    }

    fn serialize_clickhouse_format(
        &self,
        column: &ColumnRef,
    ) -> Result<common_clickhouse_srv::types::column::ArcColumnData> {
        let nulls = vec![1u8; column.len()];
        let inner = Vec::column_from::<ArcColumnWrapper>(vec![1u8; column.len()]);
        let data = NullableColumnData { nulls, inner };
        Ok(Arc::new(data))
    }
}
