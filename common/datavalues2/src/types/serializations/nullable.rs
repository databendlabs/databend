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

use common_clickhouse_srv::types::column::NullableColumnData;
use common_exception::Result;
use serde_json::Value;

use crate::prelude::DataValue;
use crate::Column;
use crate::ColumnRef;
use crate::NullableColumn;
use crate::Series;
use crate::TypeSerializer;

pub struct NullableSerializer {
    pub inner: Box<dyn TypeSerializer>,
}

impl TypeSerializer for NullableSerializer {
    fn serialize_value(&self, value: &DataValue) -> Result<String> {
        if value.is_null() {
            Ok("NULL".to_owned())
        } else {
            self.inner.serialize_value(value)
        }
    }

    fn serialize_column(&self, column: &ColumnRef) -> Result<Vec<String>> {
        let column: &NullableColumn = Series::check_get(column)?;
        let rows = column.len();
        let mut res = self.inner.serialize_column(column.inner())?;

        (0..rows).for_each(|row| {
            if column.null_at(row) {
                res[row] = "NULL".to_owned();
            }
        });
        Ok(res)
    }

    fn serialize_json(&self, column: &ColumnRef) -> Result<Vec<Value>> {
        let column: &NullableColumn = Series::check_get(column)?;
        let rows = column.len();
        let mut res = self.inner.serialize_json(column.inner())?;

        (0..rows).for_each(|row| {
            if column.null_at(row) {
                res[row] = Value::Null;
            }
        });
        Ok(res)
    }

    fn serialize_clickhouse_format(
        &self,
        column: &ColumnRef,
    ) -> Result<common_clickhouse_srv::types::column::ArcColumnData> {
        let column: &NullableColumn = Series::check_get(column)?;
        let inner = self.inner.serialize_clickhouse_format(column.inner())?;
        let nulls = column.ensure_validity().iter().map(|v| !v as u8).collect();
        let data = NullableColumnData { nulls, inner };

        Ok(Arc::new(data))
    }
}
