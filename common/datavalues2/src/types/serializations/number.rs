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

use std::marker::PhantomData;

use common_clickhouse_srv::types::column::ArcColumnWrapper;
use common_clickhouse_srv::types::column::ColumnFrom;
use common_clickhouse_srv::types::HasSqlType;
use common_exception::Result;
use common_io::prelude::Marshal;
use common_io::prelude::Unmarshal;
use serde_json::Value;

use crate::prelude::*;

pub struct NumberSerializer<T: PrimitiveType> {
    t: PhantomData<T>,
}

impl<T: PrimitiveType> Default for NumberSerializer<T> {
    fn default() -> Self {
        Self {
            t: Default::default(),
        }
    }
}

impl<T> TypeSerializer for NumberSerializer<T>
where T: PrimitiveType
        + common_clickhouse_srv::types::StatBuffer
        + Marshal
        + Unmarshal<T>
        + HasSqlType
        + std::convert::Into<common_clickhouse_srv::types::Value>
        + std::convert::From<common_clickhouse_srv::types::Value>
{
    fn serialize_value(&self, value: &DataValue) -> Result<String> {
        Ok(format!("{:?}", value))
    }

    fn serialize_column(&self, column: &ColumnRef) -> Result<Vec<String>> {
        let column: &PrimitiveColumn<T> = Series::check_get(column)?;
        let result: Vec<String> = column.iter().map(|x| format!("{}", x)).collect();
        Ok(result)
    }

    fn serialize_json(&self, column: &ColumnRef) -> Result<Vec<Value>> {
        let column: &PrimitiveColumn<T> = Series::check_get(column)?;
        let result: Vec<Value> = column
            .iter()
            .map(|x| serde_json::to_value(x).unwrap())
            .collect();
        Ok(result)
    }

    fn serialize_clickhouse_format(
        &self,
        column: &ColumnRef,
    ) -> Result<common_clickhouse_srv::types::column::ArcColumnData> {
        let col: &PrimitiveColumn<T> = Series::check_get(column)?;
        let values: Vec<T> = col.iter().map(|c| c.to_owned()).collect();
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }
}
