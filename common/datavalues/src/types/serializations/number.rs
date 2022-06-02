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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use common_io::prelude::Marshal;
use common_io::prelude::Unmarshal;
use opensrv_clickhouse::types::column::ArcColumnWrapper;
use opensrv_clickhouse::types::column::ColumnFrom;
use opensrv_clickhouse::types::HasSqlType;
use serde_json::Value;

use crate::prelude::*;

#[derive(Debug, Clone)]
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
        + opensrv_clickhouse::types::StatBuffer
        + Marshal
        + Unmarshal<T>
        + HasSqlType
        + std::convert::Into<opensrv_clickhouse::types::Value>
        + std::convert::From<opensrv_clickhouse::types::Value>
        + opensrv_clickhouse::io::Marshal
        + opensrv_clickhouse::io::Unmarshal<T>
{
    fn serialize_value(&self, value: &DataValue, _format: &FormatSettings) -> Result<String> {
        Ok(format!("{:?}", value))
    }

    fn serialize_column(
        &self,
        column: &ColumnRef,
        _format: &FormatSettings,
    ) -> Result<Vec<String>> {
        let column: &PrimitiveColumn<T> = Series::check_get(column)?;
        let result: Vec<String> = column.iter().map(|x| x.to_string()).collect();
        Ok(result)
    }

    fn serialize_json(&self, column: &ColumnRef, _format: &FormatSettings) -> Result<Vec<Value>> {
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
        _format: &FormatSettings,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let col: &PrimitiveColumn<T> = Series::check_get(column)?;
        let values: Vec<T> = col.iter().map(|c| c.to_owned()).collect();
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }

    fn serialize_json_object(
        &self,
        column: &ColumnRef,
        _valids: Option<&Bitmap>,
        format: &FormatSettings,
    ) -> Result<Vec<Value>> {
        self.serialize_json(column, format)
    }

    fn serialize_json_object_suppress_error(
        &self,
        column: &ColumnRef,
        _format: &FormatSettings,
    ) -> Result<Vec<Option<Value>>> {
        let column: &PrimitiveColumn<T> = Series::check_get(column)?;
        let result: Vec<Option<Value>> = column
            .iter()
            .map(|x| match serde_json::to_value(x) {
                Ok(v) => Some(v),
                Err(_) => None,
            })
            .collect();
        Ok(result)
    }
}
