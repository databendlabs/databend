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

use common_clickhouse_srv::types::column::ArcColumnData;
use common_clickhouse_srv::types::column::TupleColumnData;
use common_exception::ErrorCode;
use common_exception::Result;
use itertools::izip;
use serde_json::Value;

use crate::prelude::*;

pub struct StructSerializer {
    pub names: Vec<String>,
    pub inners: Vec<Box<dyn TypeSerializer>>,
    pub types: Vec<DataTypePtr>,
}

impl TypeSerializer for StructSerializer {
    fn serialize_value(&self, value: &DataValue) -> Result<String> {
        if let DataValue::Struct(vals) = value {
            let mut res = String::new();
            res.push('(');
            let mut first = true;

            for (val, inner, typ) in izip!(vals, &self.inners, &self.types) {
                if !first {
                    res.push_str(", ");
                }
                first = false;

                let s = inner.serialize_value(val)?;
                if typ.data_type_id().is_quoted() {
                    res.push_str(&format!("'{}'", s));
                } else {
                    res.push_str(&s);
                }
            }
            res.push(')');
            Ok(res)
        } else {
            Err(ErrorCode::BadBytes("Incorrect Struct value"))
        }
    }

    fn serialize_column(&self, column: &ColumnRef) -> Result<Vec<String>> {
        let column: &StructColumn = Series::check_get(column)?;
        let mut result = Vec::with_capacity(column.len());
        for i in 0..column.len() {
            let val = column.get(i);
            let s = self.serialize_value(&val)?;
            result.push(s);
        }
        Ok(result)
    }

    fn serialize_json(&self, _column: &ColumnRef) -> Result<Vec<Value>> {
        // let column: &StructColumn = Series::check_get(column)?;
        // let inner_columns = column.values();
        // let result = self
        //     .inners
        //     .iter()
        //     .zip(inner_columns.iter())
        //     .map(|(inner, col)| inner.serialize_json(col))
        //     .collect::<Result<Vec<Vec<Value>>>>()?;
        todo!()
    }

    fn serialize_clickhouse_format(&self, column: &ColumnRef) -> Result<ArcColumnData> {
        let column: &StructColumn = Series::check_get(column)?;
        let result = self
            .inners
            .iter()
            .zip(column.values().iter())
            .map(|(inner, col)| inner.serialize_clickhouse_format(col))
            .collect::<Result<Vec<ArcColumnData>>>()?;

        let data = TupleColumnData { inner: result };
        Ok(Arc::new(data))
    }
}
