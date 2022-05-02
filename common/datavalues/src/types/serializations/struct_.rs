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

use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use itertools::izip;
use opensrv_clickhouse::types::column::ArcColumnData;
use opensrv_clickhouse::types::column::TupleColumnData;
use serde_json::Value;

use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct StructSerializer {
    pub names: Vec<String>,
    pub inners: Vec<Box<TypeSerializerImpl>>,
    pub types: Vec<DataTypeImpl>,
}

impl TypeSerializer for StructSerializer {
    fn serialize_value(&self, value: &DataValue, format: Arc<FormatSettings>) -> Result<String> {
        if let DataValue::Struct(vals) = value {
            let mut res = String::new();
            res.push('(');
            let mut first = true;

            for (val, inner, typ) in izip!(vals, &self.inners, &self.types) {
                if !first {
                    res.push_str(", ");
                }
                first = false;

                let s = inner.serialize_value(val, format.clone())?;
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

    fn serialize_column(
        &self,
        column: &ColumnRef,
        format: Arc<FormatSettings>,
    ) -> Result<Vec<String>> {
        let column: &StructColumn = Series::check_get(column)?;
        let mut result = Vec::with_capacity(column.len());
        for i in 0..column.len() {
            let val = column.get(i);
            let s = self.serialize_value(&val, format.clone())?;
            result.push(s);
        }
        Ok(result)
    }

    fn serialize_json(
        &self,
        _column: &ColumnRef,
        _format: Arc<FormatSettings>,
    ) -> Result<Vec<Value>> {
        todo!()
    }

    fn serialize_clickhouse_format(
        &self,
        column: &ColumnRef,
        format: Arc<FormatSettings>,
    ) -> Result<ArcColumnData> {
        let column: &StructColumn = Series::check_get(column)?;
        let result = self
            .inners
            .iter()
            .zip(column.values().iter())
            .map(|(inner, col)| inner.serialize_clickhouse_format(col, format.clone()))
            .collect::<Result<Vec<ArcColumnData>>>()?;

        let data = TupleColumnData { inner: result };
        Ok(Arc::new(data))
    }
}
