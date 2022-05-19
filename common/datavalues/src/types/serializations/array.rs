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
use opensrv_clickhouse::types::column::ArrayColumnData;
use serde_json::Value;

use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct ArraySerializer {
    pub inner: Box<TypeSerializerImpl>,
    pub typ: DataTypeImpl,
}

impl TypeSerializer for ArraySerializer {
    fn serialize_value(&self, value: &DataValue, format: &FormatSettings) -> Result<String> {
        if let DataValue::Array(vals) = value {
            let mut res = String::new();
            res.push('[');
            let mut first = true;
            let quoted = self.typ.data_type_id().is_quoted();
            for val in vals {
                if !first {
                    res.push_str(", ");
                }
                first = false;

                let s = self.inner.serialize_value(val, format)?;
                if quoted {
                    res.push_str(&format!("'{}'", s));
                } else {
                    res.push_str(&s);
                }
            }
            res.push(']');
            Ok(res)
        } else {
            Err(ErrorCode::BadBytes("Incorrect Array value"))
        }
    }

    fn serialize_column(&self, column: &ColumnRef, format: &FormatSettings) -> Result<Vec<String>> {
        let column: &ArrayColumn = Series::check_get(column)?;
        let mut result = Vec::with_capacity(column.len());
        for i in 0..column.len() {
            let val = column.get(i);
            let s = self.serialize_value(&val, format)?;
            result.push(s);
        }
        Ok(result)
    }

    fn serialize_json(&self, column: &ColumnRef, _format: &FormatSettings) -> Result<Vec<Value>> {
        let column: &ArrayColumn = Series::check_get(column)?;
        let mut result = Vec::with_capacity(column.len());
        for i in 0..column.len() {
            let val = column.get(i);
            let s = serde_json::to_value(val)?;
            result.push(s);
        }
        Ok(result)
    }

    fn serialize_clickhouse_format(
        &self,
        column: &ColumnRef,
        format: &FormatSettings,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let column: &ArrayColumn = Series::check_get(column)?;
        let mut offsets = opensrv_clickhouse::types::column::List::with_capacity(column.len());
        for offset in column.offsets().iter().skip(1) {
            offsets.push(*offset as u64);
        }

        let inner_data = self
            .inner
            .serialize_clickhouse_format(column.values(), format)?;
        Ok(Arc::new(ArrayColumnData::create(inner_data, offsets)))
    }
}
