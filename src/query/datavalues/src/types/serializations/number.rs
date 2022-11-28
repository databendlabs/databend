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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use micromarshal::Marshal;
use micromarshal::Unmarshal;
use serde_json::Value;

use crate::ColumnRef;
use crate::PrimitiveColumn;
use crate::PrimitiveType;
use crate::Series;
use crate::TypeSerializer;

#[derive(Debug, Clone)]
pub struct NumberSerializer<'a, T: PrimitiveType> {
    pub values: &'a [T],
}

impl<'a, T: PrimitiveType> NumberSerializer<'a, T> {
    pub fn try_create(col: &'a ColumnRef) -> Result<Self> {
        let col: &PrimitiveColumn<T> = Series::check_get(col)?;
        Ok(NumberSerializer {
            values: col.values(),
        })
    }
}

impl<'a, T> TypeSerializer<'a> for NumberSerializer<'a, T>
where T: PrimitiveType + Marshal + Unmarshal<T> + lexical_core::ToLexical
{
    fn serialize_json_values(&self, _format: &FormatSettings) -> Result<Vec<Value>> {
        let result: Vec<Value> = self
            .values
            .iter()
            .map(|x| serde_json::to_value(x).unwrap())
            .collect();
        Ok(result)
    }

    fn serialize_json_object(
        &self,
        _valids: Option<&Bitmap>,
        format: &FormatSettings,
    ) -> Result<Vec<Value>> {
        self.serialize_json_values(format)
    }

    fn serialize_json_object_suppress_error(
        &self,
        _format: &FormatSettings,
    ) -> Result<Vec<Option<Value>>> {
        let result: Vec<Option<Value>> = self
            .values
            .iter()
            .map(|x| match serde_json::to_value(x) {
                Ok(v) => Some(v),
                Err(_) => None,
            })
            .collect();
        Ok(result)
    }
}
