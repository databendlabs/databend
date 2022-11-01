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

use common_io::prelude::FormatSettings;
use serde_json;
use serde_json::Value;

use crate::types::string::StringColumn;
use crate::Column;
use crate::TypeSerializer;

#[derive(Debug, Clone)]
pub struct VariantSerializer {
    pub(crate) column: StringColumn,
}

impl VariantSerializer {
    pub fn try_create(col: Column) -> Result<Self, String> {
        let column = col
            .into_variant()
            .map_err(|_| "unable to get variant column".to_string())?;

        Ok(Self { column })
    }
}

impl TypeSerializer for VariantSerializer {
    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, _format: &FormatSettings) {
        let s = unsafe { self.column.index_unchecked(row_index) };
        let value = common_jsonb::to_string(s);
        buf.extend_from_slice(value.as_bytes());
    }

    fn serialize_field(
        &self,
        row_index: usize,
        _format: &FormatSettings,
    ) -> Result<String, String> {
        let s = unsafe { self.column.index_unchecked(row_index) };
        let value = common_jsonb::to_string(s);
        Ok(value)
    }

    fn serialize_json_values(&self, _format: &FormatSettings) -> Result<Vec<Value>, String> {
        let result: Vec<Value> = self
            .column
            .iter()
            .map(|x| {
                let value = common_jsonb::from_slice(x).unwrap();
                value.into()
            })
            .collect();
        Ok(result)
    }
}
