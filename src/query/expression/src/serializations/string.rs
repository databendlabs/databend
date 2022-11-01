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
use serde_json::Value;

pub use super::helper::json::write_json_string;
use crate::serializations::helper::escape::write_escaped_string;
use crate::types::string::StringColumn;
use crate::Column;
use crate::TypeSerializer;

#[derive(Debug, Clone)]
pub struct StringSerializer {
    pub(crate) column: StringColumn,
}

impl StringSerializer {
    pub fn try_create(col: Column) -> Result<Self, String> {
        let column = col
            .into_string()
            .map_err(|_| "unable to get string column".to_string())?;

        Ok(Self { column })
    }
}

impl TypeSerializer for StringSerializer {
    fn need_quote(&self) -> bool {
        true
    }

    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, _format: &FormatSettings) {
        buf.extend_from_slice(unsafe { self.column.index_unchecked(row_index) });
    }

    fn write_field_escaped(
        &self,
        row_index: usize,
        buf: &mut Vec<u8>,
        _format: &FormatSettings,
        quote: u8,
    ) {
        write_escaped_string(
            unsafe { self.column.index_unchecked(row_index) },
            buf,
            quote,
        )
    }

    fn write_field_json(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        buf.push(b'\"');
        write_json_string(
            unsafe { self.column.index_unchecked(row_index) },
            buf,
            format,
        );
        buf.push(b'\"');
    }

    fn serialize_json_values(&self, _format: &FormatSettings) -> Result<Vec<Value>, String> {
        let result: Vec<Value> = self
            .column
            .iter()
            .map(|x| serde_json::to_value(String::from_utf8_lossy(x).to_string()).unwrap())
            .collect();
        Ok(result)
    }
}
