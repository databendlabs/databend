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

use common_exception::Result;
use common_io::prelude::FormatSettings;
use serde_json::Value;

use crate::prelude::*;
use crate::serializations::write_csv_string;
use crate::serializations::write_json_string;

#[derive(Clone)]
pub struct StructSerializer<'a> {
    pub(crate) inners: Vec<TypeSerializerImpl<'a>>,
    pub(crate) column: &'a ColumnRef,
}

impl<'a> TypeSerializer<'a> for StructSerializer<'a> {
    fn write_field_values(
        &self,
        row_index: usize,
        buf: &mut Vec<u8>,
        format: &FormatSettings,
        _in_nested: bool,
    ) {
        buf.push(b'(');
        let mut first = true;

        for inner in &self.inners {
            if !first {
                buf.extend_from_slice(b", ");
            }
            first = false;
            inner.write_field_values(row_index, buf, format, true);
        }
        buf.push(b')');
    }

    fn write_field_tsv(
        &self,
        row_index: usize,
        buf: &mut Vec<u8>,
        format: &FormatSettings,
        _in_nested: bool,
    ) {
        buf.push(b'(');
        let mut first = true;

        for inner in &self.inners {
            if !first {
                buf.extend_from_slice(b", ");
            }
            first = false;
            inner.write_field_tsv(row_index, buf, format, true);
        }
        buf.push(b')');
    }

    fn write_field_csv(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        let v = self.to_vec_values(row_index, format);
        write_csv_string(&v, buf, format.quote_char);
    }

    fn write_field_json(
        &self,
        row_index: usize,
        buf: &mut Vec<u8>,
        format: &FormatSettings,
        quote: bool,
    ) {
        let v = self.to_vec_values(row_index, format);
        if quote {
            buf.push(b'\"');
        }
        write_json_string(&v, buf, format);
        if quote {
            buf.push(b'\"');
        }
    }

    fn serialize_json_values(&self, _format: &FormatSettings) -> Result<Vec<Value>> {
        let column = self.column;
        let mut result = Vec::with_capacity(column.len());
        for i in 0..column.len() {
            let val = column.get(i);
            let s = serde_json::to_value(val)?;
            result.push(s);
        }
        Ok(result)
    }
}
