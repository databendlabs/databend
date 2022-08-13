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

use crate::serializations::TypeSerializer;

#[derive(Clone, Debug, Default)]
pub struct NullSerializer {
    pub size: usize,
}

impl<'a> TypeSerializer<'a> for NullSerializer {
    fn need_quote(&self) -> bool {
        false
    }

    fn write_field(&self, _row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        buf.extend_from_slice(&format.null_bytes);
    }

    fn serialize_field(&self, _row_index: usize, format: &FormatSettings) -> Result<String> {
        Ok(unsafe { String::from_utf8_unchecked(format.null_bytes.clone()) })
    }

    fn serialize_json_values(&self, _format: &FormatSettings) -> Result<Vec<Value>> {
        let null = Value::Null;
        let result: Vec<Value> = vec![null; self.size];
        Ok(result)
    }
}
