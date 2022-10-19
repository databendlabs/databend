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
use serde_json::Value;

use crate::Column;
use crate::TypeSerializer;

#[derive(Clone)]
pub struct BooleanSerializer {
    pub(crate) values: Bitmap,
}

impl BooleanSerializer {
    pub fn try_create(col: Column) -> Result<Self, String> {
        let values = col
            .into_boolean()
            .map_err(|_| "unable to get boolean column".to_string())?;

        // let values = col.into_boolean().unwrap();

        Ok(Self { values })
    }
}

impl<'a> TypeSerializer<'a> for BooleanSerializer {
    fn need_quote(&self) -> bool {
        false
    }

    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        let v = if self.values.get_bit(row_index) {
            &format.true_bytes
        } else {
            &format.false_bytes
        };
        buf.extend_from_slice(v);
    }

    fn serialize_json_values(&self, _format: &FormatSettings) -> Result<Vec<Value>> {
        let result: Vec<Value> = self
            .values
            .iter()
            .map(|v| serde_json::to_value(v).unwrap())
            .collect();
        Ok(result)
    }
}
