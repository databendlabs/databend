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

use chrono::DateTime;
use chrono_tz::Tz;
use common_arrow::arrow::buffer::Buffer;
use common_io::prelude::FormatSettings;
use serde_json::Value;

use crate::utils::date_helper::DateConverter;
use crate::Column;
use crate::TypeSerializer;

const TIME_FMT: &str = "%Y-%m-%d %H:%M:%S";

#[derive(Debug, Clone)]
pub struct TimestampSerializer {
    pub(crate) values: Buffer<i64>,
}

impl TimestampSerializer {
    pub fn try_create(col: Column) -> Result<Self, String> {
        let values = col
            .into_timestamp()
            .map_err(|_| "unable to get timestamp column".to_string())?;

        Ok(Self { values })
    }

    pub fn to_timestamp(&self, value: &i64, tz: &Tz) -> DateTime<Tz> {
        value.to_timestamp(tz)
    }
}

impl TypeSerializer for TimestampSerializer {
    fn need_quote(&self) -> bool {
        true
    }

    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        let dt = self.to_timestamp(&self.values[row_index], &format.timezone);
        let s = dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        buf.extend_from_slice(s.as_bytes())
    }

    fn serialize_json_values(&self, format: &FormatSettings) -> Result<Vec<Value>, String> {
        let result: Vec<Value> = self
            .values
            .iter()
            .map(|v| {
                let dt = self.to_timestamp(v, &format.timezone);
                serde_json::to_value(dt.format(TIME_FMT).to_string()).unwrap()
            })
            .collect();
        Ok(result)
    }
}
