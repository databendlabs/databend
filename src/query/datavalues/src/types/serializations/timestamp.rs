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

use chrono::DateTime;
use chrono_tz::Tz;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use serde_json::Value;

use crate::ColumnRef;
use crate::DateConverter;
use crate::PrimitiveColumn;
use crate::Series;
use crate::TypeSerializer;

const TIME_FMT: &str = "%Y-%m-%d %H:%M:%S";
const TIME_FMT_MICRO: &str = "%Y-%m-%d %H:%M:%S%.6f";

#[derive(Debug, Clone)]
pub struct TimestampSerializer<'a> {
    pub(crate) values: &'a [i64],
}

impl<'a> TimestampSerializer<'a> {
    pub fn try_create(col: &'a ColumnRef) -> Result<Self> {
        let col: &PrimitiveColumn<i64> = Series::check_get(col)?;
        Ok(Self {
            values: col.values(),
        })
    }

    pub fn to_timestamp(&self, value: &i64, tz: &Tz) -> DateTime<Tz> {
        value.to_timestamp(tz)
    }

    pub fn to_string_micro(&self, row_index: usize, tz: &Tz) -> String {
        let dt = self.to_timestamp(&self.values[row_index], tz);
        dt.format(TIME_FMT_MICRO).to_string()
    }
}

impl<'a> TypeSerializer<'a> for TimestampSerializer<'a> {
    fn serialize_json_values(&self, format: &FormatSettings) -> Result<Vec<Value>> {
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
