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

use std::ops::AddAssign;

use chrono::Duration;
use chrono::NaiveDate;
use common_arrow::arrow::buffer::Buffer;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use serde_json::Value;

use crate::Column;
use crate::TypeSerializer;

const DATE_FMT: &str = "%Y-%m-%d";

#[derive(Debug, Clone)]
pub struct DateSerializer {
    pub(crate) values: Buffer<i32>,
}

impl DateSerializer {
    pub fn try_create(col: Column) -> Result<Self, String> {
        let values = col
            .into_date()
            .map_err(|_| "unable to get date column".to_string())?;

        Ok(Self { values })
    }

    pub fn to_date(&self, v: &i32) -> String {
        let mut date = NaiveDate::from_ymd(1970, 1, 1);
        let d = Duration::days((*v).into());
        date.add_assign(d);
        date.format(DATE_FMT).to_string()
    }
}

impl TypeSerializer for DateSerializer {
    fn need_quote(&self) -> bool {
        true
    }

    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, _format: &FormatSettings) {
        let s = self.to_date(&self.values[row_index]);
        buf.extend_from_slice(s.as_bytes())
    }

    fn serialize_json_values(&self, _format: &FormatSettings) -> Result<Vec<Value>, String> {
        let result: Vec<Value> = (0..self.values.len())
            .map(|row_index| {
                let s = self.to_date(&self.values[row_index]);
                serde_json::to_value(s).unwrap()
            })
            .collect();
        Ok(result)
    }
}
