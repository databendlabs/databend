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
use opensrv_clickhouse::types::column::ArcColumnWrapper;
use opensrv_clickhouse::types::column::ColumnFrom;
use serde_json::Value;

use crate::ColumnRef;
use crate::DateConverter;
use crate::PrimitiveColumn;
use crate::Series;
use crate::TypeSerializer;

const TIME_FMT: &str = "%Y-%m-%d %H:%M:%S";

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
}

impl<'a> TypeSerializer<'a> for TimestampSerializer<'a> {
    fn need_quote(&self) -> bool {
        true
    }

    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        let dt = self.to_timestamp(&self.values[row_index], &format.timezone);
        let s = dt.format(TIME_FMT).to_string();
        buf.extend_from_slice(s.as_bytes())
    }

    fn serialize_json(&self, format: &FormatSettings) -> Result<Vec<Value>> {
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

    fn serialize_clickhouse_const(
        &self,
        format: &FormatSettings,
        size: usize,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let times: Vec<DateTime<Tz>> = self
            .values
            .iter()
            .map(|v| self.to_timestamp(v, &format.timezone))
            .collect();
        let mut values: Vec<DateTime<Tz>> = Vec::with_capacity(self.values.len() * size);
        for _ in 0..size {
            for v in times.iter() {
                values.push(*v)
            }
        }
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }

    fn serialize_clickhouse_column(
        &self,
        format: &FormatSettings,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let values: Vec<DateTime<Tz>> = self
            .values
            .iter()
            .map(|v| self.to_timestamp(v, &format.timezone))
            .collect();
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }
}
