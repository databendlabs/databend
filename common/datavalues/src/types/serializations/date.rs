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

use std::ops::AddAssign;

use chrono::Date;
use chrono::Duration;
use chrono::NaiveDate;
use chrono_tz::Tz;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use lexical_core::ToLexical;
use num::cast::AsPrimitive;
use opensrv_clickhouse::types::column::ArcColumnData;
use opensrv_clickhouse::types::column::ArcColumnWrapper;
use opensrv_clickhouse::types::column::ColumnFrom;
use serde_json::Value;

use crate::serializations::TypeSerializer;
use crate::ColumnRef;
use crate::DateConverter;
use crate::PrimitiveColumn;
use crate::PrimitiveType;
use crate::Series;

const DATE_FMT: &str = "%Y-%m-%d";

#[derive(Debug, Clone)]
pub struct DateSerializer<'a, T: PrimitiveType + AsPrimitive<i64> + ToLexical> {
    pub(crate) values: &'a [T],
}

fn v_to_string(v: &i64) -> String {
    let mut date = NaiveDate::from_ymd(1970, 1, 1);
    let d = Duration::days(*v);
    date.add_assign(d);
    date.format(DATE_FMT).to_string()
}

impl<'a, T: PrimitiveType + AsPrimitive<i64> + ToLexical> DateSerializer<'a, T> {
    pub fn try_create(col: &'a ColumnRef) -> Result<Self> {
        let col: &PrimitiveColumn<T> = Series::check_get(col)?;
        Ok(Self {
            values: col.values(),
        })
    }
}

impl<'a, T: PrimitiveType + AsPrimitive<i64> + ToLexical> TypeSerializer<'a>
    for DateSerializer<'a, T>
{
    fn need_quote(&self) -> bool {
        true
    }

    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, _format: &FormatSettings) {
        let s = v_to_string(&self.values[row_index].as_i64());
        buf.extend_from_slice(s.as_bytes())
    }

    fn serialize_json_values(&self, _format: &FormatSettings) -> Result<Vec<Value>> {
        let result: Vec<Value> = (0..self.values.len())
            .map(|row_index| {
                let s = v_to_string(&self.values[row_index].as_i64());
                serde_json::to_value(s).unwrap()
            })
            .collect();
        Ok(result)
    }

    fn serialize_clickhouse_const(
        &self,
        _format: &FormatSettings,
        size: usize,
    ) -> Result<ArcColumnData> {
        let tz: Tz = "UTC".parse().unwrap();
        let dates: Vec<Date<Tz>> = self.values.iter().map(|v| v.to_date(&tz)).collect();
        let mut values: Vec<Date<Tz>> = Vec::with_capacity(self.values.len() * size);
        for _ in 0..size {
            for v in dates.iter() {
                values.push(*v)
            }
        }
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }

    fn serialize_clickhouse_column(
        &self,
        _format: &FormatSettings,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let tz: Tz = "UTC".parse().unwrap();
        let values: Vec<Date<Tz>> = self.values.iter().map(|v| v.to_date(&tz)).collect();
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }
}
