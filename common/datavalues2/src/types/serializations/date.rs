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

use std::marker::PhantomData;
use std::ops::AddAssign;

use chrono::Date;
use chrono::Duration;
use chrono::NaiveDate;
use chrono_tz::Tz;
use common_clickhouse_srv::types::column::ArcColumnWrapper;
use common_clickhouse_srv::types::column::ColumnFrom;
use common_exception::*;
use num::cast::AsPrimitive;
use serde_json::Value;

use crate::prelude::*;

pub struct DateSerializer<T: PrimitiveType + AsPrimitive<i64>> {
    t: PhantomData<T>,
}

impl<T: PrimitiveType + AsPrimitive<i64>> Default for DateSerializer<T> {
    fn default() -> Self {
        Self {
            t: Default::default(),
        }
    }
}

const DATE_FMT: &str = "%Y-%m-%d";

impl<T: PrimitiveType + AsPrimitive<i64>> TypeSerializer for DateSerializer<T> {
    fn serialize_value(&self, value: &DataValue) -> Result<String> {
        let mut date = NaiveDate::from_ymd(1970, 1, 1);
        let d = Duration::days(value.as_i64()?);
        date.add_assign(d);
        Ok(date.format(DATE_FMT).to_string())
    }

    fn serialize_column(&self, column: &ColumnRef) -> Result<Vec<String>> {
        let column: &PrimitiveColumn<T> = Series::check_get(column)?;

        let result: Vec<String> = column
            .iter()
            .map(|v| {
                let mut date = NaiveDate::from_ymd(1970, 1, 1);
                let d = Duration::days(v.to_i64().unwrap());
                date.add_assign(d);
                date.format(DATE_FMT).to_string()
            })
            .collect();
        Ok(result)
    }

    fn serialize_json(&self, column: &ColumnRef) -> Result<Vec<Value>> {
        let array: &PrimitiveColumn<T> = Series::check_get(column)?;
        let result: Vec<Value> = array
            .iter()
            .map(|v| {
                let mut date = NaiveDate::from_ymd(1970, 1, 1);
                let d = Duration::days(v.to_i64().unwrap());
                date.add_assign(d);
                let str = date.format(DATE_FMT).to_string();
                serde_json::to_value(str).unwrap()
            })
            .collect();
        Ok(result)
    }

    fn serialize_clickhouse_format(
        &self,
        column: &ColumnRef,
    ) -> Result<common_clickhouse_srv::types::column::ArcColumnData> {
        let array: &PrimitiveColumn<T> = Series::check_get(column)?;
        let tz: Tz = "UTC".parse().unwrap();

        let values: Vec<Date<Tz>> = array.iter().map(|v| v.to_date(&tz)).collect();
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }
}
