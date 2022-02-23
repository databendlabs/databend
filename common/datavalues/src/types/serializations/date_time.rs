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

use chrono::DateTime;
use chrono_tz::Tz;
use common_clickhouse_srv::types::column::ArcColumnWrapper;
use common_clickhouse_srv::types::column::ColumnFrom;
use common_exception::*;
use serde_json::Value;

use crate::prelude::*;

pub struct DateTimeSerializer<T: PrimitiveType> {
    _marker: PhantomData<T>,
    precision: u32,
    tz: Tz,
}

impl<T: PrimitiveType> DateTimeSerializer<T> {
    pub fn create(tz: Tz, precision: u32) -> Self {
        Self {
            _marker: PhantomData,
            precision,
            tz,
        }
    }

    pub fn to_date_time(&self, value: &T) -> DateTime<Tz> {
        let value = value.to_i64().unwrap();

        match T::SIZE {
            4 => value.to_date_time(&self.tz),
            8 => value.to_date_time64(self.precision as usize, &self.tz),
            _ => unreachable!(),
        }
    }
}

const TIME_FMT: &str = "%Y-%m-%d %H:%M:%S";

impl<T: PrimitiveType> TypeSerializer for DateTimeSerializer<T> {
    fn serialize_value(&self, value: &DataValue) -> Result<String> {
        let value = DFTryFrom::try_from(value.clone())?;
        let dt = self.to_date_time(&value);
        Ok(dt.format(TIME_FMT).to_string())
    }

    fn serialize_column(&self, column: &ColumnRef) -> Result<Vec<String>> {
        let column: &PrimitiveColumn<T> = Series::check_get(column)?;
        let result: Vec<String> = column
            .iter()
            .map(|v| {
                let dt = self.to_date_time(v);
                dt.format(TIME_FMT).to_string()
            })
            .collect();
        Ok(result)
    }

    fn serialize_json(&self, column: &ColumnRef) -> Result<Vec<Value>> {
        let array: &PrimitiveColumn<T> = Series::check_get(column)?;
        let result: Vec<Value> = array
            .iter()
            .map(|v| {
                let dt = self.to_date_time(v);
                serde_json::to_value(dt.format(TIME_FMT).to_string()).unwrap()
            })
            .collect();
        Ok(result)
    }

    fn serialize_clickhouse_format(
        &self,
        column: &ColumnRef,
    ) -> Result<common_clickhouse_srv::types::column::ArcColumnData> {
        let array: &PrimitiveColumn<T> = Series::check_get(column)?;
        let values: Vec<DateTime<Tz>> = array.iter().map(|v| self.to_date_time(v)).collect();
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }
}
