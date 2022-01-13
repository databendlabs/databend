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

use chrono::Duration;
use chrono::NaiveDate;
use common_exception::*;

use crate::prelude::*;

pub struct DateSerializer<T: PrimitiveType> {
    t: PhantomData<T>,
}

impl<T: PrimitiveType> Default for DateSerializer<T> {
    fn default() -> Self {
        Self {
            t: Default::default(),
        }
    }
}

impl<T: PrimitiveType> TypeSerializer for DateSerializer<T> {
    fn serialize_value(&self, value: &DataValue) -> Result<String> {
        let mut date = NaiveDate::from_ymd(1970, 1, 1);
        let d = Duration::days(value.as_i64()?);
        date.add_assign(d);
        Ok(date.format("%Y-%m-%d").to_string())
    }

    fn serialize_column(&self, column: &ColumnRef) -> Result<Vec<String>> {
        let data_type = create_primitive_datatype::<T>();
        let column: &PrimitiveColumn<T> = unsafe { Series::static_cast(column) };

        let result: Vec<String> = column
            .iter()
            .map(|v| {
                let mut date = NaiveDate::from_ymd(1970, 1, 1);
                let d = Duration::days(v.to_i64().unwrap());
                date.add_assign(d);
                date.format("%Y-%m-%d").to_string()
            })
            .collect();
        Ok(result)
    }
}
