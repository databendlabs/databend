// Copyright 2020 Datafuse Labs.
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
use common_exception::Result;

use crate::prelude::DFPrimitiveArray;
use crate::prelude::DataColumn;
use crate::DFPrimitiveType;
use crate::TypeSerializer;

pub struct DateSerializer<T: DFPrimitiveType> {
    t: PhantomData<T>,
}

impl<T: DFPrimitiveType> Default for DateSerializer<T> {
    fn default() -> Self {
        Self {
            t: Default::default(),
        }
    }
}

impl<T: DFPrimitiveType> TypeSerializer for DateSerializer<T> {
    fn serialize_strings(&self, column: &DataColumn) -> Result<Vec<String>> {
        let array = column.to_array()?;
        let array: &DFPrimitiveArray<T> = array.static_cast();

        let result: Vec<String> = array
            .iter()
            .map(|x| {
                x.map(|v| {
                    let mut date = NaiveDate::from_num_days_from_ce(0);
                    let d = Duration::days(v.to_i64().unwrap());
                    date.add_assign(d);
                    date.format("%Y-%m-%d").to_string()
                })
                .unwrap_or_else(|| "NULL".to_owned())
            })
            .collect();
        Ok(result)
    }
}
