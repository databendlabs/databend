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

use std::fmt;
use std::marker::PhantomData;
use std::ops::Sub;
use std::sync::Arc;

use common_datavalues::chrono::Date;
use common_datavalues::chrono::NaiveDate;
use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone, Debug)]
pub struct SimpleFunction<T> {
    display_name: String,
    t: PhantomData<T>,
}

pub trait NoArgDateFunction {
    const IS_DETERMINISTIC: bool;

    fn execute() -> u16;
}

#[derive(Clone)]
pub struct Today;

impl NoArgDateFunction for Today {
    const IS_DETERMINISTIC: bool = false;

    fn execute() -> u16 {
        let utc: Date<Utc> = Utc::now().date();
        let epoch = NaiveDate::from_ymd(1970, 1, 1);

        let duration = utc.naive_utc().sub(epoch);
        duration.num_days() as u16
    }
}

#[derive(Clone)]
pub struct Yesterday;

impl NoArgDateFunction for Yesterday {
    const IS_DETERMINISTIC: bool = false;

    fn execute() -> u16 {
        let utc: Date<Utc> = Utc::now().date();
        let epoch = NaiveDate::from_ymd(1970, 1, 1);

        let duration = utc.naive_utc().sub(epoch);
        duration.num_days() as u16 - 1
    }
}

#[derive(Clone)]
pub struct Tomorrow;

impl NoArgDateFunction for Tomorrow {
    const IS_DETERMINISTIC: bool = false;

    fn execute() -> u16 {
        let utc: Date<Utc> = Utc::now().date();
        let epoch = NaiveDate::from_ymd(1970, 1, 1);

        let duration = utc.naive_utc().sub(epoch);
        duration.num_days() as u16 + 1
    }
}

impl<T> SimpleFunction<T>
where T: NoArgDateFunction + Clone + Sync + Send + 'static
{
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(SimpleFunction::<T> {
            display_name: display_name.to_string(),
            t: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        let mut features = FunctionFeatures::default();

        if T::IS_DETERMINISTIC {
            features = features.deterministic();
        }

        FunctionDescription::creator(Box::new(Self::try_create)).features(features)
    }
}

impl<T> Function for SimpleFunction<T>
where T: NoArgDateFunction + Clone + Sync + Send + 'static
{
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(
        &self,
        _args: &[&common_datavalues::DataTypePtr],
    ) -> Result<common_datavalues::DataTypePtr> {
        Ok(Date16Type::arc())
    }

    fn eval(
        &self,
        _columns: &common_datavalues::ColumnsWithField,
        input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        let value = T::execute();
        let column = Series::from_data(&[value as u16]);
        Ok(Arc::new(ConstColumn::new(column, input_rows)))
    }
}

impl<T> fmt::Display for SimpleFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

pub type TodayFunction = SimpleFunction<Today>;
pub type YesterdayFunction = SimpleFunction<Yesterday>;
pub type TomorrowFunction = SimpleFunction<Tomorrow>;
