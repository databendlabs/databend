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

use common_datavalues2::chrono::Datelike;
use common_datavalues2::chrono::Duration;
use common_datavalues2::chrono::NaiveDate;
use common_datavalues2::chrono::NaiveDateTime;
use common_datavalues2::prelude::*;
use common_datavalues2::with_match_primitive_types_error;
use common_exception::ErrorCode;
use common_exception::Result;
use num_traits::AsPrimitive;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::ArithmeticCreator;
use crate::scalars::ArithmeticDescription;
use crate::scalars::Function2;

pub trait IntervalArithmetic<L: Scalar, R: Scalar, O: Scalar> {
    fn eval(&self, l: L::RefType<'_>, r: R::RefType<'_>, factor: i64) -> Result<O>;
}

impl<L: Scalar, R: Scalar, O: Scalar, F> IntervalArithmetic<L, R, O> for F
where F: Fn(L::RefType<'_>, R::RefType<'_>, i64) -> Result<O>
{
    fn eval(&self, i1: L::RefType<'_>, i2: R::RefType<'_>, factor: i64) -> Result<O> {
        self(i1, i2, factor)
    }
}

#[derive(Clone)]
pub struct IntervalFunction<L: IntegerType, R: PrimitiveType, O: IntegerType, F> {
    display_name: String,
    result_type: DataTypePtr,
    func: F,
    factor: i64,
    _phantom: PhantomData<(L, R, O)>,
}

impl<L, R, O, F> IntervalFunction<L, R, O, F>
where
    L: IntegerType + Send + Sync + Clone,
    R: PrimitiveType + Send + Sync + Clone,
    O: IntegerType + Send + Sync + Clone,
    F: IntervalArithmetic<L, R, O> + Send + Sync + Clone + 'static,
{
    pub fn try_create_func(
        display_name: &str,
        result_type: DataTypePtr,
        func: F,
        factor: i64,
    ) -> Result<Box<dyn Function2>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
            result_type,
            func,
            factor,
            _phantom: PhantomData,
        }))
    }
}

impl<L, R, O, F> Function2 for IntervalFunction<L, R, O, F>
where
    L: IntegerType + Send + Sync + Clone,
    R: PrimitiveType + Send + Sync + Clone,
    O: IntegerType + Send + Sync + Clone,
    F: IntervalArithmetic<L, R, O> + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str {
        "IntervalFunction"
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(self.result_type.clone())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let left = ColumnViewerIter::<L>::try_create(columns[0].column())?;
        let right = ColumnViewerIter::<R>::try_create(columns[1].column())?;
        let mut col_builder = MutablePrimitiveColumn::<O>::with_capacity(left.size);
        for (l, r) in left.zip(right) {
            let o = self.func.eval(l, r, self.factor)?;
            col_builder.append_value(o);
        }
        Ok(col_builder.to_column())
    }
}

impl<L, R, O, F> fmt::Display for IntervalFunction<L, R, O, F>
where
    L: IntegerType + Send + Sync + Clone,
    R: PrimitiveType + Send + Sync + Clone,
    O: IntegerType + Send + Sync + Clone,
    F: IntervalArithmetic<L, R, O> + Send + Sync + Clone + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

pub type AddYearsFunction = IntervalFunctionCreator<AddYearsImpl>;

pub struct IntervalFunctionCreator<T> {
    t: PhantomData<T>,
}

impl<T> IntervalFunctionCreator<T>
where T: IntervalArithmeticImpl + Send + Sync + Clone + 'static
{
    pub fn try_create_func(
        display_name: &str,
        factor: i64,
        args: &[&DataTypePtr],
    ) -> Result<Box<dyn Function2>> {
        let left_arg = remove_nullable(args[0]);
        let right_arg = remove_nullable(args[1]);
        let left_type = left_arg.data_type_id();
        let right_type = right_arg.data_type_id();

        with_match_primitive_types_error!(right_type, |$R| {
            match left_type {
                TypeID::Date16 => IntervalFunction::<u16, $R, T::Date16Result, _>::try_create_func(
                    display_name,
                    left_arg,
                    T::eval_date16::<$R>,
                    factor,
                ),
                TypeID::Date32 => IntervalFunction::<i32, $R, T::Date32Result, _>::try_create_func(
                    display_name,
                    left_arg,
                    T::eval_date32::<$R>,
                    factor,
                ),
                TypeID::DateTime32 => IntervalFunction::<u32, $R, u32, _>::try_create_func(
                    display_name,
                    left_arg,
                    T::eval_datetime32::<$R>,
                    factor,
                ),
                TypeID::DateTime64 => IntervalFunction::<u64, $R, u64, _>::try_create_func(
                    display_name,
                    left_arg,
                    T::eval_datetime64::<$R>,
                    factor,
                ),
                _=> Err(ErrorCode::BadDataValueType(format!(
                    "DataValue Error: Unsupported arithmetic {}({:?}, {:?})",
                    display_name, left_type, right_type
                ))),
            }
        })
    }

    pub fn desc(factor: i64) -> ArithmeticDescription {
        let function_creator: ArithmeticCreator =
            Box::new(move |display_name, args| Self::try_create_func(display_name, factor, args));

        ArithmeticDescription::creator(function_creator)
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

pub trait IntervalArithmeticImpl {
    type Date16Result: IntegerType;
    type Date32Result: IntegerType;

    fn eval_date16<R: PrimitiveType + AsPrimitive<i64>>(
        l: u16,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<Self::Date16Result>;

    fn eval_date32<R: PrimitiveType + AsPrimitive<i64>>(
        l: i32,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<Self::Date32Result>;

    fn eval_datetime32<R: PrimitiveType + AsPrimitive<i64>>(
        l: u32,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<u32>;

    fn eval_datetime64<R: PrimitiveType + AsPrimitive<i64>>(
        l: u64,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<u64>;
}

macro_rules! define_date_add_year_months {
    ($l: ident, $r: ident, $factor: ident, $date_type:ident, $op: expr) => {{
        let epoch = NaiveDate::from_ymd(1970, 1, 1);
        let date = epoch
            .checked_add_signed(Duration::days($l as i64))
            .ok_or_else(|| ErrorCode::Overflow(format!("Overflow on date with days {}.", $l)))?;

        let new_date = $op(
            date.year(),
            date.month(),
            date.day(),
            $r.to_owned_scalar().as_() * $factor,
        )?;
        let duration = new_date.signed_duration_since(epoch);
        Ok(duration.num_days() as $date_type)
    }};
}

macro_rules! define_datetime32_add_year_months {
    ($l: ident, $r: ident, $factor: ident, $op: expr) => {{
        let naive = NaiveDateTime::from_timestamp_opt($l as i64, 0).ok_or_else(|| {
            ErrorCode::Overflow(format!("Overflow on datetime with seconds {}.", $l))
        })?;
        let new_date = $op(
            naive.year(),
            naive.month(),
            naive.day(),
            $r.to_owned_scalar().as_() * $factor,
        )?;
        let dt = NaiveDateTime::new(new_date, naive.time());
        Ok(dt.timestamp() as u32)
    }};
}

macro_rules! define_datetime64_add_year_months {
    ($l: ident, $r: ident, $factor: ident, $op: expr) => {{
        let l = $l as i64;
        let naive =
            NaiveDateTime::from_timestamp_opt(l / 1_000_000_000, (l % 1_000_000_000) as u32)
                .ok_or_else(|| {
                    ErrorCode::Overflow(format!("Overflow on datetime with seconds {}.", l))
                })?;
        let new_date = $op(
            naive.year(),
            naive.month(),
            naive.day(),
            $r.to_owned_scalar().as_() * $factor,
        )?;
        let dt = NaiveDateTime::new(new_date, naive.time());
        Ok(dt.timestamp_nanos() as u64)
    }};
}

#[derive(Clone)]
pub struct AddYearsImpl;

impl IntervalArithmeticImpl for AddYearsImpl {
    type Date16Result = u16;
    type Date32Result = i32;

    fn eval_date16<R: PrimitiveType + AsPrimitive<i64>>(
        l: u16,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<Self::Date16Result> {
        define_date_add_year_months!(l, r, factor, u16, add_years_base)
    }

    fn eval_date32<R: PrimitiveType + AsPrimitive<i64>>(
        l: i32,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<Self::Date32Result> {
        define_date_add_year_months!(l, r, factor, i32, add_years_base)
    }

    fn eval_datetime32<R: PrimitiveType + AsPrimitive<i64>>(
        l: u32,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<u32> {
        define_datetime32_add_year_months!(l, r, factor, add_years_base)
    }

    fn eval_datetime64<R: PrimitiveType + AsPrimitive<i64>>(
        l: u64,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<u64> {
        define_datetime64_add_year_months!(l, r, factor, add_years_base)
    }
}

fn add_years_base(year: i32, month: u32, day: u32, delta: i64) -> Result<NaiveDate> {
    let new_year = year + delta as i32;
    let mut new_day = day;
    if std::intrinsics::unlikely(month == 2 && day == 29) {
        new_day = last_day_of_year_month(new_year, month);
    }
    NaiveDate::from_ymd_opt(new_year, month, new_day).ok_or_else(|| {
        ErrorCode::Overflow(format!(
            "Overflow on date YMD {}-{}-{}.",
            new_year, month, new_day
        ))
    })
}

fn add_months_base(year: i32, month: u32, day: u32, delta: i64) -> Result<NaiveDate> {
    let total_months = month as i64 + delta - 1;
    let mut new_year = year + (total_months / 12) as i32;
    let mut new_month0 = total_months % 12;
    if new_month0 < 0 {
        new_year -= 1;
        new_month0 += 12;
    }

    // Handle month last day overflow, "2020-2-29" + "1 year" should be "2021-2-28", or "1990-1-31" + "3 month" should be "1990-4-30".
    let new_day = std::cmp::min::<u32>(
        day,
        last_day_of_year_month(new_year, (new_month0 + 1) as u32),
    );

    NaiveDate::from_ymd_opt(new_year, (new_month0 + 1) as u32, new_day).ok_or_else(|| {
        ErrorCode::Overflow(format!(
            "Overflow on date YMD {}-{}-{}.",
            new_year,
            new_month0 + 1,
            new_day
        ))
    })
}

// Get the last day of the year month, could be 28(non leap Feb), 29(leap year Feb), 30 or 31
fn last_day_of_year_month(year: i32, month: u32) -> u32 {
    let is_leap_year = NaiveDate::from_ymd_opt(year, 2, 29).is_some();
    if month == 2 && is_leap_year {
        return 29;
    }
    let last_day_lookup = [0u32, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    last_day_lookup[month as usize]
}
