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
use std::sync::Arc;

use common_datavalues::chrono::Datelike;
use common_datavalues::chrono::Duration;
use common_datavalues::chrono::NaiveDate;
use common_datavalues::chrono::NaiveDateTime;
use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_types_error;
use common_exception::ErrorCode;
use common_exception::Result;
use num_traits::AsPrimitive;

use crate::define_date_add_year_months;
use crate::define_timestamp_add_year_months;
use crate::impl_interval_year_month;
use crate::scalars::scalar_binary_op;
use crate::scalars::EvalContext;
use crate::scalars::FactoryCreator;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

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
    ) -> Result<Box<dyn Function>> {
        let left_type = args[0].data_type_id();
        let right_type = args[1].data_type_id();

        with_match_primitive_types_error!(right_type, |$R| {
            match left_type {
                TypeID::Date => IntervalFunction::<i32, $R, T::DateResultType, _>::try_create_func(
                    display_name,
                    T::DateResultType::to_date_type(),
                    T::eval_date,
                    factor,
                    0,
                ),
                TypeID::TimeStamp => {
                    let ts = args[0].as_any().downcast_ref::<TimeStampType>().unwrap();
                    let precision = ts.precision();
                    IntervalFunction::<i64, $R, i64, _>::try_create_func(
                        display_name,
                        args[0].clone(),
                        T::eval_timestamp,
                        factor,
                        precision,
                    )
                },
                _=> Err(ErrorCode::BadDataValueType(format!(
                    "DataValue Error: Unsupported arithmetic {}({:?}, {:?})",
                    display_name, left_type, right_type
                ))),
            }
        })
    }

    pub fn desc(factor: i64) -> FunctionDescription {
        let function_creator: FactoryCreator =
            Box::new(move |display_name, args| Self::try_create_func(display_name, factor, args));

        FunctionDescription::creator(function_creator)
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

#[derive(Clone)]
pub struct IntervalFunction<L: LogicalDateType, R: PrimitiveType, O: LogicalDateType, F> {
    display_name: String,
    result_type: DataTypePtr,
    func: F,
    factor: i64,
    precision: usize,
    _phantom: PhantomData<(L, R, O)>,
}

impl<L, R, O, F> IntervalFunction<L, R, O, F>
where
    L: LogicalDateType + Send + Sync + Clone,
    R: PrimitiveType + Send + Sync + Clone,
    O: LogicalDateType + Send + Sync + Clone,
    F: Fn(L::RefType<'_>, R::RefType<'_>, &mut EvalContext) -> O + Send + Sync + Clone + 'static,
{
    pub fn try_create_func(
        display_name: &str,
        result_type: DataTypePtr,
        func: F,
        factor: i64,
        precision: usize,
    ) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
            result_type,
            func,
            factor,
            precision,
            _phantom: PhantomData,
        }))
    }
}

impl<L, R, O, F> Function for IntervalFunction<L, R, O, F>
where
    L: LogicalDateType + Send + Sync + Clone,
    R: PrimitiveType + Send + Sync + Clone,
    O: LogicalDateType + Send + Sync + Clone,
    F: Fn(L::RefType<'_>, R::RefType<'_>, &mut EvalContext) -> O + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self) -> DataTypePtr {
        self.result_type.clone()
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        _input_rows: usize,
    ) -> Result<ColumnRef> {
        // Todo(zhyass): define the ctx out of the eval.
        let mut ctx = EvalContext::new(self.factor, self.precision, None);
        let col = scalar_binary_op(
            columns[0].column(),
            columns[1].column(),
            self.func.clone(),
            &mut ctx,
        )?;
        Ok(Arc::new(col))
    }
}

impl<L, R, O, F> fmt::Display for IntervalFunction<L, R, O, F>
where
    L: LogicalDateType + Send + Sync + Clone,
    R: PrimitiveType + Send + Sync + Clone,
    O: LogicalDateType + Send + Sync + Clone,
    F: Fn(L::RefType<'_>, R::RefType<'_>, &mut EvalContext) -> O + Send + Sync + Clone + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

pub trait IntervalArithmeticImpl {
    type DateResultType: LogicalDateType + ToDateType;

    /// when date type add year/month/day, output is date type
    /// when date type add hour/minute/second/... output is timestamp type
    fn eval_date(l: i32, r: impl AsPrimitive<i64>, ctx: &mut EvalContext) -> Self::DateResultType;

    fn eval_timestamp(l: i64, r: impl AsPrimitive<i64>, ctx: &mut EvalContext) -> i64;
}

impl_interval_year_month!(AddYearsImpl, add_years_base);
impl_interval_year_month!(AddMonthsImpl, add_months_base);

#[derive(Clone)]
pub struct AddDaysImpl;

impl IntervalArithmeticImpl for AddDaysImpl {
    type DateResultType = i32;

    fn eval_date(l: i32, r: impl AsPrimitive<i64>, ctx: &mut EvalContext) -> Self::DateResultType {
        (l as i64 + r.as_() * ctx.factor) as Self::DateResultType
    }

    fn eval_timestamp(l: i64, r: impl AsPrimitive<i64>, ctx: &mut EvalContext) -> i64 {
        let base = 10_i64.pow(ctx.precision as u32);
        let factor = ctx.factor * 24 * 3600 * base;
        l as i64 + r.as_() * factor
    }
}

#[derive(Clone)]
pub struct AddTimesImpl;

impl IntervalArithmeticImpl for AddTimesImpl {
    type DateResultType = i64;

    fn eval_date(l: i32, r: impl AsPrimitive<i64>, ctx: &mut EvalContext) -> Self::DateResultType {
        (l as i64 * 3600 * 24 + r.as_() * ctx.factor) as Self::DateResultType
    }

    fn eval_timestamp(l: i64, r: impl AsPrimitive<i64>, ctx: &mut EvalContext) -> i64 {
        let base = 10_i64.pow(ctx.precision as u32);
        let factor = ctx.factor * base;
        l as i64 + r.as_() * factor
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
    if std::intrinsics::unlikely(month == 2 && is_leap_year) {
        return 29;
    }
    let last_day_lookup = [0u32, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    last_day_lookup[month as usize]
}

pub type AddYearsFunction = IntervalFunctionCreator<AddYearsImpl>;
pub type AddMonthsFunction = IntervalFunctionCreator<AddMonthsImpl>;
pub type AddDaysFunction = IntervalFunctionCreator<AddDaysImpl>;
pub type AddTimesFunction = IntervalFunctionCreator<AddTimesImpl>;
