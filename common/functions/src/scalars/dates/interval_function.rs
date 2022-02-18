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

use std::collections::BTreeMap;
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
use crate::define_datetime32_add_year_months;
use crate::define_datetime64_add_year_months;
use crate::impl_interval_year_month;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::ArithmeticCreator;
use crate::scalars::ArithmeticDescription;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::ScalarBinaryExpression;
use crate::scalars::ScalarBinaryFunction;

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
        let left_arg = remove_nullable(args[0]);
        let right_arg = remove_nullable(args[1]);
        let left_type = left_arg.data_type_id();
        let right_type = right_arg.data_type_id();

        with_match_primitive_types_error!(right_type, |$R| {
            match left_type {
                TypeID::Date16 => IntervalFunction::<u16, $R, T::Date16Result, _>::try_create_func(
                    display_name,
                    T::Date16Result::to_date_type(),
                    T::eval_date16::<$R>,
                    factor,
                    None,
                ),
                TypeID::Date32 => IntervalFunction::<i32, $R, T::Date32Result, _>::try_create_func(
                    display_name,
                    T::Date32Result::to_date_type(),
                    T::eval_date32::<$R>,
                    factor,
                    None,
                ),
                TypeID::DateTime32 => IntervalFunction::<u32, $R, u32, _>::try_create_func(
                    display_name,
                    left_arg,
                    T::eval_datetime32::<$R>,
                    factor,
                    None,
                ),
                TypeID::DateTime64 => {
                    let mut mp = BTreeMap::new();
                    let datetime = left_arg.as_any().downcast_ref::<DateTime64Type>().unwrap();
                    let precision = datetime.precision().to_string();
                    mp.insert("precision".to_string(), precision);
                    IntervalFunction::<i64, $R, i64, _>::try_create_func(
                        display_name,
                        left_arg,
                        T::eval_datetime64::<$R>,
                        factor,
                        Some(mp),
                    )
                },
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

#[derive(Clone)]
pub struct IntervalFunction<L: DateType, R: PrimitiveType, O: DateType, F> {
    display_name: String,
    result_type: DataTypePtr,
    binary: ScalarBinaryExpression<L, R, O, F>,
    factor: i64,
    metadata: Option<BTreeMap<String, String>>,
}

impl<L, R, O, F> IntervalFunction<L, R, O, F>
where
    L: DateType + Send + Sync + Clone,
    R: PrimitiveType + Send + Sync + Clone,
    O: DateType + Send + Sync + Clone,
    F: ScalarBinaryFunction<L, R, O> + Send + Sync + Clone + 'static,
{
    pub fn try_create_func(
        display_name: &str,
        result_type: DataTypePtr,
        func: F,
        factor: i64,
        metadata: Option<BTreeMap<String, String>>,
    ) -> Result<Box<dyn Function>> {
        let binary = ScalarBinaryExpression::<L, R, O, _>::new(func);
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
            result_type,
            binary,
            factor,
            metadata,
        }))
    }
}

impl<L, R, O, F> Function for IntervalFunction<L, R, O, F>
where
    L: DateType + Send + Sync + Clone,
    R: PrimitiveType + Send + Sync + Clone,
    O: DateType + Send + Sync + Clone,
    F: ScalarBinaryFunction<L, R, O> + Send + Sync + Clone + 'static,
{
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(self.result_type.clone())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        // Todo(zhyass): define the ctx out of the eval.
        let mut ctx = EvalContext::new(self.factor, None, self.metadata.clone());
        let col = self
            .binary
            .eval(columns[0].column(), columns[1].column(), &mut ctx)?;
        Ok(Arc::new(col))
    }
}

impl<L, R, O, F> fmt::Display for IntervalFunction<L, R, O, F>
where
    L: DateType + Send + Sync + Clone,
    R: PrimitiveType + Send + Sync + Clone,
    O: DateType + Send + Sync + Clone,
    F: ScalarBinaryFunction<L, R, O> + Send + Sync + Clone + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

pub trait IntervalArithmeticImpl {
    type Date16Result: DateType + ToDateType;
    type Date32Result: DateType + ToDateType;

    fn eval_date16<R: PrimitiveType + AsPrimitive<i64>>(
        l: u16,
        r: R::RefType<'_>,
        ctx: &mut EvalContext,
    ) -> Self::Date16Result;

    fn eval_date32<R: PrimitiveType + AsPrimitive<i64>>(
        l: i32,
        r: R::RefType<'_>,
        ctx: &mut EvalContext,
    ) -> Self::Date32Result;

    fn eval_datetime32<R: PrimitiveType + AsPrimitive<i64>>(
        l: u32,
        r: R::RefType<'_>,
        ctx: &mut EvalContext,
    ) -> u32;

    fn eval_datetime64<R: PrimitiveType + AsPrimitive<i64>>(
        l: i64,
        r: R::RefType<'_>,
        ctx: &mut EvalContext,
    ) -> i64;
}

impl_interval_year_month!(AddYearsImpl, add_years_base);
impl_interval_year_month!(AddMonthsImpl, add_months_base);

#[derive(Clone)]
pub struct AddDaysImpl;

impl IntervalArithmeticImpl for AddDaysImpl {
    type Date16Result = u16;
    type Date32Result = i32;

    fn eval_date16<R: PrimitiveType + AsPrimitive<i64>>(
        l: u16,
        r: R::RefType<'_>,
        ctx: &mut EvalContext,
    ) -> Self::Date16Result {
        (l as i64 + r.to_owned_scalar().as_() * ctx.factor) as Self::Date16Result
    }

    fn eval_date32<R: PrimitiveType + AsPrimitive<i64>>(
        l: i32,
        r: R::RefType<'_>,
        ctx: &mut EvalContext,
    ) -> Self::Date32Result {
        (l as i64 + r.to_owned_scalar().as_() * ctx.factor) as Self::Date32Result
    }

    fn eval_datetime32<R: PrimitiveType + AsPrimitive<i64>>(
        l: u32,
        r: R::RefType<'_>,
        ctx: &mut EvalContext,
    ) -> u32 {
        let factor = ctx.factor * 24 * 3600;
        (l as i64 + r.to_owned_scalar().as_() * factor) as u32
    }

    fn eval_datetime64<R: PrimitiveType + AsPrimitive<i64>>(
        l: i64,
        r: R::RefType<'_>,
        ctx: &mut EvalContext,
    ) -> i64 {
        let precision = ctx
            .get_meta_value("precision".to_string())
            .map_or(0, |v| v.parse::<u32>().unwrap());
        let base = 10_i64.pow(precision);
        let factor = ctx.factor * 24 * 3600 * base;
        l as i64 + r.to_owned_scalar().as_() * factor
    }
}

#[derive(Clone)]
pub struct AddTimesImpl;

impl IntervalArithmeticImpl for AddTimesImpl {
    type Date16Result = u32;
    type Date32Result = i64;

    fn eval_date16<R: PrimitiveType + AsPrimitive<i64>>(
        l: u16,
        r: R::RefType<'_>,
        ctx: &mut EvalContext,
    ) -> Self::Date16Result {
        (l as i64 * 3600 * 24 + r.to_owned_scalar().as_() * ctx.factor) as Self::Date16Result
    }

    fn eval_date32<R: PrimitiveType + AsPrimitive<i64>>(
        l: i32,
        r: R::RefType<'_>,
        ctx: &mut EvalContext,
    ) -> Self::Date32Result {
        l as i64 * 3600 * 24 + r.to_owned_scalar().as_() * ctx.factor
    }

    fn eval_datetime32<R: PrimitiveType + AsPrimitive<i64>>(
        l: u32,
        r: R::RefType<'_>,
        ctx: &mut EvalContext,
    ) -> u32 {
        (l as i64 + r.to_owned_scalar().as_() * ctx.factor) as u32
    }

    fn eval_datetime64<R: PrimitiveType + AsPrimitive<i64>>(
        l: i64,
        r: R::RefType<'_>,
        ctx: &mut EvalContext,
    ) -> i64 {
        let precision = ctx
            .get_meta_value("precision".to_string())
            .map_or(0, |v| v.parse::<u32>().unwrap());
        let base = 10_i64.pow(precision);
        let factor = ctx.factor * base;
        l as i64 + r.to_owned_scalar().as_() * factor
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
