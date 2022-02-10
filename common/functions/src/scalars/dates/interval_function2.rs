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
use common_datavalues2::with_match_primitive_type_id;
use common_datavalues2::with_match_primitive_types_error;
use common_exception::ErrorCode;
use common_exception::Result;
use num_traits::AsPrimitive;

use crate::scalars::function_factory::FunctionFeatures;
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

#[derive(Clone, Debug)]
pub struct IntervalFunction2<T> {
    t: PhantomData<T>,
    display_name: String,
    factor: i64,
}

impl<T> Function2 for IntervalFunction2<T>
where T: IntervalArithmeticImpl + Send + Sync + Clone + 'static
{
    fn name(&self) -> &str {
        "IntervalFunction"
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        todo!()
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        with_match_primitive_types_error!(columns[1].data_type().data_type_id(), |$R| {
            let right = ColumnViewerIter::<$R>::try_create(columns[1].column())?;
            match columns[0].data_type().data_type_id() {
                TypeID::Date16 => {
                    let left = ColumnViewerIter::<u16>::try_create(columns[0].column())?;
                    let mut col_builder = MutablePrimitiveColumn::<T::Date16Result>::with_capacity(left.size);
                    for (l, r) in left.zip(right) {
                        let o = T::eval_date16::<$R>(l, r, self.factor)?;
                        col_builder.append_value(o);
                    }
                    Ok(col_builder.to_column())
                },
                TypeID::Date32 => {
                    let left = ColumnViewerIter::<i32>::try_create(columns[0].column())?;
                    let mut col_builder = MutablePrimitiveColumn::<T::Date32Result>::with_capacity(left.size);
                    for (l, r) in left.zip(right) {
                        let o = T::eval_date32::<$R>(l, r, self.factor)?;
                        col_builder.append_value(o);
                    }
                    Ok(col_builder.to_column())
                },
                TypeID::DateTime32 => {
                    let left = ColumnViewerIter::<u32>::try_create(columns[0].column())?;
                    let mut col_builder = MutablePrimitiveColumn::<u32>::with_capacity(left.size);
                    for (l, r) in left.zip(right) {
                        let o = T::eval_datetime32::<$R>(l, r, self.factor)?;
                        col_builder.append_value(o);
                    }
                    Ok(col_builder.to_column())
                },
                TypeID::DateTime64 => {
                    let left = ColumnViewerIter::<u64>::try_create(columns[0].column())?;
                    let mut col_builder = MutablePrimitiveColumn::<u64>::with_capacity(left.size);
                    for (l, r) in left.zip(right) {
                        let o = T::eval_datetime64::<$R>(l, r, self.factor)?;
                        col_builder.append_value(o);
                    }
                    Ok(col_builder.to_column())
                },
                _=> unreachable!(),
            }
        })
    }
}

impl<T> fmt::Display for IntervalFunction2<T>
where T: IntervalArithmeticImpl + Send + Sync + Clone + 'static
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

pub trait IntervalArithmeticImpl {
    type Date16Result: IntegerType;
    type Date32Result: IntegerType;
    fn eval_date16<R: PrimitiveType>(
        l: u16,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<Self::Date16Result>;
    fn eval_date32<R: PrimitiveType>(
        l: i32,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<Self::Date32Result>;
    fn eval_datetime32<R: PrimitiveType>(
        l: u32,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<u32>;
    fn eval_datetime64<R: PrimitiveType>(
        l: u64,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<u64>;
}

pub struct AddYearsImpl;

impl IntervalArithmeticImpl for AddYearsImpl {
    type Date16Result = u16;
    type Date32Result = i32;

    fn eval_date16<R: PrimitiveType>(
        l: u16,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<Self::Date16Result> {
        todo!()
    }

    fn eval_date32<R: PrimitiveType>(
        l: i32,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<Self::Date32Result> {
        todo!()
    }

    fn eval_datetime32<R: PrimitiveType>(
        l: u32,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<u32> {
        todo!()
    }

    fn eval_datetime64<R: PrimitiveType>(
        l: u64,
        r: R::RefType<'_>,
        factor: i64,
    ) -> Result<u64> {
        todo!()
    }
}

pub struct AddYearsFunction;

impl AddYearsFunction {
    pub fn try_create_func(
        display_name: &str,
        args: &[&DataTypePtr],
    ) -> Result<Box<dyn Function2>> {
        let left_arg = remove_nullable(args[0]);
        let right_arg = remove_nullable(args[1]);
        let left_type = left_arg.data_type_id();
        let right_type = right_arg.data_type_id();

        let error_fn = || -> Result<Box<dyn Function2>> {
            Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error: Unsupported arithmetic {}({:?}, {:?})",
                display_name, left_type, right_type
            )))
        };

        with_match_primitive_type_id!(right_type, |$R| {
            match left_type {
                TypeID::Date16 => IntervalFunction::<u16, $R, u16, _>::try_create_func(
                    display_name,
                    left_arg,
                    date16_add_years::<$R>,
                    1,
                ),
                TypeID::Date32 => IntervalFunction::<i32, $R, i32, _>::try_create_func(
                    display_name,
                    left_arg,
                    date32_add_years::<$R>,
                    1,
                ),
                TypeID::DateTime32 => IntervalFunction::<u32, $R, u32, _>::try_create_func(
                    display_name,
                    left_arg,
                    datetime32_add_years::<$R>,
                    1,
                ),
                TypeID::DateTime64 => {
                    let precision = left_arg.as_any().downcast_ref::<DateTime64Type>().unwrap().precision() as u32;
                    IntervalFunction::<u64, $R, u64, _>::try_create_func(
                    display_name,
                    left_arg,
                    datetime64_add_years::<$R>,
                    10_i64.pow(9 - precision),
                )},
                _=> error_fn(),
            }
        }, {
            error_fn()
        })
    }

    pub fn desc() -> ArithmeticDescription {
        ArithmeticDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().num_arguments(2))
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

#[macro_export]
macro_rules! define_date_add_year_months {
    ($fn_name:ident, $date_type:ident, $op: expr) => {
        fn $fn_name<R>(l: $date_type, r: R::RefType<'_>, _factor: i64) -> Result<$date_type>
        where R: PrimitiveType + AsPrimitive<i64> {
            let epoch = NaiveDate::from_ymd(1970, 1, 1);
            let date = epoch
                .checked_add_signed(Duration::days(l as i64))
                .ok_or_else(|| ErrorCode::Overflow(format!("Overflow on date with days {}.", l)))?;

            let new_date = $op(
                date.year(),
                date.month(),
                date.day(),
                r.to_owned_scalar().as_(),
            )?;
            let duration = new_date.signed_duration_since(epoch);
            Ok(duration.num_days() as $date_type)
        }
    };
}

crate::define_date_add_year_months!(date16_add_years, u16, add_years_base);
crate::define_date_add_year_months!(date32_add_years, i32, add_years_base);
//crate::define_date_add_year_months!(date16_add_months, u16, add_months_base);
//crate::define_date_add_year_months!(date32_add_months, i32, add_months_base);

#[macro_export]
macro_rules! define_datetime32_add_year_months {
    ($fn_name:ident, $op: expr) => {
        fn $fn_name<R>(l: u32, r: R::RefType<'_>, _factor: i64) -> Result<u32>
        where R: PrimitiveType + AsPrimitive<i64> {
            let naive = NaiveDateTime::from_timestamp_opt(l as i64, 0).ok_or_else(|| {
                ErrorCode::Overflow(format!("Overflow on datetime with seconds {}.", l))
            })?;
            let new_date = $op(
                naive.year(),
                naive.month(),
                naive.day(),
                r.to_owned_scalar().as_(),
            )?;
            let dt = NaiveDateTime::new(new_date, naive.time());
            Ok(dt.timestamp() as u32)
        }
    };
}

crate::define_datetime32_add_year_months!(datetime32_add_years, add_years_base);
//crate::define_datetime32_add_year_months!(datetime32_add_months, add_months_base);

#[macro_export]
macro_rules! define_datetime64_add_year_months {
    ($fn_name:ident, $op: expr) => {
        fn $fn_name<R>(l: u64, r: R::RefType<'_>, factor: i64) -> Result<u64>
        where R: PrimitiveType + AsPrimitive<i64> {
            let nano = l as i64 * factor;
            let naive = NaiveDateTime::from_timestamp_opt(
                nano / 1_000_000_000,
                (nano % 1_000_000_000) as u32,
            )
            .ok_or_else(|| {
                ErrorCode::Overflow(format!("Overflow on datetime with seconds {}.", l))
            })?;
            let new_date = $op(
                naive.year(),
                naive.month(),
                naive.day(),
                r.to_owned_scalar().as_(),
            )?;
            let dt = NaiveDateTime::new(new_date, naive.time());
            Ok((dt.timestamp_nanos() / factor) as u64)
        }
    };
}

crate::define_datetime64_add_year_months!(datetime64_add_years, add_years_base);
//crate::define_datetime64_add_year_months!(datetime64_add_months, add_months_base);

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
