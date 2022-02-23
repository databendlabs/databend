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

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_type_id;
use common_exception::Result;
use num_traits::AsPrimitive;

use crate::scalars::assert_numeric;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::ScalarBinaryExpression;
use crate::scalars::ScalarUnaryExpression;

fn round<S>(value: S, _ctx: &mut EvalContext) -> f64
where S: AsPrimitive<f64> {
    value.as_().round()
}

fn trunc<S>(value: S, _ctx: &mut EvalContext) -> f64
where S: AsPrimitive<f64> {
    value.as_().trunc()
}

fn round_to<S, T>(value: S, to: T, _ctx: &mut EvalContext) -> f64
where
    S: AsPrimitive<f64>,
    T: AsPrimitive<i64>,
{
    let value = value.as_();
    let to = to.as_();
    match to.cmp(&0) {
        Ordering::Greater => {
            let z = 10_f64.powi(if to > 30 { 30 } else { to as i32 });
            (value * z).round() / z
        }
        Ordering::Less => {
            let z = 10_f64.powi(if to < -30 { 30 } else { -to as i32 });
            (value / z).round() * z
        }
        Ordering::Equal => value.round(),
    }
}

fn trunc_to<S, T>(value: S, to: T, _ctx: &mut EvalContext) -> f64
where
    S: AsPrimitive<f64>,
    T: AsPrimitive<i64>,
{
    let value = value.as_();
    let to = to.as_();
    match to.cmp(&0) {
        Ordering::Greater => {
            let z = 10_f64.powi(if to > 30 { 30 } else { to as i32 });
            (value * z).trunc() / z
        }
        Ordering::Less => {
            let z = 10_f64.powi(if to < -30 { 30 } else { -to as i32 });
            (value / z).trunc() * z
        }
        Ordering::Equal => value.trunc(),
    }
}

impl<const IS_TRUNC: bool> Function for RoundingFunction<IS_TRUNC> {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        for arg in args {
            assert_numeric(*arg)?;
        }
        Ok(f64::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        match IS_TRUNC {
            false => eval_round(columns),
            true => eval_trunc(columns),
        }
    }
}

fn eval_round(columns: &ColumnsWithField) -> Result<ColumnRef> {
    let mut ctx = EvalContext::default();
    match columns.len() {
        1 => {
            with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
                let unary = ScalarUnaryExpression::<$S, f64, _>::new(round);
                let col = unary.eval(columns[0].column(), &mut ctx)?;
                Ok(Arc::new(col))
            },{
                unreachable!()
            })
        }

        _ => {
            with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
                with_match_primitive_type_id!(columns[1].data_type().data_type_id(), |$T| {
                    let binary = ScalarBinaryExpression::<$S, $T, f64, _>::new(round_to);
                    let col = binary.eval(
                        columns[0].column(),
                        columns[1].column(),
                        &mut ctx
                    )?;
                    Ok(Arc::new(col))
                },{
                    unreachable!()
                })
            },{
                unreachable!()
            })
        }
    }
}

fn eval_trunc(columns: &ColumnsWithField) -> Result<ColumnRef> {
    let mut ctx = EvalContext::default();
    match columns.len() {
        1 => {
            with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
                let unary = ScalarUnaryExpression::<$S, f64, _>::new(trunc);
                let col = unary.eval(columns[0].column(), &mut ctx)?;
                Ok(Arc::new(col))
            },{
                unreachable!()
            })
        }

        _ => {
            with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
                with_match_primitive_type_id!(columns[1].data_type().data_type_id(), |$T| {
                    let binary = ScalarBinaryExpression::<$S, $T, f64, _>::new(trunc_to);
                    let col = binary.eval(
                        columns[0].column(),
                        columns[1].column(),
                        &mut ctx,
                    )?;
                    Ok(Arc::new(col))
                },{
                    unreachable!()
                })
            },{
                unreachable!()
            })
        }
    }
}

#[derive(Clone)]
pub struct RoundingFunction<const IS_TRUNC: bool> {
    display_name: String,
}

impl<const IS_TRUNC: bool> RoundingFunction<IS_TRUNC> {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(1, 2),
        )
    }
}

impl<const IS_TRUNC: bool> fmt::Display for RoundingFunction<IS_TRUNC> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

pub type RoundNumberFunction = RoundingFunction<false>;
pub type TruncNumberFunction = RoundingFunction<true>;
