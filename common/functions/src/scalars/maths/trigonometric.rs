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

#[derive(Clone)]
pub struct TrigonometricFunction {
    t: Trigonometric,
}

#[derive(Clone, Debug)]
pub enum Trigonometric {
    SIN,
    COS,
    COT,
    TAN,
    ACOS,
    ASIN,
    ATAN,
    ATAN2,
}

impl fmt::Display for Trigonometric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let display = match &self {
            Trigonometric::SIN => "sin",
            Trigonometric::COS => "cos",
            Trigonometric::TAN => "tan",
            Trigonometric::COT => "cot",
            Trigonometric::ACOS => "acos",
            Trigonometric::ASIN => "asin",
            Trigonometric::ATAN => "atan",
            Trigonometric::ATAN2 => "atan2",
        };
        write!(f, "{}", display)
    }
}

impl TrigonometricFunction {
    pub fn try_create_func(t: Trigonometric) -> Result<Box<dyn Function>> {
        Ok(Box::new(TrigonometricFunction { t }))
    }
}

impl Function for TrigonometricFunction {
    fn name(&self) -> &str {
        "TrigonometricFunction"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        for arg in args {
            assert_numeric(*arg)?;
        }
        Ok(f64::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let mut ctx = EvalContext::default();
        match columns.len() {
            1 => {
                with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
                   match self.t {
                        Trigonometric::COS => {
                           let unary =  ScalarUnaryExpression::<$S, f64, _>::new(|v: $S, _ctx: &mut EvalContext|  AsPrimitive::<f64>::as_(v).cos());
                           let col = unary.eval(columns[0].column(), &mut ctx)?;
                           Ok(Arc::new(col))
                        },
                        Trigonometric::SIN => {
                           let unary =  ScalarUnaryExpression::<$S, f64, _>::new(|v: $S, _ctx: &mut EvalContext|  AsPrimitive::<f64>::as_(v).sin());
                           let col = unary.eval(columns[0].column(), &mut ctx)?;
                           Ok(Arc::new(col))
                        },
                        Trigonometric::COT => {
                           let unary =  ScalarUnaryExpression::<$S, f64, _>::new(|v: $S, _ctx: &mut EvalContext| 1.0 /  AsPrimitive::<f64>::as_(v).tan());
                           let col = unary.eval(columns[0].column(), &mut ctx)?;
                           Ok(Arc::new(col))
                        },
                        Trigonometric::TAN => {
                           let unary =  ScalarUnaryExpression::<$S, f64, _>::new(|v: $S, _ctx: &mut EvalContext| AsPrimitive::<f64>::as_(v).tan());
                           let col = unary.eval(columns[0].column(), &mut ctx)?;
                           Ok(Arc::new(col))
                        },
                        // the range [0, pi] or NaN if the number is outside the range
                        Trigonometric::ACOS => {
                           let unary =  ScalarUnaryExpression::<$S, f64, _>::new(|v: $S, _ctx: &mut EvalContext| AsPrimitive::<f64>::as_(v).acos());
                           let col = unary.eval(columns[0].column(), &mut ctx)?;
                           Ok(Arc::new(col))
                        },
                        Trigonometric::ASIN => {
                           let unary =  ScalarUnaryExpression::<$S, f64, _>::new(|v: $S, _ctx: &mut EvalContext| AsPrimitive::<f64>::as_(v).asin());
                           let col = unary.eval(columns[0].column(), &mut ctx)?;
                           Ok(Arc::new(col))
                        },
                        Trigonometric::ATAN => {
                           let unary =  ScalarUnaryExpression::<$S, f64, _>::new(|v: $S, _ctx: &mut EvalContext| AsPrimitive::<f64>::as_(v).atan());
                           let col = unary.eval(columns[0].column(), &mut ctx)?;
                           Ok(Arc::new(col))
                        },
                        _ => unreachable!(),
                    }
                },
                {
                    unreachable!()
                })
            }

            _ => {
                with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
                    with_match_primitive_type_id!(columns[1].data_type().data_type_id(), |$T| {
                        let binary = ScalarBinaryExpression::<$S, $T, f64, _>::new(scalar_atan2);
                        let col = binary.eval(columns[0].column(), columns[1].column(), &mut EvalContext::default())?;
                        Ok(Arc::new(col))
                    }, {
                        unreachable!()
                    })
                }, {
                    unreachable!()
                })
            }
        }
    }
}

fn scalar_atan2<S: AsPrimitive<f64>, T: AsPrimitive<f64>>(
    s: S,
    t: T,
    _ctx: &mut EvalContext,
) -> f64 {
    s.as_().atan2(t.as_())
}

impl fmt::Display for TrigonometricFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.t)
    }
}

pub struct TrigonometricSinFunction;

impl TrigonometricSinFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::SIN)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

pub struct TrigonometricCosFunction;

impl TrigonometricCosFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::COS)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

pub struct TrigonometricTanFunction;

impl TrigonometricTanFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::TAN)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

pub struct TrigonometricCotFunction;

impl TrigonometricCotFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::COT)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

pub struct TrigonometricAsinFunction;

impl TrigonometricAsinFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::ASIN)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

pub struct TrigonometricAcosFunction;

impl TrigonometricAcosFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::ACOS)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

pub struct TrigonometricAtanFunction;

impl TrigonometricAtanFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::ATAN)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(1, 2),
        )
    }
}

pub struct TrigonometricAtan2Function;

impl TrigonometricAtan2Function {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        TrigonometricFunction::try_create_func(Trigonometric::ATAN2)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}
