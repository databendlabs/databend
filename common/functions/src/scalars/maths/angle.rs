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

use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_type_id;
use common_exception::Result;
use num::cast::AsPrimitive;

use crate::scalars::function_common::assert_numeric;
use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::ScalarUnaryExpression;

#[derive(Clone)]
pub struct AngleFunction<T> {
    _display_name: String,
    t: PhantomData<T>,
}

pub trait AngleConvertFunction {
    fn convert(v: impl AsPrimitive<f64>, _ctx: &mut EvalContext) -> f64;
}

impl<T> AngleFunction<T>
where T: AngleConvertFunction + Clone + Sync + Send + 'static
{
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(AngleFunction::<T> {
            _display_name: display_name.to_string(),
            t: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl<T> Function for AngleFunction<T>
where T: AngleConvertFunction + Clone + Sync + Send + 'static
{
    fn name(&self) -> &str {
        "AngleFunction"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        assert_numeric(args[0])?;
        Ok(Float64Type::arc())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let mut ctx = EvalContext::default();
        with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
             let unary = ScalarUnaryExpression::<$S, f64, _>::new(T::convert);
             let col = unary.eval(columns[0].column(), &mut ctx)?;
             Ok(col.arc())
        },{
            unreachable!()
        })
    }
}

impl<T> fmt::Display for AngleFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self._display_name)
    }
}

#[derive(Clone)]
pub struct ToDegrees;

impl AngleConvertFunction for ToDegrees {
    fn convert(v: impl AsPrimitive<f64>, _ctx: &mut EvalContext) -> f64 {
        v.as_().to_degrees()
    }
}

#[derive(Clone)]
pub struct ToRadians;

impl AngleConvertFunction for ToRadians {
    fn convert(v: impl AsPrimitive<f64>, _ctx: &mut EvalContext) -> f64 {
        v.as_().to_radians()
    }
}

pub type DegressFunction = AngleFunction<ToDegrees>;
pub type RadiansFunction = AngleFunction<ToRadians>;
