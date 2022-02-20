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
use num::traits::Pow;
use num_traits::AsPrimitive;

use crate::scalars::assert_numeric;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::ScalarBinaryExpression;

#[derive(Clone)]
pub struct PowFunction {
    display_name: String,
}

impl PowFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(PowFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

pub fn scalar_pow<S, T>(value: S, base: T, _ctx: &mut EvalContext) -> f64
where
    S: AsPrimitive<f64>,
    T: AsPrimitive<f64>,
{
    value.as_().pow(base.as_())
}

impl Function for PowFunction {
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
        with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
            with_match_primitive_type_id!(columns[1].data_type().data_type_id(), |$T| {
                let binary = ScalarBinaryExpression::<$S, $T, f64, _>::new(scalar_pow);
                let col = binary.eval(columns[0].column(), columns[1].column(), &mut EvalContext::default())?;
                Ok(Arc::new(col))
            },{
                unreachable!()
            })
        },{
            unreachable!()
        })
    }
}

impl fmt::Display for PowFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
