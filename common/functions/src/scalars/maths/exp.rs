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
pub struct ExpFunction {
    _display_name: String,
}

impl ExpFunction {
    pub fn try_create(_display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ExpFunction {
            _display_name: _display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

fn exp<S>(value: S, _ctx: &mut EvalContext) -> f64
where S: AsPrimitive<f64> {
    value.as_().exp()
}

impl Function for ExpFunction {
    fn name(&self) -> &str {
        &*self._display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        assert_numeric(args[0])?;
        Ok(Float64Type::arc())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let mut ctx = EvalContext::default();
        with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
             let unary = ScalarUnaryExpression::<$S, f64, _>::new(exp::<$S>);
             let col = unary.eval(columns[0].column(), &mut ctx)?;
             Ok(col.arc())
        },{
            unreachable!()
        })
    }
}

impl fmt::Display for ExpFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EXP")
    }
}
