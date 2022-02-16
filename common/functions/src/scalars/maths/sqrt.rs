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

use common_datavalues2::prelude::*;
use common_datavalues2::with_match_primitive_type_id;
use common_exception::Result;
use num::cast::AsPrimitive;

use crate::scalars::function2_factory::Function2Description;
use crate::scalars::function_common::assert_numeric;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;
use crate::scalars::ScalarUnaryExpression;

#[derive(Clone)]
pub struct SqrtFunction {
    display_name: String,
}

impl SqrtFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(SqrtFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> Function2Description {
        Function2Description::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

fn sqrt<S>(value: S) -> f64
where S: AsPrimitive<f64> {
    value.as_().sqrt()
}

impl Function2 for SqrtFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        assert_numeric(args[0])?;
        Ok(Float64Type::arc())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
             let unary = ScalarUnaryExpression::<$S, f64, _>::new(sqrt::<$S>);
             let col = unary.eval(columns[0].column())?;
             Ok(col.arc())
        },{
            unreachable!()
        })
    }
}

impl fmt::Display for SqrtFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SQRT")
    }
}
