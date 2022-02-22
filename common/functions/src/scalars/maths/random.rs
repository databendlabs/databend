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
use rand::prelude::*;

use crate::scalars::assert_numeric;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::ScalarUnaryExpression;

#[derive(Clone)]
pub struct RandomFunction {
    display_name: String,
}

impl RandomFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(RandomFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().variadic_arguments(0, 1))
    }
}

impl Function for RandomFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        for arg in args {
            assert_numeric(*arg)?;
        }
        Ok(f64::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        match columns.len() {
            0 => {
                let mut rng = rand::thread_rng();
                Ok(Float64Column::from_owned_iterator(
                    (0..input_rows).into_iter().map(|_| rng.gen::<f64>()),
                )
                .arc())
            }
            _ => {
                let mut ctx = EvalContext::default();
                with_match_primitive_type_id!(columns[1].data_type().data_type_id(), |$T| {
                      let unary = ScalarUnaryExpression::<$T, f64, _>::new(rand_seed);
                    let col = unary.eval(columns[0].column(), &mut ctx)?;
                    Ok(Arc::new(col))
                },{
                    unreachable!()
                })
            }
        }
    }
}

fn rand_seed<T: AsPrimitive<u64>>(seed: T, _ctx: &mut EvalContext) -> f64 {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed.as_());
    rng.gen::<f64>()
}

impl fmt::Display for RandomFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
