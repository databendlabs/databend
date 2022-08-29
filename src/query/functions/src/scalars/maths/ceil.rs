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
use std::str;

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::function_common::assert_numeric;
use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::scalar_unary_op;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionFeatures;
use crate::scalars::Monotonicity;

#[derive(Clone)]
pub struct CeilFunction {
    display_name: String,
    return_type: DataTypeImpl,
}

impl CeilFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        assert_numeric(args[0])?;
        Ok(Box::new(CeilFunction {
            display_name: display_name.to_string(),
            return_type: args[0].clone(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .monotonicity()
                .num_arguments(1),
        )
    }
}

impl Function for CeilFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self) -> DataTypeImpl {
        self.return_type.clone()
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        _input_rows: usize,
    ) -> Result<ColumnRef> {
        let mut ctx = EvalContext::default();
        let data_type_id = columns[0].data_type().data_type_id();
        if data_type_id.is_integer() {
            return Ok(columns[0].column().clone());
        }
        match data_type_id {
            TypeID::Float32 => {
                let column = scalar_unary_op::<f32, f32, _>(
                    columns[0].column(),
                    |v: f32, _| v.ceil(),
                    &mut ctx,
                )?;
                Ok(column.arc())
            }

            TypeID::Float64 => {
                let column = scalar_unary_op::<f64, f64, _>(
                    columns[0].column(),
                    |v: f64, _| v.ceil(),
                    &mut ctx,
                )?;
                Ok(column.arc())
            }
            _ => unreachable!(),
        }
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        // Ceil function should be monotonically positive. For val_1 > val2, we should have ceil(val_1) >= ceil(val_2), and vise versa.
        // So we return the monotonicity same as the input.
        Ok(Monotonicity::clone_without_range(&args[0]))
    }
}

impl fmt::Display for CeilFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
