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
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_type_id;
use common_datavalues::ColumnWithField;
use common_exception::Result;

use crate::scalars::function_common::assert_numeric;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::Monotonicity;
use crate::scalars::ScalarUnaryExpression;

#[derive(Clone)]
pub struct SignFunction {
    display_name: String,
}

impl SignFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(SignFunction {
            display_name: display_name.to_string(),
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

fn sign<S>(value: S, _ctx: &mut EvalContext) -> i8
where S: Scalar + Default + PartialOrd {
    match value.partial_cmp(&S::default()) {
        Some(std::cmp::Ordering::Greater) => 1,
        Some(std::cmp::Ordering::Less) => -1,
        _ => 0,
    }
}

impl Function for SignFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        assert_numeric(args[0])?;
        Ok(i8::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let mut ctx = EvalContext::default();
        with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
            let unary = ScalarUnaryExpression::<$S, i8, _>::new(sign::<$S>);
            let col = unary.eval(columns[0].column(), &mut ctx)?;
            Ok(Arc::new(col))
        },{
            unreachable!()
        })
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        let mono = args[0].clone();
        if mono.is_constant {
            return Ok(Monotonicity::create_constant());
        }

        // check whether the left/right boundary is numeric or not.
        let is_boundary_numeric = |boundary: Option<ColumnWithField>| -> bool {
            if let Some(column_field) = boundary {
                column_field.data_type().data_type_id().is_numeric()
            } else {
                false
            }
        };

        // sign operator is monotonically non-decreasing for numeric values. However,'String' input is an exception.
        // For example, query like "SELECT sign('-1'), sign('+1'), '-1' >= '+1';" returns -1, 1, 1(true),
        // which is not monotonically increasing.
        if is_boundary_numeric(mono.left) || is_boundary_numeric(mono.right) {
            return Ok(Monotonicity::clone_without_range(&args[0]));
        }

        Ok(Monotonicity::default())
    }
}

impl fmt::Display for SignFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
