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
use crate::scalars::assert_string;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::ScalarBinaryExpression;

pub type LeftFunction = LeftRightFunction<true>;
pub type RightFunction = LeftRightFunction<false>;

#[inline]
fn left<'a, S>(str: &'a [u8], index: S, _ctx: &mut EvalContext) -> &'a [u8]
where S: AsPrimitive<usize> {
    let index = index.as_();
    if index < str.len() {
        return &str[0..index];
    }
    str
}

#[inline]
fn right<'a, S>(str: &'a [u8], index: S, _ctx: &mut EvalContext) -> &'a [u8]
where S: AsPrimitive<usize> {
    let index = index.as_();
    if index < str.len() {
        return &str[str.len() - index..];
    }
    str
}

#[derive(Clone)]
pub struct LeftRightFunction<const IS_LEFT: bool> {
    display_name: String,
}

impl<const IS_LEFT: bool> LeftRightFunction<IS_LEFT> {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl<const IS_LEFT: bool> Function for LeftRightFunction<IS_LEFT> {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        assert_string(args[0])?;
        assert_numeric(args[1])?;
        Ok(Vu8::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        match IS_LEFT {
            true => {
                with_match_primitive_type_id!(columns[1].data_type().data_type_id(), |$S| {
                    let binary = ScalarBinaryExpression::<Vu8, $S, Vu8, _>::new_ref(left);
                    let col = binary.eval_ref(columns[0].column(), columns[1].column(), &mut EvalContext::default())?;
                    Ok(Arc::new(col))
                },{
                    unreachable!()
                })
            }
            false => {
                with_match_primitive_type_id!(columns[1].data_type().data_type_id(), |$S| {
                    let binary = ScalarBinaryExpression::<Vu8, $S, Vu8, _>::new_ref(right);
                    let col = binary.eval_ref(columns[0].column(), columns[1].column(), &mut EvalContext::default())?;
                    Ok(Arc::new(col))
                },{
                    unreachable!()
                })
            }
        }
    }
}

impl<const IS_LEFT: bool> fmt::Display for LeftRightFunction<IS_LEFT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.display_name)
    }
}
