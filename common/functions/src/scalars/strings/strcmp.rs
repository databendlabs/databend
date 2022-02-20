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

use common_datavalues::prelude::*;
use common_exception::Result;
use itertools::izip;

use crate::scalars::assert_string;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::ScalarBinaryExpression;

#[derive(Clone)]
pub struct StrcmpFunction {
    display_name: String,
}

impl StrcmpFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(StrcmpFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl Function for StrcmpFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        for arg in args {
            assert_string(*arg)?;
        }
        Ok(i8::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let binary = ScalarBinaryExpression::<Vu8, Vu8, i8, _>::new(strcmp);
        let col = binary.eval(
            columns[0].column(),
            columns[1].column(),
            &mut EvalContext::default(),
        )?;
        Ok(col.arc())
    }
}

impl fmt::Display for StrcmpFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[inline]
fn strcmp(s1: &[u8], s2: &[u8], _ctx: &mut EvalContext) -> i8 {
    let res = match s1.len().cmp(&s2.len()) {
        Ordering::Equal => {
            let mut res = Ordering::Equal;
            for (s1i, s2i) in izip!(s1, s2) {
                match s1i.cmp(s2i) {
                    Ordering::Equal => continue,
                    ord => {
                        res = ord;
                        break;
                    }
                }
            }
            res
        }
        ord => ord,
    };
    match res {
        Ordering::Equal => 0,
        Ordering::Greater => 1,
        Ordering::Less => -1,
    }
}
