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
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::Monotonicity;
use crate::scalars::ScalarUnaryExpression;

#[derive(Clone)]
pub struct RoundFunction {
    display_name: String,
    round: u32,
}

impl RoundFunction {
    pub fn try_create(display_name: &str, round: u32) -> Result<Box<dyn Function>> {
        let s = Self {
            display_name: display_name.to_owned(),
            round,
        };

        Ok(Box::new(s))
    }

    // TODO: (sundy-li)
    // Consider about the timezones/offsets
    // Currently: assuming timezone offset is a multiple of round.
    #[inline]
    fn execute(&self, time: u32) -> u32 {
        time / self.round * self.round
    }
}

impl Function for RoundFunction {
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(
        &self,
        args: &[&common_datavalues::DataTypePtr],
    ) -> Result<common_datavalues::DataTypePtr> {
        if args[0].data_type_id() == TypeID::DateTime32 {
            return Ok(DateTime32Type::arc(None));
        } else {
            return Err(ErrorCode::BadDataValueType(format!(
                "Function {} must have a DateTime type as argument, but got {}",
                self.display_name,
                args[0].name(),
            )));
        }
    }

    fn eval(
        &self,
        columns: &common_datavalues::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        let unary = ScalarUnaryExpression::<u32, _, _>::new(|val: u32, _ctx: &mut EvalContext| {
            self.execute(val)
        });
        let col = unary.eval(columns[0].column(), &mut EvalContext::default())?;
        Ok(col.arc())
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        Ok(Monotonicity::clone_without_range(&args[0]))
    }
}

impl fmt::Display for RoundFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
