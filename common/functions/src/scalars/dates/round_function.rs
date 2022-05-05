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

use chrono_tz::Tz;
use common_datavalues::chrono::TimeZone;
use std::fmt;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::scalar_unary_op;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::Monotonicity;

#[derive(Clone)]
pub struct RoundFunction {
    display_name: String,
    round: u32,
}

impl RoundFunction {
    pub fn try_create(
        display_name: &str,
        args: &[&DataTypeImpl],
        round: u32,
    ) -> Result<Box<dyn Function>> {
        if args[0].data_type_id() != TypeID::Timestamp {
            return Err(ErrorCode::BadDataValueType(format!(
                "Function {} must have a Timestamp type as argument, but got {}",
                display_name,
                args[0].name(),
            )));
        }

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
    fn execute(&self, time: i64, tz: &Tz) -> i64 {
        let round = self.round as i64;
        time / MICROSECONDS / round * round * MICROSECONDS
    }
}

impl Function for RoundFunction {
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self) -> DataTypeImpl {
        TimestampType::new_impl(0)
    }

    fn eval(
        &self,
        func_ctx: FunctionContext,
        columns: &common_datavalues::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        let func = |val: i64, ctx: &mut EvalContext| self.execute(val, &ctx.tz);
        let mut eval_context = EvalContext::default();
        let tz = func_ctx.tz.parse::<Tz>().map_err(|_| {
            ErrorCode::InvalidTimezone("Timezone has been checked and should be valid")
        })?;
        eval_context.tz = tz;
        let col =
            scalar_unary_op::<i64, _, _>(columns[0].column(), func, &mut eval_context)?;
        for micros in col.iter() {
            let _ = check_timestamp(*micros)?;
        }
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
