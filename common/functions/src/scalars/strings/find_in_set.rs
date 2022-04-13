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
use common_exception::Result;

use crate::scalars::assert_string;
use crate::scalars::scalar_binary_op;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct FindInSetFunction {
    display_name: String,
}

impl FindInSetFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypePtr]) -> Result<Box<dyn Function>> {
        assert_string(args[0])?;
        assert_string(args[1])?;
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl Function for FindInSetFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(u64::to_data_type())
    }

    fn eval(
        &self,
        columns: &ColumnsWithField,
        _input_rows: usize,
        _func_ctx: FunctionContext,
    ) -> Result<ColumnRef> {
        let col = scalar_binary_op::<Vu8, Vu8, u64, _>(
            columns[0].column(),
            columns[1].column(),
            find_in_set,
            &mut EvalContext::default(),
        )?;
        Ok(col.arc())
    }
}

impl fmt::Display for FindInSetFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[inline]
fn find_in_set(str: &[u8], list: &[u8], _ctx: &mut EvalContext) -> u64 {
    if str.is_empty() || str.len() > list.len() {
        return 0;
    }
    let mut pos = 1;
    for (p, w) in list.windows(str.len()).enumerate() {
        if w[0] == 44 {
            pos += 1;
        } else if w == str && (p + w.len() == list.len() || list[p + w.len()] == 44) {
            return pos;
        }
    }
    0
}
use crate::scalars::FunctionContext;
