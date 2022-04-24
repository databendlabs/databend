// Copyright 2022 Datafuse Labs
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
use common_datavalues::DataTypePtr;
use common_exception::Result;
use common_io::prelude::convert_byte_size;
use num_traits::AsPrimitive;

use crate::scalars::assert_numeric;
use crate::scalars::scalar_unary_op;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct HumanizeFunction {
    display_name: String,
}

impl HumanizeFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypePtr]) -> Result<Box<dyn Function>> {
        assert_numeric(args[0])?;
        Ok(Box::new(HumanizeFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

fn humanize<S>(value: S, _ctx: &mut EvalContext) -> Vec<u8>
where S: AsPrimitive<f64> {
    Vec::from(convert_byte_size(value.as_()))
}

impl Function for HumanizeFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self) -> DataTypePtr {
        Vu8::to_data_type()
    }

    fn eval(
        &self,
        _func_ctx: crate::scalars::FunctionContext,
        columns: &common_datavalues::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$F| {
            let col = scalar_unary_op::<$F, Vu8, _>(columns[0].column(), humanize, &mut EvalContext::default())?;
            Ok(col.arc())
        },{
            unreachable!()
        })
    }
}

impl fmt::Display for HumanizeFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
