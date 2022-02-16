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

use common_datavalues2::prelude::*;
use common_datavalues2::with_match_primitive_type_id;
use common_exception::Result;
use num_traits::AsPrimitive;

use crate::scalars::assert_numeric;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;
use crate::scalars::Function2Description;
use crate::scalars::ScalarUnaryExpression;

// Returns a string consisting of N space characters.
#[derive(Clone)]
pub struct SpaceFunction {
    display_name: String,
}

impl SpaceFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> Function2Description {
        Function2Description::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl Function2 for SpaceFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        assert_numeric(args[0])?;
        Ok(Vu8::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        with_match_primitive_type_id!(columns[0].data_type().data_type_id(), |$S| {
            let unary = ScalarUnaryExpression::<$S, Vu8, _>::new(|n: $S| -> Vu8 { vec![32u8; n.as_()] });
            let col = unary.eval(columns[0].column())?;
            Ok(Arc::new(col))
        },{
            unreachable!()
        })
    }
}

impl fmt::Display for SpaceFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
