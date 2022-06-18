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

use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct AssumeNotNullFunction {
    return_type: DataTypeImpl,
}

impl AssumeNotNullFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        if args[0].is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                    "Can't accept null datatype to call {} function",
                    display_name
                )));
        }
        
        let return_type = remove_nullable(args[0]);
        Ok(Box::new(AssumeNotNullFunction {
            return_type
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .disable_passthrough_null()
                .num_arguments(1),
        )
    }
}

impl Function for AssumeNotNullFunction {
    fn name(&self) -> &str {
        "AssumeNotNullFunction"
    }

    fn return_type(&self) -> DataTypeImpl {
        self.return_type.clone()
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &common_datavalues::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        if columns[0].column().is_nullable() {
            let c: &NullableColumn = Series::check_get(columns[0].column())?;
            return Ok(c.inner().clone())
        }
        Ok(columns[0].column().clone())
    }
}

impl fmt::Display for AssumeNotNullFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AssumeNotNull")
    }
}
