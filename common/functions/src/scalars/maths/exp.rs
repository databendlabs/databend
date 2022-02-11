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

use common_datavalues2::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::cast_column_field;
use crate::scalars::function2_factory::Function2Description;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;

#[derive(Clone)]
pub struct ExpFunction {
    _display_name: String,
}

impl ExpFunction {
    pub fn try_create(_display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(ExpFunction {
            _display_name: _display_name.to_string(),
        }))
    }

    pub fn desc() -> Function2Description {
        Function2Description::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl Function2 for ExpFunction {
    fn name(&self) -> &str {
        &*self._display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        let data_type = if args[0].data_type_id().is_numeric()
            || args[0].data_type_id().is_string()
            || args[0].data_type_id().is_null()
        {
            Ok(Float64Type::arc())
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "Expected numeric, but got {}",
                args[0].data_type_id()
            )))
        }?;

        Ok(data_type)
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let column = cast_column_field(&columns[0], &Float64Type::arc())?;
        let viewer = f64::try_create_viewer(&column)?;
        let input_rows = viewer.size();
        let mut builder = ColumnBuilder::<f64>::with_capacity(input_rows);
        for val in viewer.iter() {
            builder.append(val.exp());
        }
        Ok(builder.build(input_rows))
    }
}

impl fmt::Display for ExpFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EXP")
    }
}
