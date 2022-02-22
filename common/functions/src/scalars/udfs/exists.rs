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

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

#[derive(Clone)]
pub struct ExistsFunction;

impl ExistsFunction {
    pub fn try_create(_display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ExistsFunction {}))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().bool_function().num_arguments(1))
    }
}

impl Function for ExistsFunction {
    fn name(&self) -> &str {
        "ExistsFunction"
    }

    fn return_type(
        &self,
        _args: &[&common_datavalues::DataTypePtr],
    ) -> Result<common_datavalues::DataTypePtr> {
        Ok(bool::to_data_type())
    }

    fn eval(
        &self,
        columns: &common_datavalues::ColumnsWithField,
        input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        if columns[0].column().is_const() || columns[0].column().len() == 1 {
            let value = columns[0].column().get(0);
            match value {
                DataValue::Array(v) => {
                    let c = Series::from_data(vec![!v.is_empty()]);
                    Ok(ConstColumn::new(c, input_rows).arc())
                }
                DataValue::Struct(v) => {
                    if let Some(DataValue::Array(values)) = v.get(0) {
                        let c = Series::from_data(vec![!values.is_empty()]);
                        Ok(ConstColumn::new(c, input_rows).arc())
                    } else {
                        Err(ErrorCode::LogicalError(
                            "Logical error: subquery result set must be Struct(List(Some)).",
                        ))
                    }
                }
                _ => Err(ErrorCode::LogicalError(
                    "Logical error: subquery result set must be List(Some) or Struct(List(Some)).",
                )),
            }
        } else {
            Err(ErrorCode::LogicalError(
                "Logical error: subquery result set must be const.",
            ))
        }
    }

    fn passthrough_constant(&self) -> bool {
        false
    }
}

impl fmt::Display for ExistsFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EXISTS")
    }
}
