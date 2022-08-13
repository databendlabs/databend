// Copyright 2022 Datafuse Labs.
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
use common_datavalues::DataTypeImpl;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct VariantArrayLengthFunction {
    display_name: String,
}

impl VariantArrayLengthFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        let data_type = args[0];

        if !data_type.data_type_id().is_variant_or_array() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Invalid argument types for function '{}': ({:?})",
                display_name.to_uppercase(),
                data_type.data_type_id(),
            )));
        }

        Ok(Box::new(VariantArrayLengthFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl Function for VariantArrayLengthFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self) -> DataTypeImpl {
        u64::to_data_type()
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let array_column: &VariantColumn = Series::check_get(columns[0].column())?;
        let mut res: Vec<u64> = Vec::with_capacity(input_rows);

        for array in array_column.iter() {
            if !array.is_array() {
                return Err(ErrorCode::IllegalDataType(format!(
                    "Invalid argument types for function '{}': Variant is not VariantArray",
                    self.display_name.to_uppercase(),
                )));
            }
            let len = array.to_owned_scalar().as_array().unwrap().len() as u64;
            res.push(len);
        }

        Ok(Arc::new(PrimitiveColumn::new_from_vec(res)))
    }
}

impl fmt::Display for VariantArrayLengthFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}
