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

use common_datavalues::prelude::*;
use common_datavalues::DataTypeImpl;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct ObjectKeysFunction {
    display_name: String,
}

impl ObjectKeysFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        if !args[0].data_type_id().is_variant_or_object() {
            return Err(ErrorCode::BadDataValueType(format!(
                "Unsupported type: {}",
                args[0].data_type_id()
            )));
        };

        Ok(Box::new(ObjectKeysFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl Function for ObjectKeysFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self) -> DataTypeImpl {
        ArrayType::new_impl(StringType::new_impl())
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let object_column: &VariantColumn = Series::check_get(columns[0].column())?;
        let mut data_column = MutableStringColumn::default();
        let mut offsets: Vec<i64> = vec![0];
        for i in 0..input_rows {
            let object = match object_column.get_data(i).as_object() {
                Some(x) => x,
                None => {
                    return Err(ErrorCode::BadDataValueType("Variant is not Object Type"));
                }
            };
            for key in object.keys() {
                data_column.append_value(key);
            }
            offsets.push(object.keys().len() as i64);
        }

        Ok(ArrayColumn::from_data(
            DataTypeImpl::Array(ArrayType::create(StringType::new_impl())),
            offsets.into(),
            data_column.to_column(),
        )
        .arc())
    }
}

impl fmt::Display for ObjectKeysFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}
