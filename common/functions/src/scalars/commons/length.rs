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

use common_datavalues::prelude::*;
use common_datavalues::DataTypeImpl;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::ArrayLengthFunction;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;
use crate::scalars::StringLengthFunction;
use crate::scalars::VariantArrayLengthFunction;

pub struct LengthFunction {
    _display_name: String,
}

impl LengthFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        let data_type = args[0];

        if data_type.data_type_id().is_string() {
            StringLengthFunction::try_create(display_name, args)
        } else if data_type.data_type_id().is_array() {
            ArrayLengthFunction::try_create(display_name, args)
        } else if data_type.data_type_id().is_variant_or_array() {
            VariantArrayLengthFunction::try_create(display_name, args)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "Invalid argument types for function '{}': ({:?})",
                display_name.to_uppercase(),
                data_type.data_type_id(),
            )))
        }
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}
