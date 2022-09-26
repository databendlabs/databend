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
pub struct InFunction<const NEGATED: bool> {
    is_null: bool,
    is_nullable: bool,
}

impl<const NEGATED: bool> InFunction<NEGATED> {
    pub fn try_create(_display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        let type_id = remove_nullable(args[0]).data_type_id();
        if type_id.is_interval() || type_id.is_array() || type_id.is_struct() {
            return Err(ErrorCode::UnexpectedError(format!(
                "{} type is not supported for IN now",
                type_id
            )));
        }

        let is_null = args[0].data_type_id() == TypeID::Null;
        let is_nullable = args[0].is_nullable();
        Ok(Box::new(InFunction::<NEGATED> {
            is_null,
            is_nullable,
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .disable_passthrough_null()
                .variadic_arguments(2, usize::MAX),
        )
    }
}

impl<const NEGATED: bool> Function for InFunction<NEGATED> {
    fn name(&self) -> &str {
        "InFunction"
    }

    fn return_type(&self) -> DataTypeImpl {
        if self.is_null {
            return NullType::new_impl();
        }
        if self.is_nullable {
            return NullableType::new_impl(BooleanType::new_impl());
        }
        BooleanType::new_impl()
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        _columns: &ColumnsWithField,
        _input_rows: usize,
    ) -> Result<ColumnRef> {
        Result::Err(ErrorCode::LogicalError("Should use in_evaulator"))
    }
}

impl<const NEGATED: bool> fmt::Display for InFunction<NEGATED> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if NEGATED {
            write!(f, "NOT IN")
        } else {
            write!(f, "IN")
        }
    }
}
