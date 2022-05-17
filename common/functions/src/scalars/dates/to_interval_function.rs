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
use common_datavalues::IntervalKind;
use common_datavalues::IntervalType;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::default_column_cast;
use crate::scalars::FactoryCreator;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct ToIntervalFunction {
    display_name: String,
    interval_kind: IntervalKind,
}

impl ToIntervalFunction {
    pub fn try_create(
        display_name: String,
        interval_kind: IntervalKind,
        args: &[&DataTypeImpl],
    ) -> Result<Box<dyn Function>> {
        if !args[0].data_type_id().is_numeric() {
            return Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error: Unsupported type {:?}",
                args[0].data_type_id()
            )));
        }

        Ok(Box::new(ToIntervalFunction {
            display_name,
            interval_kind,
        }))
    }
}

impl Function for ToIntervalFunction {
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self) -> DataTypeImpl {
        IntervalType::new_impl(self.interval_kind)
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        _input_rows: usize,
    ) -> Result<ColumnRef> {
        default_column_cast(columns[0].column(), &i64::to_data_type())
    }
}

impl fmt::Display for ToIntervalFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

pub fn to_interval_function_creator(interval_kind: IntervalKind) -> FunctionDescription {
    let creator: FactoryCreator = Box::new(move |display_name, args| {
        ToIntervalFunction::try_create(display_name.to_string(), interval_kind, args)
    });

    FunctionDescription::creator(creator).features(FunctionFeatures::default().num_arguments(1))
}
