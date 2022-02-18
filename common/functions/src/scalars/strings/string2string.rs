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
use std::marker::PhantomData;
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_datavalues::StringType;
use common_datavalues::TypeID;
use common_exception::ErrorCode;
use common_exception::Result;

// use common_tracing::tracing;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

pub trait StringOperator: Send + Sync + Clone + Default + 'static {
    fn try_apply<'a>(&'a mut self, _: &'a [u8], _: &mut [u8]) -> Result<usize>;

    // as much as the original bytes
    fn estimate_bytes(&self, array: &StringColumn) -> usize {
        array.values().len()
    }
}

/// A common function template that transform string column into string column
/// Eg: trim, lower, upper, etc.
#[derive(Clone)]
pub struct String2StringFunction<T> {
    display_name: String,
    _marker: PhantomData<T>,
}

impl<T: StringOperator> String2StringFunction<T> {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
            _marker: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl<T: StringOperator> Function for String2StringFunction<T> {
    fn name(&self) -> &str {
        &self.display_name
    }

    fn return_type(
        &self,
        args: &[&common_datavalues::DataTypePtr],
    ) -> Result<common_datavalues::DataTypePtr> {
        if args[0].data_type_id() != TypeID::String {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected string arg, but got {:?}",
                args[0]
            )));
        }
        Ok(StringType::arc())
    }

    fn eval(
        &self,
        columns: &common_datavalues::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        let mut op = T::default();
        let column: &StringColumn = Series::check_get(columns[0].column())?;
        let estimate_bytes = op.estimate_bytes(column);
        let col = StringColumn::try_transform(column, estimate_bytes, |val, buffer| {
            op.try_apply(val, buffer)
        })?;
        Ok(Arc::new(col))
    }
}

impl<F> fmt::Display for String2StringFunction<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.display_name)
    }
}
