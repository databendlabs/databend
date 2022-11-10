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
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::assert_string;
use crate::scalars::cast_column_field;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

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
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        if display_name.to_lowercase() != "upper" {
            assert_string(args[0])?;
        }

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

    fn return_type(&self) -> DataTypeImpl {
        StringType::new_impl()
    }

    fn eval(
        &self,
        func_ctx: FunctionContext,
        columns: &common_datavalues::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        let mut op = T::default();
        match columns[0].data_type().data_type_id() {
            TypeID::UInt8
            | TypeID::UInt16
            | TypeID::UInt32
            | TypeID::UInt64
            | TypeID::Int8
            | TypeID::Int16
            | TypeID::Int32
            | TypeID::Int64
            | TypeID::String => {
                let col = cast_column_field(
                    &columns[0],
                    columns[0].data_type(),
                    &StringType::new_impl(),
                    &func_ctx,
                )?;
                let column = col.as_any().downcast_ref::<StringColumn>().unwrap();
                let estimate_bytes = op.estimate_bytes(column);
                let col = StringColumn::try_transform(column, estimate_bytes, |val, buffer| {
                    op.try_apply(val, buffer)
                })?;
                Ok(Arc::new(col))
            }
            _ => Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string type but got {}",
                columns[0].data_type().data_type_id()
            ))),
        }
    }
}

impl<F> fmt::Display for String2StringFunction<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.display_name)
    }
}
