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
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

pub trait NumberOperator<R>: Send + Sync + Clone + Default + 'static {
    const IS_DETERMINISTIC: bool;
    const MAYBE_MONOTONIC: bool;

    fn apply<'a>(&'a mut self, _: &'a [u8]) -> R;
}

#[derive(Clone)]
pub struct String2NumberFunction<T, R> {
    display_name: String,
    t: PhantomData<T>,
    r: PhantomData<R>,
}

impl<T, R> String2NumberFunction<T, R>
where
    T: NumberOperator<R>,
    R: PrimitiveType + Clone + ToDataType,
{
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
            t: PhantomData,
            r: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        let mut features = FunctionFeatures::default().num_arguments(1);

        if T::IS_DETERMINISTIC {
            features = features.deterministic();
        }

        if T::MAYBE_MONOTONIC {
            features = features.monotonicity();
        }

        FunctionDescription::creator(Box::new(Self::try_create)).features(features)
    }
}

/// A common function template that transform string column into number column
/// Eg: length
impl<T, R> Function for String2NumberFunction<T, R>
where
    T: NumberOperator<R> + Clone,
    R: PrimitiveType + Clone + ToDataType,
{
    fn name(&self) -> &str {
        &self.display_name
    }

    fn return_type(
        &self,
        args: &[&common_datavalues::DataTypePtr],
    ) -> Result<common_datavalues::DataTypePtr> {
        // We allow string AND null as input
        if args[0].data_type_id().is_string() {
            Ok(R::to_data_type())
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "Expected string, numeric or null, but got {:?}",
                args[0]
            )))
        }
    }

    fn eval(
        &self,
        columns: &common_datavalues::ColumnsWithField,
        input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        let mut op = T::default();
        let column: &StringColumn = Series::check_get(columns[0].column())?;
        let mut array = Vec::with_capacity(input_rows);
        for x in column.iter() {
            let r = op.apply(x);
            array.push(r);
        }

        Ok(Arc::new(PrimitiveColumn::new_from_vec(array)))
    }
}

impl<T, R> fmt::Display for String2NumberFunction<T, R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}
