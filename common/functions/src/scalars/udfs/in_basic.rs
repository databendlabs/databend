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
use std::collections::HashSet;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::DataType;
use common_datavalues::DataTypeAndNullable;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_datavalues::prelude::Series;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct InFunction<const NEGATED: bool>;

impl<const NEGATED: bool> InFunction<NEGATED> {
    pub fn try_create(_display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(InFunction::<NEGATED> {}))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().bool_function().variadic_arguments(2, usize::MAX))
    }
}

impl<const NEGATED: bool> Function for InFunction<NEGATED> {
    fn name(&self) -> &str {
        "InFunction"
    }

    fn return_type(&self, _args: &[DataTypeAndNullable]) -> Result<DataTypeAndNullable> {
        let dt = DataType::Boolean;
        Ok(DataTypeAndNullable::create(&dt, false))
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let input_column = columns[0].column();
        let input_array = match input_column {
            DataColumn::Array(array) => array.to_owned(),
            DataColumn::Constant(scalar, _) => scalar.to_array()?,
        };

        let input_dt = input_array.data_type();
        common_tracing::tracing::debug!("input_dt: {:#?}", input_dt);
        let check_arrays = &columns[1..];
        let mut check_column_set = Vec::new();
        for column in check_arrays {
            let array = column.column();
            let array = match array {
                DataColumn::Array(_) => {
                    return Err(ErrorCode::UnexpectedError("logical bug"));
                },
                DataColumn::Constant(scalar, _) => {
                    // cast to DataType::Struct(for tuple struct) will fail, maybe we can consider get into seperate ColumnSet to compare
                    common_tracing::tracing::debug!("input_dt: {:#?}", scalar.data_type());
                    scalar.to_array()?.cast_with_type(input_dt)?
                }
            };
            check_column_set.push(ColumnSet::new(&array)?);
        }
        
        unimplemented!()
    }
}

impl<const NEGATED: bool> fmt::Display for InFunction<NEGATED> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if NEGATED {
            write!(f, "NOT IN")
        }else {
            write!(f, "IN")
        }
    }
}

struct ColumnSet {
    inner: HashSet<DataValue>,
}

impl ColumnSet {
    fn new(array: &Series) -> Result<Self> {
        let mut inner = HashSet::new();
        for idx in 0..array.len() {
            let val = array.try_get(idx)?;
            // f32 and f32 can not impl `Eq` and `Hash`
            // inner.insert(val);
        }
        Ok(Self {
            inner
        })
    }
}
