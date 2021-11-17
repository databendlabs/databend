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

use common_arrow::arrow::array::StructArray;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_datavalues::arrays::DFStructArray;
use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::series::IntoSeries;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct TupleFunction {
    _display_name: String,
}

impl TupleFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(TupleFunction {
            _display_name: "tuple".to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic())
    }
}

impl Function for TupleFunction {
    fn name(&self) -> &str {
        "TupleFunction"
    }

    fn num_arguments(&self) -> usize {
        0
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((1, usize::MAX))
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let fields = args
            .iter()
            .enumerate()
            .map(|(i, x)| DataField::new(format!("item_{}", i).as_str(), x.clone(), true))
            .collect::<Vec<_>>();
        Ok(DataType::Struct(fields))
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let mut arrays = vec![];
        let mut fields = vec![];
        for (i, x) in columns.iter().enumerate() {
            let xfield = x.field();
            let field = DataField::new(
                format!("item_{}", i).as_str(),
                xfield.data_type().clone(),
                xfield.is_nullable(),
            );
            fields.push(field.to_arrow());
            arrays.push(x.column().to_array()?.get_array_ref());
        }
        let arr: DFStructArray =
            StructArray::from_data(ArrowType::Struct(fields), arrays, None).into();
        Ok(arr.into_series().into())
    }
}

impl std::fmt::Display for TupleFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TUPLE")
    }
}
