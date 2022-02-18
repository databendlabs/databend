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
use std::sync::Arc;

use common_datavalues::StructColumn;
use common_datavalues::StructType;
use common_exception::Result;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

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
        FunctionDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(1, usize::MAX),
        )
    }
}

impl Function for TupleFunction {
    fn name(&self) -> &str {
        "TupleFunction"
    }

    fn return_type(
        &self,
        args: &[&common_datavalues::DataTypePtr],
    ) -> Result<common_datavalues::DataTypePtr> {
        let names = (0..args.len())
            .map(|i| format!("item_{}", i))
            .collect::<Vec<_>>();
        let types = args.iter().map(|x| (*x).clone()).collect::<Vec<_>>();
        let t = Arc::new(StructType::create(names, types));
        Ok(t)
    }

    fn eval(
        &self,
        columns: &common_datavalues::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        let mut cols = vec![];
        let mut types = vec![];

        let names = (0..columns.len())
            .map(|i| format!("item_{}", i))
            .collect::<Vec<_>>();

        for c in columns {
            cols.push(c.column().clone());
            types.push(c.data_type().clone());
        }

        let t = Arc::new(StructType::create(names, types));

        let arr: StructColumn = StructColumn::from_data(cols, t);
        Ok(Arc::new(arr))
    }

    fn passthrough_null(&self) -> bool {
        false
    }
}

impl std::fmt::Display for TupleFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TUPLE")
    }
}
