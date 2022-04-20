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

use common_datavalues::DataTypePtr;
use common_datavalues::StructColumn;
use common_datavalues::StructType;
use common_exception::Result;

use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct TupleFunction {
    _display_name: String,
    result_type: DataTypePtr,
}

impl TupleFunction {
    pub fn try_create_func(
        _display_name: &str,
        args: &[&DataTypePtr],
    ) -> Result<Box<dyn Function>> {
        let names = (0..args.len())
            .map(|i| format!("item_{}", i))
            .collect::<Vec<_>>();
        let types = args.iter().map(|x| (*x).clone()).collect::<Vec<_>>();
        let result_type = Arc::new(StructType::create(names, types));

        Ok(Box::new(TupleFunction {
            _display_name: "tuple".to_string(),
            result_type,
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .disable_passthrough_null()
                .variadic_arguments(1, usize::MAX),
        )
    }
}

impl Function for TupleFunction {
    fn name(&self) -> &str {
        "TupleFunction"
    }

    fn return_type(&self) -> DataTypePtr {
        self.result_type.clone()
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &common_datavalues::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        let cols = columns
            .iter()
            .map(|v| v.column().clone())
            .collect::<Vec<_>>();
        let arr: StructColumn = StructColumn::from_data(cols, self.result_type.clone());
        Ok(Arc::new(arr))
    }
}

impl std::fmt::Display for TupleFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TUPLE")
    }
}
