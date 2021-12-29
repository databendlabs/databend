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
use common_datavalues::DataType;
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;
use rand::prelude::*;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct RandomFunction {
    display_name: String,
}

impl RandomFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(RandomFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().variadic_arguments(0, 1))
    }
}

impl Function for RandomFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataType> {
        if args.is_empty()
            || matches!(
                args[0].data_type(),
                DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::String
                    | DataType::Null
            )
        {
            Ok(DataType::Float64)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "Expected numeric types, but got {}",
                args[0]
            )))
        }
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let r_column: DataColumn = match columns.len() {
            1 => {
                let seed_column: &DataColumn =
                    &columns[0].column().cast_with_type(&DataType::UInt64)?;

                match seed_column {
                    DataColumn::Constant(seed, _) => {
                        let s: u64 = if seed.is_null() {
                            0
                        } else {
                            DFTryFrom::try_from(seed.clone())?
                        };
                        let mut rng = rand::rngs::StdRng::seed_from_u64(s);
                        DFFloat64Array::new_from_iter(
                            (0..input_rows).into_iter().map(|_| rng.gen::<f64>()),
                        )
                    }

                    DataColumn::Array(seed_series) => seed_series.u64()?.apply_cast_numeric(|s| {
                        let mut rng = StdRng::seed_from_u64(s);
                        rng.gen::<f64>()
                    }),
                }
            }
            _ => {
                let mut rng = rand::thread_rng();
                DFFloat64Array::new_from_iter((0..input_rows).into_iter().map(|_| rng.gen::<f64>()))
            }
        }
        .into();
        Ok(r_column.resize_constant(input_rows))
    }
}

impl fmt::Display for RandomFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
