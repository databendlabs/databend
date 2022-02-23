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

use crate::scalars::cast_column_field;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

#[derive(Clone)]
pub struct UnhexFunction {
    _display_name: String,
}

impl UnhexFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(UnhexFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl Function for UnhexFunction {
    fn name(&self) -> &str {
        "unhex"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        if !args[0].data_type_id().is_string() && !args[0].data_type_id().is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected string or null, but got {}",
                args[0].data_type_id()
            )));
        }

        Ok(StringType::arc())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        const BUFFER_SIZE: usize = 32;

        let col = cast_column_field(&columns[0], &StringType::arc())?;
        let col = col.as_any().downcast_ref::<StringColumn>().unwrap();

        let mut builder: ColumnBuilder<Vu8> = ColumnBuilder::with_capacity(input_rows);

        for val in col.iter() {
            if val.len() <= BUFFER_SIZE * 2 {
                let size = val.len() / 2;
                let mut buffer = vec![0u8; size];
                let buffer = &mut buffer[0..size];

                match hex::decode_to_slice(val, buffer) {
                    Ok(()) => builder.append(buffer),
                    Err(err) => {
                        return Err(ErrorCode::UnexpectedError(format!(
                            "{} can not unhex because: {}",
                            String::from_utf8_lossy(val),
                            err
                        )))
                    }
                }
            } else {
                return Err(ErrorCode::UnexpectedError(format!(
                    "{} is too long than buffer size",
                    String::from_utf8_lossy(val)
                )));
            }
        }

        Ok(builder.build(input_rows))
    }
}

impl fmt::Display for UnhexFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UNHEX")
    }
}
