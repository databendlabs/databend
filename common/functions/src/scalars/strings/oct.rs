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

use std::cmp::Ordering;
use std::fmt;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::cast_column_field;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

trait OctString {
    fn oct_string(self) -> String;
}

impl OctString for i64 {
    fn oct_string(self) -> String {
        match self.cmp(&0) {
            Ordering::Less => {
                format!("-0{:o}", self.unsigned_abs())
            }
            Ordering::Equal => "0".to_string(),
            Ordering::Greater => format!("0{:o}", self),
        }
    }
}

impl OctString for u64 {
    fn oct_string(self) -> String {
        if self == 0 {
            "0".to_string()
        } else {
            format!("0{:o}", self)
        }
    }
}

#[derive(Clone)]
pub struct OctFunction {
    _display_name: String,
}

impl OctFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(OctFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl Function for OctFunction {
    fn name(&self) -> &str {
        "oct"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        if !args[0].data_type_id().is_numeric() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer but got {}",
                args[0].data_type_id()
            )));
        }

        Ok(StringType::arc())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let mut builder: ColumnBuilder<Vu8> = ColumnBuilder::with_capacity(input_rows);

        match columns[0].data_type().data_type_id() {
            TypeID::UInt8 | TypeID::UInt16 | TypeID::UInt32 | TypeID::UInt64 => {
                let col = cast_column_field(&columns[0], &UInt64Type::arc())?;
                let col = col.as_any().downcast_ref::<UInt64Column>().unwrap();
                for val in col.iter() {
                    builder.append(val.oct_string().as_bytes());
                }
            }
            _ => {
                let col = cast_column_field(&columns[0], &Int64Type::arc())?;
                let col = col.as_any().downcast_ref::<Int64Column>().unwrap();
                for val in col.iter() {
                    builder.append(val.oct_string().as_bytes());
                }
            }
        }
        Ok(builder.build(input_rows))
    }
}

impl fmt::Display for OctFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OCT")
    }
}
