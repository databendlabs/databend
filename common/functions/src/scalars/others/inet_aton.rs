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
use std::str;

use common_datavalues::prelude::DFUInt32Array;
use common_datavalues::prelude::DataColumn;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::prelude::NewDataArray;
use common_datavalues::DataType;
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
#[doc(alias = "IPv4StringToNumFunction")]
pub struct InetAtonFunction {
    display_name: String,
}

impl InetAtonFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(InetAtonFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl Function for InetAtonFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn nullable(&self, _args: &[DataTypeAndNullable]) -> Result<bool> {
        Ok(true)
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataType> {
        if args[0].is_string() || args[0].is_null() {
            Ok(DataType::UInt32)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "Expected string or null type, but got {}",
                args[0]
            )))
        }
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let opt_iter = columns[0]
            .column()
            .to_minimal_array()?
            .cast_with_type(&DataType::String)?;

        let opt_iter = opt_iter.string()?.into_iter().map(|vo| {
            vo.and_then(
                |v| match String::from_utf8_lossy(v).parse::<std::net::Ipv4Addr>() {
                    Ok(a) => Some(u32::from(a)),
                    Err(_) => None,
                },
            )
        });

        let result = DFUInt32Array::new_from_opt_iter(opt_iter);
        let column: DataColumn = result.into();
        Ok(column.resize_constant(columns[0].column().len()))
    }
}

impl fmt::Display for InetAtonFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
