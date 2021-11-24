// Copyright 2020 Datafuse Lfloor.
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

use common_datavalues::is_numeric;
use common_datavalues::prelude::DFStringArray;
use common_datavalues::prelude::DataColumn;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::prelude::NewDataArray;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
#[doc(alias = "IPv4NumToStringFunction")]
pub struct InetNtoaFunction {
    display_name: String,
}

impl InetNtoaFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(InetNtoaFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

impl Function for InetNtoaFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if is_numeric(&args[0]) || args[0] == DataType::String || args[0] == DataType::Null {
            Ok(DataType::String)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "Expected numeric or string or null type, but got {}",
                args[0]
            )))
        }
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let opt_iter = columns[0]
            .column()
            .to_minimal_array()?
            .cast_with_type(&DataType::Float64)?;
        let opt_iter = opt_iter.f64()?.iter().map(|val_opt| {
            val_opt.and_then(|&val| {
                if val.is_nan() || val < 0.0 || val > u32::MAX as f64 {
                    None
                } else {
                    Some(std::net::Ipv4Addr::from((val as u32).to_be_bytes()).to_string())
                }
            })
        });
        let result = DFStringArray::new_from_opt_iter(opt_iter);
        let column: DataColumn = result.into();
        Ok(column.resize_constant(columns[0].column().len()))
    }
}

impl fmt::Display for InetNtoaFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
