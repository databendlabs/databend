// Copyright 2020 Datafuse Labs.
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

use common_arrow::arrow::array::Array;
use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::DataSchema;
use common_datavalues::prelude::*;

use common_datavalues::DataType;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct RunningDifferenceFunction {
    display_name: String,
}

impl RunningDifferenceFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(RunningDifferenceFunction {
            display_name: display_name.to_string(),
        }))
    }
}

impl Function for RunningDifferenceFunction {
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        // 1. 支持number类型
        let array = match columns[0].data_type() {
            DataType::Date16 => {
                if let DataColumn::Constant(_, _) = columns[0].column() {
                    Ok(DataColumn::Constant(DataValue::Int16(Some(0i16)), input_rows))
                } else {
                    let inner_array = columns[0].column()
                        .to_array()?
                        .u16()?
                        .inner();
                    
                        let result_vec = Vec::with_capacity(inner_array.len());
                        // if inner_array.len() == 0 {
                        //    DataColumn::
                        //}
                        for index in 0.. inner_array.len() {    
                            match inner_array.is_null(index) {
                                true => result_vec.push(None), 
                                false => {
                                    if index > 0 {
                                        
                                    }
                                }
                            }
                        }

                    Ok(result.into())
                }
            },
            DataType::Date32 => {

            },
            DataType::DateTime32(_) => {

            },
            _ => unreachable!(),
            
        } 

        Ok(columns[0].column().clone())
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn num_arguments(&self) -> usize {
        1
    }
}

impl fmt::Display for RunningDifferenceFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}
