// Copyright 2022 Datafuse Labs.
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

use common_datavalues2::Series;
use common_datavalues2::TypeID;
use common_datavalues2::prelude::*;
use common_exception::Result;
use common_exception::ErrorCode;
use common_datavalues2::with_match_primitive_type;
use crate::scalars::{Function2, Function2Description};
use crate::scalars::function_factory::FunctionFeatures;

#[derive(Clone, Debug)]
pub struct IfFunction2 {
    display_name: String,
}

impl IfFunction2 {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(IfFunction2 {
            display_name: display_name.to_string()
        }))
    }

    pub fn desc() -> Function2Description {
        let mut features = FunctionFeatures::default().num_arguments(3);
        features = features.deterministic();
        Function2Description::creator(Box::new(Self::try_create)).features(features)
    }
}

impl Function2 for IfFunction2 {
    fn name(&self) -> &str {
        "IfFunction"
    }

    fn return_type(&self, args: &[&common_datavalues2::DataTypePtr]) -> Result<DataTypePtr> {
        let dt = UInt8Type::arc();
        Ok(dt)
    }

    fn eval(&self, columns: &common_datavalues2::ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let predicate = columns[0].column();
        let lhs = columns[1].column();
        let rhs = columns[2].column();

        if lhs.data_type() != rhs.data_type() {
            return Err(ErrorCode::BadDataValueType(
                "lhs and rhs must have the same data type".to_string(),
            ));
        }

        let type_id = lhs.data_type_id();
        
        match type_id {
            TypeID::UInt8 => {
                let predicate_wrapper = ColumnViewer::<bool>::create(predicate)?;
                let lhs_wrapper = ColumnViewer::<u8>::create(lhs)?;
                let rhs_wrapper = ColumnViewer::<u8>::create(rhs)?;
                let size = lhs_wrapper.len();
        
                let mut builder = NullableColumnBuilder::<u8>::with_capacity(size);
        
                for row in 0..size {
                    let predicate = predicate_wrapper.value(row);
                    let valid = predicate_wrapper.valid_at(row);
                     if predicate {
                        builder.append(lhs_wrapper.value(row), valid & lhs_wrapper.valid_at(row));
                    } else {
                        builder.append(rhs_wrapper.value(row), valid & rhs_wrapper.valid_at(row));
                    };
                }
        
                Ok(builder.build(size))
            },
            _ => {
                unimplemented!()
            }
        }
        // with_match_primitive_type!(type_id, |$T| {
        //     let lhs_wrapper = ColumnViewer::<$T>::create(lhs)?;
        //     let rhs_wrapper = ColumnViewer::<$T>::create(rhs)?;
        //     let size = lhs_wrapper.len();

        //     let mut builder = NullableColumnBuilder::<$T>::with_capacity(size);

        //     for row in 0..size {
        //         let valid = validity_predict.get_bit(row);
        //         if bools.get_bit(row) {
        //             builder.append(lhs_wrapper.value(row), valid & lhs_wrapper.valid_at(row));
        //         } else {
        //             builder.append(rhs_wrapper.value(row), valid & rhs_wrapper.valid_at(row));
        //         };
        //     }

        //     Ok(builder.build(size))
        // }, {

        // });
        // unimplemented!()
    }

    fn passthrough_null(&self) -> bool {
        false
    }
}

impl std::fmt::Display for IfFunction2 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}
