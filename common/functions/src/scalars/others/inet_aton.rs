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

use std::fmt;
use std::str;
use std::sync::Arc;

use common_datavalues2::remove_nullable;
use common_datavalues2::type_primitive;
use common_datavalues2::ColumnRef;
use common_datavalues2::ColumnViewer;
use common_datavalues2::ColumnsWithField;
use common_datavalues2::DataTypePtr;
use common_datavalues2::NullableColumnBuilder;
use common_datavalues2::NullableType;
use common_datavalues2::TypeID;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;
use crate::scalars::Function2Description;

#[derive(Clone)]
#[doc(alias = "IPv4StringToNumFunction")]
pub struct InetAtonFunction {
    display_name: String,
}

impl InetAtonFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(InetAtonFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> Function2Description {
        Function2Description::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl Function2 for InetAtonFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        let input_type = remove_nullable(args[0]);

        let output_type = match input_type.data_type_id() {
            TypeID::String | TypeID::Null => Ok(type_primitive::UInt32Type::arc()),
            _ => Err(ErrorCode::IllegalDataType(format!(
                "Expected string or null type, but got {}",
                args[0].name()
            ))),
        }?;

        // For invalid input, the function should return null. So the return type must be nullable.
        Ok(Arc::new(NullableType::create(output_type)))
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let mut builder: NullableColumnBuilder<u32> =
            NullableColumnBuilder::with_capacity(input_rows);
        let viewer = ColumnViewer::<Vec<u8>>::create(columns[0].column())?;

        for i in 0..input_rows {
            // We skip the null check because the function has passthrough_null is true.
            // This is arguably correct because the address parsing is not optimized by SIMD, not quite sure how much we can gain from skipping branch prediction.
            // Think about the case if we have 1000 rows and 999 are Nulls.
            let input = viewer.value(i);
            let parsed_addr = String::from_utf8_lossy(input).parse::<std::net::Ipv4Addr>();

            match parsed_addr {
                Ok(addr) => {
                    let addr_binary: u32 = u32::from(addr);
                    builder.append(addr_binary, viewer.valid_at(i));
                }
                Err(_) => builder.append_null(),
            }
        }
        Ok(builder.build(input_rows))
    }
}

impl fmt::Display for InetAtonFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
