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
use std::net::Ipv4Addr;
use std::str;
use std::sync::Arc;

use common_datavalues2::remove_nullable;
use common_datavalues2::type_primitive;
use common_datavalues2::ColumnBuilder;
use common_datavalues2::ColumnRef;
use common_datavalues2::ColumnViewer;
use common_datavalues2::ColumnsWithField;
use common_datavalues2::DataTypePtr;
use common_datavalues2::DataValue;
use common_datavalues2::NullType;
use common_datavalues2::NullableColumnBuilder;
use common_datavalues2::NullableType;
use common_datavalues2::TypeID;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;
use crate::scalars::Function2Description;

#[doc(alias = "TryIPv4StringToNumFunction")]
pub type TryInetAtonFunction = InetAtonFunctionImpl<true>;

#[doc(alias = "IPv4StringToNumFunction")]
pub type InetAtonFunction = InetAtonFunctionImpl<false>;

#[derive(Clone)]
pub struct InetAtonFunctionImpl<const SUPPRESS_PARSE_ERROR: bool> {
    display_name: String,
}

impl<const SUPPRESS_PARSE_ERROR: bool> InetAtonFunctionImpl<SUPPRESS_PARSE_ERROR> {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(InetAtonFunctionImpl::<SUPPRESS_PARSE_ERROR> {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> Function2Description {
        Function2Description::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl<const SUPPRESS_PARSE_ERROR: bool> Function2 for InetAtonFunctionImpl<SUPPRESS_PARSE_ERROR> {
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

        if SUPPRESS_PARSE_ERROR {
            // For invalid input, we suppress parse error and return null. So the return type must be nullable.
            return Ok(Arc::new(NullableType::create(output_type)));
        }

        if args[0].is_nullable() {
            Ok(Arc::new(NullableType::create(output_type)))
        } else {
            Ok(output_type)
        }
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        if SUPPRESS_PARSE_ERROR {
            let viewer = ColumnViewer::<Vec<u8>>::create(columns[0].column())?;

            let mut builder = NullableColumnBuilder::<u32>::with_capacity(input_rows);

            for i in 0..input_rows {
                // We skip the null check because the function has passthrough_null is true.
                // This is arguably correct because the address parsing is not optimized by SIMD, not quite sure how much we can gain from skipping branch prediction.
                // Think about the case if we have 1000 rows and 999 are Nulls.
                let input = viewer.value(i);
                let addr_str = String::from_utf8_lossy(input);
                match addr_str.parse::<Ipv4Addr>() {
                    Ok(addr) => {
                        let addr_binary: u32 = u32::from(addr);
                        builder.append(addr_binary, viewer.valid_at(i));
                    }
                    Err(_) => builder.append_null(),
                }
            }
            return Ok(builder.build(input_rows));
        }

        if columns[0].column().data_type_id() == TypeID::Null {
            return NullType::arc().create_constant_column(&DataValue::Null, input_rows);
        }

        let viewer = ColumnViewer::<Vec<u8>>::create(columns[0].column())?;

        if columns[0].column().is_nullable() || columns[0].column().data_type_id() == TypeID::Null {
            let mut builder = NullableColumnBuilder::<u32>::with_capacity(input_rows);
            for i in 0..input_rows {
                if viewer.null_at(i) {
                    builder.append_null();
                    continue;
                }

                let input = viewer.value(i);
                let addr_str = String::from_utf8_lossy(input);
                match addr_str.parse::<Ipv4Addr>() {
                    Ok(addr) => {
                        let addr_binary: u32 = u32::from(addr);
                        builder.append(addr_binary, viewer.valid_at(i));
                    }
                    Err(err) => {
                        return Err(ErrorCode::StrParseError(format!(
                            "Failed to parse '{}' into a IPV4 address, {}",
                            addr_str, err
                        )));
                    }
                }
            }
            Ok(builder.build(input_rows))
        } else {
            let mut builder = ColumnBuilder::<u32>::with_capacity(input_rows);
            for i in 0..input_rows {
                let input = viewer.value(i);
                let addr_str = String::from_utf8_lossy(input);
                match addr_str.parse::<Ipv4Addr>() {
                    Ok(addr) => {
                        let addr_binary: u32 = u32::from(addr);
                        builder.append(addr_binary);
                    }
                    Err(err) => {
                        return Err(ErrorCode::StrParseError(format!(
                            "Failed to parse '{}' into a IPV4 address, {}",
                            addr_str, err
                        )));
                    }
                }
            }
            Ok(builder.build(input_rows))
        }
    }

    fn passthrough_null(&self) -> bool {
        // Null will cause parse error when SUPPRESS_PARSE_ERROR is false.
        // In this case we need to check null and skip the parsing, so passthrough_null should be false.
        SUPPRESS_PARSE_ERROR
    }
}

impl<const SUPPRESS_PARSE_ERROR: bool> fmt::Display for InetAtonFunctionImpl<SUPPRESS_PARSE_ERROR> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
