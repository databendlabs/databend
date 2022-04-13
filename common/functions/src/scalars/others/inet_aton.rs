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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::assert_string;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[doc(alias = "TryIPv4StringToNumFunction")]
pub type TryInetAtonFunction = InetAtonFunctionImpl<true>;

#[doc(alias = "IPv4StringToNumFunction")]
pub type InetAtonFunction = InetAtonFunctionImpl<false>;

#[derive(Clone)]
pub struct InetAtonFunctionImpl<const SUPPRESS_PARSE_ERROR: bool> {
    display_name: String,
}

impl<const SUPPRESS_PARSE_ERROR: bool> InetAtonFunctionImpl<SUPPRESS_PARSE_ERROR> {
    pub fn try_create(
        display_name: &str,
        args: &[&common_datavalues::DataTypePtr],
    ) -> Result<Box<dyn Function>> {
        assert_string(args[0])?;

        Ok(Box::new(InetAtonFunctionImpl::<SUPPRESS_PARSE_ERROR> {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl<const SUPPRESS_PARSE_ERROR: bool> Function for InetAtonFunctionImpl<SUPPRESS_PARSE_ERROR> {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        if SUPPRESS_PARSE_ERROR {
            Ok(NullableType::arc(UInt32Type::arc()))
        } else {
            Ok(UInt32Type::arc())
        }
    }

    fn eval(
        &self,
        columns: &ColumnsWithField,
        input_rows: usize,
        _func_ctx: FunctionContext,
    ) -> Result<ColumnRef> {
        let viewer = Vu8::try_create_viewer(columns[0].column())?;
        let viewer_iter = viewer.iter();

        if SUPPRESS_PARSE_ERROR {
            let mut builder = NullableColumnBuilder::<u32>::with_capacity(input_rows);

            for (i, input) in viewer_iter.enumerate() {
                // This is arguably correct because the address parsing is not optimized by SIMD, not quite sure how much we can gain from skipping branch prediction.
                // Think about the case if we have 1000 rows and 999 are Nulls.
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

        // We skip the null check because the function has passthrough_null is true.
        let mut builder = ColumnBuilder::<u32>::with_capacity(input_rows);
        for input in viewer_iter {
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

impl<const SUPPRESS_PARSE_ERROR: bool> fmt::Display for InetAtonFunctionImpl<SUPPRESS_PARSE_ERROR> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
