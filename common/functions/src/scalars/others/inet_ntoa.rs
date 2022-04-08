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

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::assert_numeric;
use crate::scalars::cast_with_type;
use crate::scalars::CastOptions;
use crate::scalars::ExceptionMode;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionFeatures;
use crate::scalars::ParsingMode;
use crate::scalars::TypedFunctionDescription;

#[doc(alias = "TryNumToIPv4StringFunction")]
pub type TryInetNtoaFunction = InetNtoaFunctionImpl<true>;

#[doc(alias = "TryNumToIPv4StringFunction")]
pub type InetNtoaFunction = InetNtoaFunctionImpl<false>;

#[derive(Clone)]
pub struct InetNtoaFunctionImpl<const SUPPRESS_CAST_ERROR: bool> {
    display_name: String,
    result_type: DataTypePtr,
}

impl<const SUPPRESS_CAST_ERROR: bool> InetNtoaFunctionImpl<SUPPRESS_CAST_ERROR> {
    pub fn try_create(display_name: &str, args: &[&DataTypePtr]) -> Result<Box<dyn Function>> {
        let result_type = if SUPPRESS_CAST_ERROR {
            if args[0].data_type_id() == TypeID::Null {
                NullType::arc()
            } else {
                let input_type = remove_nullable(args[0]);
                assert_numeric(&input_type)?;
                // For invalid input, the function should return null. So the return type must be nullable.
                NullableType::arc(StringType::arc())
            }
        } else {
            assert_numeric(args[0])?;
            StringType::arc()
        };

        Ok(Box::new(InetNtoaFunctionImpl::<SUPPRESS_CAST_ERROR> {
            display_name: display_name.to_string(),
            result_type,
        }))
    }

    pub fn desc() -> TypedFunctionDescription {
        let mut features = FunctionFeatures::default().deterministic().num_arguments(1);
        if SUPPRESS_CAST_ERROR {
            features = features.disable_passthrough_null()
        }
        TypedFunctionDescription::creator(Box::new(Self::try_create)).features(features)
    }
}

impl<const SUPPRESS_CAST_ERROR: bool> Function for InetNtoaFunctionImpl<SUPPRESS_CAST_ERROR> {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(self.result_type.clone())
    }

    fn eval(
        &self,
        columns: &ColumnsWithField,
        input_rows: usize,
        _func_ctx: FunctionContext,
    ) -> Result<ColumnRef> {
        if columns[0].column().data_type_id() == TypeID::Null {
            return NullType::arc().create_constant_column(&DataValue::Null, input_rows);
        }

        if SUPPRESS_CAST_ERROR {
            let cast_to: DataTypePtr = Arc::new(NullableType::create(UInt32Type::arc()));
            let cast_options = CastOptions {
                // we allow cast failure
                exception_mode: ExceptionMode::Zero,
                parsing_mode: ParsingMode::Strict,
            };
            let column = cast_with_type(
                columns[0].column(),
                columns[0].data_type(),
                &cast_to,
                &cast_options,
            )?;
            let viewer = u32::try_create_viewer(&column)?;
            let viewer_iter = viewer.iter();

            let mut builder: NullableColumnBuilder<Vec<u8>> =
                NullableColumnBuilder::with_capacity(input_rows);

            for (i, val) in viewer_iter.enumerate() {
                let addr_str = Ipv4Addr::from((val as u32).to_be_bytes()).to_string();
                builder.append(addr_str.as_bytes(), viewer.valid_at(i));
            }
            Ok(builder.build(input_rows))
        } else {
            let cast_to: DataTypePtr = UInt32Type::arc();
            let cast_options = CastOptions {
                exception_mode: ExceptionMode::Throw,
                parsing_mode: ParsingMode::Strict,
            };
            let column = cast_with_type(
                columns[0].column(),
                columns[0].data_type(),
                &cast_to,
                &cast_options,
            )?;

            let viewer = u32::try_create_viewer(&column)?;

            let mut builder = ColumnBuilder::<Vec<u8>>::with_capacity(input_rows);
            for val in viewer.iter() {
                let addr_str = Ipv4Addr::from((val).to_be_bytes()).to_string();
                builder.append(addr_str.as_bytes());
            }
            Ok(builder.build(input_rows))
        }
    }
}

impl<const SUPPRESS_CAST_ERROR: bool> fmt::Display for InetNtoaFunctionImpl<SUPPRESS_CAST_ERROR> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
