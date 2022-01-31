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
use std::net::Ipv4Addr;
use std::str;
use std::sync::Arc;

use common_datavalues2::remove_nullable;
use common_datavalues2::types::type_string::StringType;
use common_datavalues2::ColumnRef;
use common_datavalues2::ColumnViewer;
use common_datavalues2::ColumnsWithField;
use common_datavalues2::DataTypePtr;
use common_datavalues2::Float64Type;
use common_datavalues2::NullableColumnBuilder;
use common_datavalues2::NullableType;
use common_datavalues2::TypeID;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::cast_with_type;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::CastOptions;
use crate::scalars::ExceptionMode;
use crate::scalars::Function2;
use crate::scalars::Function2Description;
use crate::scalars::ParsingMode;

#[derive(Clone)]
#[doc(alias = "IPv4NumToStringFunction")]
pub struct InetNtoaFunction {
    display_name: String,
}

impl InetNtoaFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(InetNtoaFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> Function2Description {
        Function2Description::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl Function2 for InetNtoaFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        let input_type = remove_nullable(args[0]);

        let output_type = if input_type.data_type_id().is_numeric()
            || input_type.data_type_id().is_string()
            || input_type.data_type_id() == TypeID::Null
        {
            Ok(StringType::arc())
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "Expected numeric, string or null type, but got {}",
                args[0].name()
            )))
        }?;

        // For invalid input, the function should return null. So the return type must be nullable.
        Ok(Arc::new(NullableType::create(output_type)))
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let cast_to: DataTypePtr = Arc::new(NullableType::create(Float64Type::arc()));
        let cast_option = CastOptions {
            // we allow cast failure
            exception_mode: ExceptionMode::Zero,
            parsing_mode: ParsingMode::Partial,
        };
        let column = cast_with_type(
            columns[0].column(),
            columns[0].data_type(),
            &cast_to,
            &cast_option,
        )?;
        let viewer = ColumnViewer::<f64>::create(&column)?;

        let mut builder: NullableColumnBuilder<Vec<u8>> =
            NullableColumnBuilder::with_capacity(input_rows);

        for i in 0..input_rows {
            let val = viewer.value(i);

            if val.is_nan() || val < 0.0 || val > u32::MAX as f64 {
                builder.append_null();
            } else {
                let addr_str = Ipv4Addr::from((val as u32).to_be_bytes()).to_string();
                builder.append(addr_str.as_bytes(), viewer.valid_at(i));
            }
        }
        Ok(builder.build(input_rows))
    }
}

impl fmt::Display for InetNtoaFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
