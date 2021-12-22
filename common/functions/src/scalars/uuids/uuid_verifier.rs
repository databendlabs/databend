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
use std::marker::PhantomData;
use std::str;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use uuid::Uuid;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

pub type UUIDIsEmptyFunction = UUIDVerifierFunction<UUIDIsEmpty>;
pub type UUIDIsNotEmptyFunction = UUIDVerifierFunction<UUIDIsNotEmpty>;

#[derive(Clone, Debug)]
pub struct UUIDVerifierFunction<T> {
    display_name: String,
    t: PhantomData<T>,
}

impl<T> UUIDVerifierFunction<T>
where T: UUIDVerifier + Clone + Sync + Send + 'static
{
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(UUIDVerifierFunction::<T> {
            display_name: display_name.to_string(),
            t: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default())
    }
}

impl<T> fmt::Display for UUIDVerifierFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

pub trait UUIDVerifier {
    fn default_verify() -> bool;

    fn verify(uuid: Uuid) -> bool;
}

#[derive(Clone, Debug)]
pub struct UUIDIsEmpty;

impl UUIDVerifier for UUIDIsEmpty {
    fn default_verify() -> bool {
        true
    }

    fn verify(uuid: Uuid) -> bool {
        uuid.is_nil()
    }
}

#[derive(Clone, Debug)]
pub struct UUIDIsNotEmpty;

impl UUIDVerifier for UUIDIsNotEmpty {
    fn default_verify() -> bool {
        false
    }

    fn verify(uuid: Uuid) -> bool {
        !uuid.is_nil()
    }
}

impl<T> Function for UUIDVerifierFunction<T>
where T: UUIDVerifier + Clone + Sync + Send + 'static
{
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args[0] != DataType::String && args[0] != DataType::Null {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected string or null, but got {}",
                args[0]
            )));
        }

        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        match columns[0].data_type() {
            &DataType::String => {
                if let Ok(string) = str::from_utf8(
                    columns[0]
                        .column()
                        .cast_with_type(&DataType::String)?
                        .to_minimal_array()?
                        .string()?
                        .inner()
                        .values()
                        .as_slice(),
                ) {
                    if let Ok(uuid) = Uuid::parse_str(string) {
                        return Ok(DataColumn::Constant(
                            DataValue::Boolean(Some(T::verify(uuid))),
                            input_rows,
                        ));
                    }
                }

                Ok(DataColumn::Constant(
                    DataValue::Boolean(Some(T::default_verify())),
                    input_rows,
                ))
            }
            _ => Ok(DataColumn::Constant(
                DataValue::Boolean(Some(T::default_verify())),
                input_rows,
            )),
        }
    }
}
