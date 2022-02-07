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
use std::sync::Arc;

use common_datavalues2::BooleanColumn;
use common_datavalues2::BooleanType;
use common_datavalues2::ColumnViewerIter;
use common_datavalues2::ScalarColumn;
use common_datavalues2::TypeID;
use common_datavalues2::Vu8;
use common_exception::ErrorCode;
use common_exception::Result;
use uuid::Uuid;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;
use crate::scalars::Function2Description;

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
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(UUIDVerifierFunction::<T> {
            display_name: display_name.to_string(),
            t: PhantomData,
        }))
    }

    pub fn desc() -> Function2Description {
        Function2Description::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().num_arguments(1))
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

impl<T> Function2 for UUIDVerifierFunction<T>
where T: UUIDVerifier + Clone + Sync + Send + 'static
{
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(
        &self,
        args: &[&common_datavalues2::DataTypePtr],
    ) -> Result<common_datavalues2::DataTypePtr> {
        if args[0].data_type_id() != TypeID::String && args[0].data_type_id() != TypeID::Null {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected string or null, but got {:?}",
                args[0]
            )));
        }

        Ok(BooleanType::arc())
    }

    fn passthrough_null(&self) -> bool {
        false
    }

    fn eval(
        &self,
        columns: &common_datavalues2::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues2::ColumnRef> {
        let result_column = if columns[0].data_type().data_type_id() == TypeID::String {
            let iter = ColumnViewerIter::<Vu8>::try_create(columns[0].column())?;
            BooleanColumn::from_iterator(iter.map(|uuid_bytes| {
                if let Ok(uuid_str) = str::from_utf8(uuid_bytes) {
                    if let Ok(uuid) = Uuid::parse_str(uuid_str) {
                        T::verify(uuid)
                    } else {
                        T::default_verify()
                    }
                } else {
                    T::default_verify()
                }
            }))
        } else {
            BooleanColumn::from_slice(&[T::default_verify()])
        };

        Ok(Arc::new(result_column))
    }
}
