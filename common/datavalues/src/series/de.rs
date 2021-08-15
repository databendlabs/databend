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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;

macro_rules! creator {
    ($T: ident, $data_type: expr, $capacity: expr) => {
        if $T::data_type() == $data_type {
            return Ok(Box::new(PrimitiveArrayBuilder::<$T>::with_capacity(
                $capacity,
            )));
        }
    };
}

impl DataType {
    pub fn create_deserializer(&self, capacity: usize) -> Result<Box<dyn ArrayDeserializer>> {
        let data_type = self.clone();
        dispatch_numeric_types! { creator, data_type, capacity}

        match self {
            DataType::Boolean => Ok(Box::new(BooleanArrayBuilder::with_capacity(capacity))),

            DataType::UInt8 => Ok(Box::new(PrimitiveArrayBuilder::<UInt8Type>::with_capacity(
                capacity,
            ))),

            DataType::Utf8 => Ok(Box::new(Utf8ArrayBuilder::with_capacity(capacity))),

            other => Err(ErrorCode::BadDataValueType(format!(
                "create_deserializer does not support type '{:?}'",
                other
            ))),
        }
    }
}
