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

use std::sync::Arc;

use super::type_array::ArrayType;
use super::type_nullable::NullableType;
use super::type_struct::StructType;
use super::DataType;

impl Eq for dyn DataType + '_ {}

impl PartialEq for dyn DataType + '_ {
    fn eq(&self, that: &dyn DataType) -> bool {
        equal(self, that)
    }
}

impl PartialEq<dyn DataType> for Arc<dyn DataType + '_> {
    fn eq(&self, that: &dyn DataType) -> bool {
        equal(&**self, that)
    }
}

impl PartialEq<dyn DataType> for Box<dyn DataType + '_> {
    fn eq(&self, that: &dyn DataType) -> bool {
        equal(&**self, that)
    }
}

pub fn equal(lhs: &dyn DataType, rhs: &dyn DataType) -> bool {
    if lhs.data_type_id() != rhs.data_type_id() {
        return false;
    }

    use crate::prelude::TypeID::*;
    match lhs.data_type_id() {
        Boolean | UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64 | Float32
        | Float64 | String | Date16 | Date32 | Interval | DateTime32 | DateTime64 | Null => true,

        Nullable => {
            let lhs: &NullableType = lhs.as_any().downcast_ref().unwrap();
            let rhs: &NullableType = rhs.as_any().downcast_ref().unwrap();

            *lhs.inner_type() == *rhs.inner_type()
        }

        Array => {
            let lhs: &ArrayType = lhs.as_any().downcast_ref().unwrap();
            let rhs: &ArrayType = rhs.as_any().downcast_ref().unwrap();

            *lhs.inner_type() == *rhs.inner_type()
        }

        Struct => {
            let lhs: &StructType = lhs.as_any().downcast_ref().unwrap();
            let rhs: &StructType = rhs.as_any().downcast_ref().unwrap();

            *lhs.types() == *rhs.types() && *lhs.names() == *rhs.names()
        }
    }
}
