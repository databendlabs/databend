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
use super::type_timestamp::TimestampType;
use super::DataTypeImpl;

impl Eq for DataTypeImpl {}

impl PartialEq for DataTypeImpl {
    fn eq(&self, that: &DataTypeImpl) -> bool {
        equal(self, that)
    }
}

impl PartialEq<DataTypeImpl> for Arc<DataTypeImpl> {
    fn eq(&self, that: &DataTypeImpl) -> bool {
        equal(&**self, that)
    }
}

impl PartialEq<DataTypeImpl> for Box<DataTypeImpl> {
    fn eq(&self, that: &DataTypeImpl) -> bool {
        equal(&**self, that)
    }
}

pub fn equal(lhs: &DataTypeImpl, rhs: &DataTypeImpl) -> bool {
    if lhs.data_type_id() != rhs.data_type_id() {
        return false;
    }

    use crate::prelude::TypeID::*;
    match lhs.data_type_id() {
        Boolean | UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64 | Float32
        | Float64 | String | Date | Interval | Null | Variant | VariantArray | VariantObject => {
            true
        }

        Timestamp => {
            let lhs: &TimestampType = lhs.as_any().downcast_ref().unwrap();
            let rhs: &TimestampType = rhs.as_any().downcast_ref().unwrap();

            lhs.precision() == rhs.precision()
        }

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
