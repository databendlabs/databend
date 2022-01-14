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

use super::data_type_list::DataTypeList;
use super::data_type_nullable::DataTypeNullable;
use super::data_type_struct::DataTypeStruct;
use super::IDataType;


impl Eq for dyn IDataType + '_ {
}


impl PartialEq for dyn IDataType + '_ {
    fn eq(&self, that: &dyn IDataType) -> bool {
        equal(self, that)
    }
}

impl PartialEq<dyn IDataType> for Arc<dyn IDataType + '_> {
    fn eq(&self, that: &dyn IDataType) -> bool {
        equal(&**self, that)
    }
}

impl PartialEq<dyn IDataType> for Box<dyn IDataType + '_> {
    fn eq(&self, that: &dyn IDataType) -> bool {
        equal(&**self, that)
    }
}

pub fn equal(lhs: &dyn IDataType, rhs: &dyn IDataType) -> bool {
    if lhs.data_type_id() != rhs.data_type_id() {
        return false;
    }

    use crate::prelude::TypeID::*;
    match lhs.data_type_id() {
        Boolean | UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64
        | Float32 | Float64 | String | Date16 | Date32 | Interval => true,

        Null | Nullable => {
            let lhs: &DataTypeNullable = lhs.as_any().downcast_ref().unwrap();
            let rhs: &DataTypeNullable = rhs.as_any().downcast_ref().unwrap();

            *lhs.inner_type() == *rhs.inner_type()
        }

        DateTime32 => todo!(),
        DateTime64 => todo!(),

        List => {
            let lhs: &DataTypeList = lhs.as_any().downcast_ref().unwrap();
            let rhs: &DataTypeList = rhs.as_any().downcast_ref().unwrap();

            *lhs.inner_type() == *rhs.inner_type()
        }

        Struct => {
            let lhs: &DataTypeStruct = lhs.as_any().downcast_ref().unwrap();
            let rhs: &DataTypeStruct = rhs.as_any().downcast_ref().unwrap();

            *lhs.types() == *rhs.types() && *lhs.names() == *rhs.names()
        }
    }
}
