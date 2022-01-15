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

use crate::prelude::*;
use crate::ArrayColumn;
use crate::BooleanColumn;
use crate::NullableColumn;
use crate::StringColumn;
use crate::StructColumn;

impl Eq for dyn Column + '_ {}

impl PartialEq for dyn Column + '_ {
    fn eq(&self, that: &dyn Column) -> bool {
        equal(self, that)
    }
}

impl PartialEq<dyn Column> for Arc<dyn Column + '_> {
    fn eq(&self, that: &dyn Column) -> bool {
        equal(&**self, that)
    }
}

impl PartialEq<dyn Column> for Box<dyn Column + '_> {
    fn eq(&self, that: &dyn Column) -> bool {
        equal(&**self, that)
    }
}

pub fn equal(lhs: &dyn Column, rhs: &dyn Column) -> bool {
    if lhs.data_type() != rhs.data_type() || lhs.len() != lhs.len() {
        return false;
    }

    use crate::PhysicalTypeID::*;

    match lhs.data_type_id().to_physical_type() {
        Null => true,
        Nullable => {
            let lhs: &NullableColumn = lhs.as_any().downcast_ref().unwrap();
            let rhs: &NullableColumn = rhs.as_any().downcast_ref().unwrap();

            lhs.validity() == rhs.validity() && lhs.inner() == rhs.inner()
        }
        Boolean => {
            let lhs: &BooleanColumn = lhs.as_any().downcast_ref().unwrap();
            let rhs: &BooleanColumn = rhs.as_any().downcast_ref().unwrap();

            lhs.values() == rhs.values()
        }
        String => {
            let lhs: &StringColumn = lhs.as_any().downcast_ref().unwrap();
            let rhs: &StringColumn = rhs.as_any().downcast_ref().unwrap();

            lhs.values() == rhs.values() && lhs.offsets() == rhs.offsets()
        }
        Primitive(e) => with_match_physical_primitive_type!(e, |$T| {
            let lhs: &PrimitiveColumn<$T> = lhs.as_any().downcast_ref().unwrap();
            let rhs: &PrimitiveColumn<$T> = rhs.as_any().downcast_ref().unwrap();

            lhs.values() == rhs.values()
        }),
        Array => {
            let lhs: &ArrayColumn = lhs.as_any().downcast_ref().unwrap();
            let rhs: &ArrayColumn = rhs.as_any().downcast_ref().unwrap();

            lhs.values() == rhs.values() && lhs.offsets() == rhs.offsets()
        }
        Struct => {
            let lhs: &StructColumn = lhs.as_any().downcast_ref().unwrap();
            let rhs: &StructColumn = rhs.as_any().downcast_ref().unwrap();

            lhs.values() == rhs.values()
        }
    }
}
