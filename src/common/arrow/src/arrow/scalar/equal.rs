// Copyright 2020-2022 Jorge C. Leitão
// Copyright 2021 Datafuse Labs
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

use super::*;
use crate::arrow::datatypes::PhysicalType;

impl PartialEq for dyn Scalar + '_ {
    fn eq(&self, that: &dyn Scalar) -> bool {
        equal(self, that)
    }
}

impl PartialEq<dyn Scalar> for Arc<dyn Scalar + '_> {
    fn eq(&self, that: &dyn Scalar) -> bool {
        equal(&**self, that)
    }
}

impl PartialEq<dyn Scalar> for Box<dyn Scalar + '_> {
    fn eq(&self, that: &dyn Scalar) -> bool {
        equal(&**self, that)
    }
}

macro_rules! dyn_eq {
    ($ty:ty, $lhs:expr, $rhs:expr) => {{
        let lhs = $lhs.as_any().downcast_ref::<$ty>().unwrap();
        let rhs = $rhs.as_any().downcast_ref::<$ty>().unwrap();
        lhs == rhs
    }};
}

fn equal(lhs: &dyn Scalar, rhs: &dyn Scalar) -> bool {
    if lhs.data_type() != rhs.data_type() {
        return false;
    }

    use PhysicalType::*;
    match lhs.data_type().to_physical_type() {
        Null => dyn_eq!(NullScalar, lhs, rhs),
        Boolean => dyn_eq!(BooleanScalar, lhs, rhs),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            dyn_eq!(PrimitiveScalar<$T>, lhs, rhs)
        }),
        Utf8 => dyn_eq!(Utf8Scalar<i32>, lhs, rhs),
        LargeUtf8 => dyn_eq!(Utf8Scalar<i64>, lhs, rhs),
        Binary => dyn_eq!(BinaryScalar<i32>, lhs, rhs),
        LargeBinary => dyn_eq!(BinaryScalar<i64>, lhs, rhs),
        List => dyn_eq!(ListScalar<i32>, lhs, rhs),
        LargeList => dyn_eq!(ListScalar<i64>, lhs, rhs),
        Dictionary(key_type) => match_integer_type!(key_type, |$T| {
            dyn_eq!(DictionaryScalar<$T>, lhs, rhs)
        }),
        Struct => dyn_eq!(StructScalar, lhs, rhs),
        FixedSizeBinary => dyn_eq!(FixedSizeBinaryScalar, lhs, rhs),
        FixedSizeList => dyn_eq!(FixedSizeListScalar, lhs, rhs),
        Union => dyn_eq!(UnionScalar, lhs, rhs),
        Map => dyn_eq!(MapScalar, lhs, rhs),
    }
}
