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

use std::marker::PhantomData;

use super::data_type::DataType;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Default, Clone, Copy, serde::Deserialize, serde::Serialize)]

pub struct PrimitiveDataType<T: Clone + Copy + std::fmt::Debug + serde::Serialize> {
    #[serde(skip)]
    _t: PhantomData<T>,
}

macro_rules! impl_numeric {
    ($ty:ident, $tname:ident, $name: expr, $sql_name:expr, $alias: expr) => {
        impl PrimitiveDataType<$ty> {
            pub fn new_impl() -> DataTypeImpl {
                DataTypeImpl::$tname(Self { _t: PhantomData })
            }

            pub fn new() -> Self {
                Self { _t: PhantomData }
            }
        }

        impl DataType for PrimitiveDataType<$ty> {
            fn data_type_id(&self) -> TypeID {
                TypeID::$tname
            }

            fn name(&self) -> String {
                $name.to_string()
            }
        }

        impl std::fmt::Debug for PrimitiveDataType<$ty> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "{}", self.name())
            }
        }

        impl std::hash::Hash for PrimitiveDataType<$ty> {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.data_type_id().hash(state);
            }
        }
    };
}
//
impl_numeric!(u8, UInt8, "UInt8", "tinyint unsigned", &["u8"]);
impl_numeric!(u16, UInt16, "UInt16", "smallint unsigned", &["u16"]);
impl_numeric!(u32, UInt32, "UInt32", "int unsigned", &["u32"]);
impl_numeric!(u64, UInt64, "UInt64", "bigint unsigned", &["u64"]);

impl_numeric!(i8, Int8, "Int8", "tinyint", &["tinyint", "i8"]);
impl_numeric!(i16, Int16, "Int16", "smallint", &["smallint", "i16"]);
impl_numeric!(i32, Int32, "Int32", "int", &["int", "i32"]);
impl_numeric!(i64, Int64, "Int64", "bigint", &["bigint", "i64"]);

impl_numeric!(f32, Float32, "Float32", "float", &["float", "f32"]);
impl_numeric!(f64, Float64, "Float64", "double", &["double", "f64"]);
