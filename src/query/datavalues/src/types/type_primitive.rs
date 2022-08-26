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

use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_exception::Result;
use rand::prelude::*;

use super::data_type::DataType;
use super::type_id::TypeID;
use crate::prelude::*;
use crate::serializations::NumberSerializer;
use crate::serializations::TypeSerializerImpl;

#[derive(Default, Clone, Copy, serde::Deserialize, serde::Serialize)]

pub struct PrimitiveDataType<
    T: PrimitiveType + Clone + Copy + std::fmt::Debug + Into<DataValue> + serde::Serialize,
> {
    #[serde(skip)]
    _t: PhantomData<T>,
}

// typetag did not support generic impls, so we have to do this
pub fn create_primitive_datatype<T: PrimitiveType>() -> DataTypeImpl {
    match (T::SIGN, T::FLOATING, T::SIZE) {
        (false, false, 1) => DataTypeImpl::UInt8(UInt8Type { _t: PhantomData }),
        (false, false, 2) => DataTypeImpl::UInt16(UInt16Type { _t: PhantomData }),
        (false, false, 4) => DataTypeImpl::UInt32(UInt32Type { _t: PhantomData }),
        (false, false, 8) => DataTypeImpl::UInt64(UInt64Type { _t: PhantomData }),

        (true, false, 1) => DataTypeImpl::Int8(Int8Type { _t: PhantomData }),
        (true, false, 2) => DataTypeImpl::Int16(Int16Type { _t: PhantomData }),
        (true, false, 4) => DataTypeImpl::Int32(Int32Type { _t: PhantomData }),
        (true, false, 8) => DataTypeImpl::Int64(Int64Type { _t: PhantomData }),

        (true, true, 4) => DataTypeImpl::Float32(Float32Type { _t: PhantomData }),
        (true, true, 8) => DataTypeImpl::Float64(Float64Type { _t: PhantomData }),

        _ => unimplemented!(),
    }
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

            #[inline]
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> String {
                $name.to_string()
            }

            fn sql_name(&self) -> String {
                $sql_name.to_uppercase()
            }

            fn aliases(&self) -> &[&str] {
                $alias
            }

            fn default_value(&self) -> DataValue {
                $ty::default().into()
            }

            fn random_value(&self) -> DataValue {
                let mut rng = rand::rngs::SmallRng::from_entropy();
                rng.gen::<$ty>().into()
            }

            fn create_constant_column(&self, data: &DataValue, size: usize) -> Result<ColumnRef> {
                let value: $ty = DFTryFrom::try_from(data)?;
                let column = Series::from_data(&[value]);
                Ok(Arc::new(ConstColumn::new(column, size)))
            }

            fn create_column(&self, data: &[DataValue]) -> Result<ColumnRef> {
                let value: Vec<$ty> = data
                    .iter()
                    .map(|v| DFTryFrom::try_from(v))
                    .collect::<Result<Vec<_>>>()?;

                Ok(Series::from_data(&value))
            }

            fn arrow_type(&self) -> ArrowType {
                ArrowType::$tname
            }

            fn create_serializer_inner<'a>(
                &self,
                col: &'a ColumnRef,
            ) -> Result<TypeSerializerImpl<'a>> {
                Ok(NumberSerializer::<'a, $ty>::try_create(col)?.into())
            }

            fn create_deserializer(&self, capacity: usize) -> TypeDeserializerImpl {
                NumberDeserializer::<$ty> {
                    builder: MutablePrimitiveColumn::<$ty>::with_capacity(capacity),
                }
                .into()
            }

            fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn> {
                Box::new(MutablePrimitiveColumn::<$ty>::with_capacity(capacity))
            }
        }

        paste::paste! {
            pub type [<$tname Type>] = PrimitiveDataType<$ty>;
        }

        impl std::fmt::Debug for PrimitiveDataType<$ty> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
