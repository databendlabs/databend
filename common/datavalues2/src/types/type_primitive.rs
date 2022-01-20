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

use super::data_type::DataType;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Debug, Default, Clone, Copy, serde::Deserialize, serde::Serialize)]

pub struct PrimitiveDataType<
    T: PrimitiveType + Clone + Copy + std::fmt::Debug + Into<DataValue> + serde::Serialize,
> {
    _t: PhantomData<T>,
}

// typetag did not support generic impls, so we have to do this
pub fn create_primitive_datatype<T: PrimitiveType>() -> Arc<dyn DataType> {
    match (T::SIGN, T::FLOATING, T::SIZE) {
        (false, false, 1) => Arc::new(UInt8Type { _t: PhantomData }),
        (false, false, 2) => Arc::new(UInt16Type { _t: PhantomData }),
        (false, false, 4) => Arc::new(UInt32Type { _t: PhantomData }),
        (false, false, 8) => Arc::new(UInt64Type { _t: PhantomData }),

        (true, false, 1) => Arc::new(Int8Type { _t: PhantomData }),
        (true, false, 2) => Arc::new(Int16Type { _t: PhantomData }),
        (true, false, 4) => Arc::new(Int32Type { _t: PhantomData }),
        (true, false, 8) => Arc::new(Int64Type { _t: PhantomData }),

        (true, true, 4) => Arc::new(Float32Type { _t: PhantomData }),
        (true, true, 8) => Arc::new(Float64Type { _t: PhantomData }),

        _ => unimplemented!(),
    }
}

pub type Int8Type = PrimitiveDataType<i8>;
pub type Int16Type = PrimitiveDataType<i16>;
pub type Int32Type = PrimitiveDataType<i32>;
pub type Int64Type = PrimitiveDataType<i64>;
pub type UInt8Type = PrimitiveDataType<u8>;
pub type UInt16Type = PrimitiveDataType<u16>;
pub type UInt32Type = PrimitiveDataType<u32>;
pub type UInt64Type = PrimitiveDataType<u64>;
pub type Float32Type = PrimitiveDataType<f32>;
pub type Float64Type = PrimitiveDataType<f64>;

macro_rules! impl_numeric {
    ($ty:ident, $tname:ident, $name: expr, $alias: expr) => {
        impl PrimitiveDataType<$ty> {
            pub fn arc() -> DataTypePtr {
                Arc::new(Self { _t: PhantomData })
            }
        }

        #[typetag::serde]
        impl DataType for PrimitiveDataType<$ty> {
            fn data_type_id(&self) -> TypeID {
                TypeID::$tname
            }

            #[inline]
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> &str {
                $name
            }

            fn alias(&self) -> &[&str] {
                $alias
            }

            fn default_value(&self) -> DataValue {
                $ty::default().into()
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

            fn create_serializer(&self) -> Box<dyn TypeSerializer> {
                Box::new(NumberSerializer::<$ty>::default())
            }

            fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer> {
                Box::new(NumberDeserializer::<$ty> {
                    builder: MutablePrimitiveColumn::<$ty>::with_capacity(capacity),
                })
            }
        }
    };
}
//
impl_numeric!(u8, UInt8, "UInt8", &[]);
impl_numeric!(u16, UInt16, "UInt16", &[]);
impl_numeric!(u32, UInt32, "UInt32", &[]);
impl_numeric!(u64, UInt64, "UInt64", &[]);

impl_numeric!(i8, Int8, "Int8", &["tinyint"]);
impl_numeric!(i16, Int16, "Int16", &["smallint"]);
impl_numeric!(i32, Int32, "Int32", &["int"]);
impl_numeric!(i64, Int64, "Int64", &["bigint"]);

impl_numeric!(f32, Float32, "Float32", &["float"]);
impl_numeric!(f64, Float64, "Float64", &["double"]);
