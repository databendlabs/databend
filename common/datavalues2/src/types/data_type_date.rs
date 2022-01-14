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

use common_arrow::arrow::datatypes::DataType as ArrowType;

use super::data_type::IDataType;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct DataTypeDate {}

// struct ColumnView<T: RuntimeType> ;

// pub fn values() -> &[T::NativeType];

// trait RuntimeType {
//   type NativeType;
//   type ColumnType;
// }

// impl RuntimeType for DataTypeDate {
//     type NativeType = u16;
//     type ColumnType = PrimitiveColumn<u16>;
// }

#[typetag::serde]
impl IDataType for DataTypeDate {
    fn data_type_id(&self) -> TypeID {
        TypeID::Date16
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn default_value(&self) -> DataValue {
        DataValue::UInt64(0)
    }

    fn create_constant_column(
        &self,
        data: &DataValue,
        size: usize,
    ) -> common_exception::Result<ColumnRef> {
        let value = data.as_u64()?;

        let column = Series::new(&[value]);
        Ok(Arc::new(ConstColumn::new(column, size)))
    }

    fn arrow_type(&self) -> ArrowType {
        ArrowType::UInt16
    }

    fn create_serializer(&self) -> Box<dyn TypeSerializer> {
        Box::new(DateSerializer::<u16>::default())
    }
    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer> {
        Box::new(DateDeserializer::<u16> {
            builder: MutablePrimitiveColumn::<u16>::with_capacity(capacity),
        })
    }
}
