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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_exception::ErrorCode;

use super::data_type::DataTypePtr;
use super::data_type::IDataType;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DataTypeNullable {
    inner: DataTypePtr,
}

impl DataTypeNullable {
    pub fn create(inner: DataTypePtr) -> Self {
        DataTypeNullable { inner }
    }

    pub fn create_null() -> Self {
        let inner = Arc::new(DataTypeNull {});
        DataTypeNullable { inner }
    }

    pub fn inner_type(&self) -> &DataTypePtr {
        &self.inner
    }
}

#[typetag::serde]
impl IDataType for DataTypeNullable {
    fn data_type_id(&self) -> TypeID {
        TypeID::Nullable
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn default_value(&self) -> DataValue {
        DataValue::Null
    }

    fn arrow_type(&self) -> ArrowType {
        self.inner.arrow_type()
    }

    fn create_serializer(&self) -> Box<dyn TypeSerializer> {
        Box::new(NullableSerializer {
            inner: self.inner.create_serializer(),
        })
    }

    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer> {
        Box::new(NullableDeserializer {
            inner: self.inner.create_deserializer(capacity),
            bitmap: MutableBitmap::with_capacity(capacity),
        })
    }

    fn create_constant_column(
        &self,
        data: &DataValue,
        size: usize,
    ) -> common_exception::Result<ColumnRef> {
        if self.inner.data_type_id() == TypeID::Null {
            return Ok(Arc::new(NullColumn::new(size)));
        }

        if self.inner.data_type_id() == TypeID::Null {
            return Result::Err(ErrorCode::BadDataValueType(format!(
                "Nullable type can't be inside nullable type",
            )));
        }
        self.inner.create_constant_column(data, size)
    }

    fn create_column(&self, data: &[DataValue]) -> common_exception::Result<ColumnRef> {
        let column = self.inner.create_column(data)?;
        let mut bitmap = MutableBitmap::with_capacity(data.len());
        bitmap.extend_constant(data.len(), true);
        Ok(Arc::new(NullableColumn::new(column, bitmap.into())))
    }
}
