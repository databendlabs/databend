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
use common_arrow::arrow::datatypes::Field;
use common_exception::ErrorCode;
use common_exception::Result;

use super::data_type::DataType;
use super::data_type::DataTypeImpl;
use super::type_id::TypeID;
use crate::prelude::*;
use crate::serializations::ArraySerializer;
use crate::serializations::TypeSerializerImpl;

#[derive(Clone, Hash, serde::Deserialize, serde::Serialize)]
pub struct ArrayType {
    inner: Box<DataTypeImpl>,
}

impl ArrayType {
    pub fn new_impl(inner: DataTypeImpl) -> DataTypeImpl {
        DataTypeImpl::Array(Self::create(inner))
    }

    pub fn create(inner: DataTypeImpl) -> Self {
        ArrayType {
            inner: Box::new(inner),
        }
    }

    pub fn inner_type(&self) -> &DataTypeImpl {
        &self.inner
    }
}

impl DataType for ArrayType {
    fn data_type_id(&self) -> TypeID {
        TypeID::Array
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        format!("Array({})", self.inner.name())
    }

    fn default_value(&self) -> DataValue {
        DataValue::Array(vec![])
    }

    fn random_value(&self) -> DataValue {
        // randomly generate an array with 3 elements.
        DataValue::Array(vec![
            self.inner.random_value(),
            self.inner.random_value(),
            self.inner.random_value(),
        ])
    }

    fn create_constant_column(&self, data: &DataValue, size: usize) -> Result<ColumnRef> {
        if let DataValue::Array(value) = data {
            let inner_column = self.inner.create_column(value)?;
            let offsets = vec![0, value.len() as i64];
            let column = Arc::new(ArrayColumn::from_data(
                DataTypeImpl::Array(self.clone()),
                offsets.into(),
                inner_column,
            ));

            return Ok(Arc::new(ConstColumn::new(column, size)));
        }

        Err(ErrorCode::BadDataValueType(format!(
            "Unexpected type:{:?} to generate list column",
            data.value_type()
        )))
    }

    fn create_column(&self, data: &[DataValue]) -> Result<ColumnRef> {
        let mut values: Vec<DataValue> = vec![];
        let mut offsets: Vec<i64> = vec![0];
        for v in data.iter() {
            if let DataValue::Array(value) = v {
                offsets.push(offsets.last().unwrap() + value.len() as i64);
                values.extend_from_slice(value);
            } else {
                return Result::Err(ErrorCode::BadDataValueType(format!(
                    "Unexpected type:{:?} to generate list column",
                    v.value_type()
                )));
            }
        }

        let inner_column = self.inner.create_column(&values)?;

        Ok(Arc::new(ArrayColumn::from_data(
            DataTypeImpl::Array(self.clone()),
            offsets.into(),
            inner_column,
        )))
    }

    fn arrow_type(&self) -> ArrowType {
        let field = Field::new(
            "list".to_string(),
            self.inner.arrow_type(),
            self.inner.is_nullable(),
        );
        ArrowType::LargeList(Box::new(field))
    }

    fn create_serializer_inner<'a>(&self, col: &'a ColumnRef) -> Result<TypeSerializerImpl<'a>> {
        Ok(ArraySerializer::try_create(col, &self.inner)?.into())
    }

    fn create_deserializer(&self, capacity: usize) -> TypeDeserializerImpl {
        ArrayDeserializer {
            inner: Box::new(self.inner.create_deserializer(capacity)),
            builder: MutableArrayColumn::with_capacity_meta(capacity, ColumnMeta::Array {
                inner_type: *self.inner.clone(),
            }),
        }
        .into()
    }

    fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn> {
        Box::new(MutableArrayColumn::with_capacity_meta(
            capacity,
            ColumnMeta::Array {
                inner_type: *self.inner.clone(),
            },
        ))
    }
}

impl std::fmt::Debug for ArrayType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
