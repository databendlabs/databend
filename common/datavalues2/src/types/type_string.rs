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

use super::data_type::DataType;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct StringType {}

impl StringType {
    pub fn arc() -> DataTypePtr {
        Arc::new(Self {})
    }
}

#[typetag::serde]
impl DataType for StringType {
    fn data_type_id(&self) -> TypeID {
        TypeID::String
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "String"
    }

    fn aliases(&self) -> &[&str] {
        &["Binary", "Varchar", "Char", "Text", "Blob"]
    }

    fn default_value(&self) -> DataValue {
        DataValue::String(vec![])
    }

    fn create_constant_column(
        &self,
        data: &DataValue,
        size: usize,
    ) -> common_exception::Result<ColumnRef> {
        let value = data.as_string()?;
        let bytes = value.as_slice();

        let column = Series::from_data(&[bytes]);
        Ok(Arc::new(ConstColumn::new(column, size)))
    }

    fn arrow_type(&self) -> ArrowType {
        ArrowType::LargeBinary
    }

    fn create_serializer(&self) -> Box<dyn TypeSerializer> {
        Box::new(StringSerializer {})
    }

    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer> {
        Box::new(StringDeserializer::with_capacity(capacity))
    }

    fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn> {
        Box::new(MutableStringColumn::with_capacity(capacity))
    }

    fn create_column(&self, data: &[DataValue]) -> common_exception::Result<ColumnRef> {
        let mut values: Vec<u8> = vec![];
        let mut offsets: Vec<i64> = vec![0];
        for v in data.iter() {
            let value = v.as_string()?;
            offsets.push(offsets.last().unwrap() + value.len() as i64);
            values.extend_from_slice(&value);
        }

        unsafe {
            Ok(Arc::new(StringColumn::from_data_unchecked(
                offsets.into(),
                values.into(),
            )))
        }
    }
}

impl std::fmt::Debug for StringType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
