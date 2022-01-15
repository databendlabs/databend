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

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono_tz::Tz;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_exception::Result;

use super::data_type::IDataType;
use super::data_type::ARROW_EXTENSION_META;
use super::data_type::ARROW_EXTENSION_NAME;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct DataTypeDateTime {
    tz: Option<String>,
}

impl DataTypeDateTime {
    pub fn create(tz: Option<String>) -> Self {
        DataTypeDateTime { tz }
    }
    pub fn arc(tz: Option<String>) -> DataTypePtr {
        Arc::new(DataTypeDateTime { tz })
    }
}

#[typetag::serde]
impl IDataType for DataTypeDateTime {
    fn data_type_id(&self) -> TypeID {
        TypeID::DateTime32
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn default_value(&self) -> DataValue {
        DataValue::UInt64(0)
    }

    fn create_constant_column(&self, data: &DataValue, size: usize) -> Result<ColumnRef> {
        let value = data.as_u64()?;

        let column = Series::new(&[value as u32]);
        Ok(Arc::new(ConstColumn::new(column, size)))
    }

    fn create_column(&self, data: &[DataValue]) -> Result<ColumnRef> {
        let value = data
            .iter()
            .map(|v| v.as_u64())
            .collect::<Result<Vec<_>>>()?;

        let value = value.iter().map(|v| *v as u32).collect::<Vec<_>>();
        Ok(Series::new(&value))
    }

    fn arrow_type(&self) -> ArrowType {
        ArrowType::UInt32
    }

    fn custom_arrow_meta(&self) -> Option<BTreeMap<String, String>> {
        let mut mp = BTreeMap::new();
        mp.insert(ARROW_EXTENSION_NAME.to_string(), "DateTime32".to_string());
        if let Some(tz) = &self.tz {
            mp.insert(ARROW_EXTENSION_META.to_string(), tz.to_string());
        }
        Some(mp)
    }

    fn create_serializer(&self) -> Box<dyn TypeSerializer> {
        Box::new(DateTimeSerializer::<u32>::default())
    }

    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer> {
        let tz = self.tz.clone().unwrap_or_else(|| "UTC".to_string());
        Box::new(DateTimeDeserializer::<u32> {
            builder: MutablePrimitiveColumn::<u32>::with_capacity(capacity),
            tz: tz.parse::<Tz>().unwrap(),
        })
    }
}
