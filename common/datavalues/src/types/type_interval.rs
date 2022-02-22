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
use std::fmt;
use std::sync::Arc;

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_exception::Result;

use super::data_type::DataType;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub struct IntervalType {
    kind: IntervalKind,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum IntervalKind {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl fmt::Display for IntervalKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            IntervalKind::Year => "YEAR",
            IntervalKind::Month => "MONTH",
            IntervalKind::Day => "DAY",
            IntervalKind::Hour => "HOUR",
            IntervalKind::Minute => "MINUTE",
            IntervalKind::Second => "SECOND",
        })
    }
}

impl From<String> for IntervalKind {
    fn from(s: String) -> Self {
        match s.as_str() {
            "YEAR" => IntervalKind::Year,
            "MONTH" => IntervalKind::Month,
            "DAY" => IntervalKind::Day,
            "HOUR" => IntervalKind::Hour,
            "MINUTE" => IntervalKind::Minute,
            "SECOND" => IntervalKind::Second,
            _ => unreachable!(),
        }
    }
}

impl IntervalType {
    pub fn arc(kind: IntervalKind) -> DataTypePtr {
        Arc::new(Self { kind })
    }

    pub fn kind(&self) -> &IntervalKind {
        &self.kind
    }
}

#[typetag::serde]
impl DataType for IntervalType {
    fn data_type_id(&self) -> TypeID {
        TypeID::Interval
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "Interval"
    }

    fn default_value(&self) -> DataValue {
        DataValue::Int64(0)
    }

    fn create_constant_column(&self, data: &DataValue, size: usize) -> Result<ColumnRef> {
        let value = data.as_i64()?;

        let column = Series::from_data(&[value]);
        Ok(Arc::new(ConstColumn::new(column, size)))
    }

    fn create_column(&self, data: &[DataValue]) -> Result<ColumnRef> {
        let value = data
            .iter()
            .map(|v| v.as_i64())
            .collect::<Result<Vec<_>>>()?;
        Ok(Series::from_data(&value))
    }

    fn arrow_type(&self) -> ArrowType {
        ArrowType::Int64
    }

    fn custom_arrow_meta(&self) -> Option<BTreeMap<String, String>> {
        let mut mp = BTreeMap::new();
        mp.insert(ARROW_EXTENSION_NAME.to_string(), "Interval".to_string());
        mp.insert(ARROW_EXTENSION_META.to_string(), format!("{}", self.kind));
        Some(mp)
    }

    fn create_serializer(&self) -> Box<dyn TypeSerializer> {
        Box::new(DateSerializer::<i64>::default())
    }

    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer> {
        Box::new(DateDeserializer::<i64> {
            builder: MutablePrimitiveColumn::<i64>::with_capacity(capacity),
        })
    }

    fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn> {
        Box::new(MutablePrimitiveColumn::<i64>::with_capacity(capacity))
    }
}

impl std::fmt::Debug for IntervalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({:?})", self.name(), self.kind)
    }
}
