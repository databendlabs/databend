// Copyright [2021] [Jorge C Leitao]
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

use parquet_format_safe::Statistics as ParquetStatistics;

use super::Statistics;
use crate::error::Error;
use crate::error::Result;
use crate::schema::types::PhysicalType;
use crate::schema::types::PrimitiveType;
use crate::types;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimitiveStatistics<T: types::NativeType> {
    pub primitive_type: PrimitiveType,
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub min_value: Option<T>,
    pub max_value: Option<T>,
}

impl<T: types::NativeType> Statistics for PrimitiveStatistics<T> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &T::TYPE
    }

    fn null_count(&self) -> Option<i64> {
        self.null_count
    }
}

pub fn read<T: types::NativeType>(
    v: &ParquetStatistics,
    primitive_type: PrimitiveType,
) -> Result<Arc<dyn Statistics>> {
    if let Some(ref v) = v.max_value {
        if v.len() != std::mem::size_of::<T>() {
            return Err(Error::oos(
                "The max_value of statistics MUST be plain encoded",
            ));
        }
    };
    if let Some(ref v) = v.min_value {
        if v.len() != std::mem::size_of::<T>() {
            return Err(Error::oos(
                "The min_value of statistics MUST be plain encoded",
            ));
        }
    };

    Ok(Arc::new(PrimitiveStatistics::<T> {
        primitive_type,
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.as_ref().map(|x| types::decode(x)),
        min_value: v.min_value.as_ref().map(|x| types::decode(x)),
    }))
}

pub fn write<T: types::NativeType>(v: &PrimitiveStatistics<T>) -> ParquetStatistics {
    ParquetStatistics {
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.map(|x| x.to_le_bytes().as_ref().to_vec()),
        min_value: v.min_value.map(|x| x.to_le_bytes().as_ref().to_vec()),
        min: None,
        max: None,
    }
}
