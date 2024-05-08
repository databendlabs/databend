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
use crate::error::Result;
use crate::schema::types::PhysicalType;
use crate::schema::types::PrimitiveType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryStatistics {
    pub primitive_type: PrimitiveType,
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub max_value: Option<Vec<u8>>,
    pub min_value: Option<Vec<u8>>,
}

impl Statistics for BinaryStatistics {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &PhysicalType::ByteArray
    }

    fn null_count(&self) -> Option<i64> {
        self.null_count
    }
}

pub fn read(v: &ParquetStatistics, primitive_type: PrimitiveType) -> Result<Arc<dyn Statistics>> {
    Ok(Arc::new(BinaryStatistics {
        primitive_type,
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.clone(),
        min_value: v.min_value.clone(),
    }))
}

pub fn write(v: &BinaryStatistics) -> ParquetStatistics {
    ParquetStatistics {
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.clone(),
        min_value: v.min_value.clone(),
        min: None,
        max: None,
    }
}
