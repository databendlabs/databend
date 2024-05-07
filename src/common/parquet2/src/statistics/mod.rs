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

mod binary;
mod boolean;
mod fixed_len_binary;
mod primitive;

use std::any::Any;
use std::sync::Arc;

pub use binary::BinaryStatistics;
pub use boolean::BooleanStatistics;
pub use fixed_len_binary::FixedLenStatistics;
pub use primitive::PrimitiveStatistics;

use crate::error::Result;
use crate::schema::types::PhysicalType;
use crate::schema::types::PrimitiveType;
pub use crate::thrift_format::Statistics as ParquetStatistics;

/// A trait used to describe specific statistics. Each physical type has its own struct.
/// Match the [`Statistics::physical_type`] to each type and downcast accordingly.
pub trait Statistics: Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;

    fn physical_type(&self) -> &PhysicalType;

    fn null_count(&self) -> Option<i64>;
}

impl PartialEq for &dyn Statistics {
    fn eq(&self, other: &Self) -> bool {
        self.physical_type() == other.physical_type() && {
            match self.physical_type() {
                PhysicalType::Boolean => {
                    self.as_any().downcast_ref::<BooleanStatistics>().unwrap()
                        == other.as_any().downcast_ref::<BooleanStatistics>().unwrap()
                }
                PhysicalType::Int32 => {
                    self.as_any()
                        .downcast_ref::<PrimitiveStatistics<i32>>()
                        .unwrap()
                        == other
                            .as_any()
                            .downcast_ref::<PrimitiveStatistics<i32>>()
                            .unwrap()
                }
                PhysicalType::Int64 => {
                    self.as_any()
                        .downcast_ref::<PrimitiveStatistics<i64>>()
                        .unwrap()
                        == other
                            .as_any()
                            .downcast_ref::<PrimitiveStatistics<i64>>()
                            .unwrap()
                }
                PhysicalType::Int96 => {
                    self.as_any()
                        .downcast_ref::<PrimitiveStatistics<[u32; 3]>>()
                        .unwrap()
                        == other
                            .as_any()
                            .downcast_ref::<PrimitiveStatistics<[u32; 3]>>()
                            .unwrap()
                }
                PhysicalType::Float => {
                    self.as_any()
                        .downcast_ref::<PrimitiveStatistics<f32>>()
                        .unwrap()
                        == other
                            .as_any()
                            .downcast_ref::<PrimitiveStatistics<f32>>()
                            .unwrap()
                }
                PhysicalType::Double => {
                    self.as_any()
                        .downcast_ref::<PrimitiveStatistics<f64>>()
                        .unwrap()
                        == other
                            .as_any()
                            .downcast_ref::<PrimitiveStatistics<f64>>()
                            .unwrap()
                }
                PhysicalType::ByteArray => {
                    self.as_any().downcast_ref::<BinaryStatistics>().unwrap()
                        == other.as_any().downcast_ref::<BinaryStatistics>().unwrap()
                }
                PhysicalType::FixedLenByteArray(_) => {
                    self.as_any().downcast_ref::<FixedLenStatistics>().unwrap()
                        == other.as_any().downcast_ref::<FixedLenStatistics>().unwrap()
                }
            }
        }
    }
}

/// Deserializes a raw parquet statistics into [`Statistics`].
/// # Error
/// This function errors if it is not possible to read the statistics to the
/// corresponding `physical_type`.
pub fn deserialize_statistics(
    statistics: &ParquetStatistics,
    primitive_type: PrimitiveType,
) -> Result<Arc<dyn Statistics>> {
    match primitive_type.physical_type {
        PhysicalType::Boolean => boolean::read(statistics),
        PhysicalType::Int32 => primitive::read::<i32>(statistics, primitive_type),
        PhysicalType::Int64 => primitive::read::<i64>(statistics, primitive_type),
        PhysicalType::Int96 => primitive::read::<[u32; 3]>(statistics, primitive_type),
        PhysicalType::Float => primitive::read::<f32>(statistics, primitive_type),
        PhysicalType::Double => primitive::read::<f64>(statistics, primitive_type),
        PhysicalType::ByteArray => binary::read(statistics, primitive_type),
        PhysicalType::FixedLenByteArray(size) => {
            fixed_len_binary::read(statistics, size, primitive_type)
        }
    }
}

/// Serializes [`Statistics`] into a raw parquet statistics.
pub fn serialize_statistics(statistics: &dyn Statistics) -> ParquetStatistics {
    match statistics.physical_type() {
        PhysicalType::Boolean => boolean::write(statistics.as_any().downcast_ref().unwrap()),
        PhysicalType::Int32 => primitive::write::<i32>(statistics.as_any().downcast_ref().unwrap()),
        PhysicalType::Int64 => primitive::write::<i64>(statistics.as_any().downcast_ref().unwrap()),
        PhysicalType::Int96 => {
            primitive::write::<[u32; 3]>(statistics.as_any().downcast_ref().unwrap())
        }
        PhysicalType::Float => primitive::write::<f32>(statistics.as_any().downcast_ref().unwrap()),
        PhysicalType::Double => {
            primitive::write::<f64>(statistics.as_any().downcast_ref().unwrap())
        }
        PhysicalType::ByteArray => binary::write(statistics.as_any().downcast_ref().unwrap()),
        PhysicalType::FixedLenByteArray(_) => {
            fixed_len_binary::write(statistics.as_any().downcast_ref().unwrap())
        }
    }
}
