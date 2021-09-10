//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

// NOTE: COPIED from parquet2

use std::sync::Arc;

use common_arrow::parquet::error::ParquetError;
use common_arrow::parquet::error::Result;
use common_arrow::parquet::schema::types::PhysicalType;
use common_arrow::parquet::statistics::*;
use common_arrow::parquet::types::NativeType;

pub fn reduce(stats: &[Arc<dyn Statistics>]) -> Result<Option<Arc<dyn Statistics>>> {
    if stats.is_empty() {
        return Ok(None);
    }
    let same_type = stats
        .iter()
        .skip(1)
        .all(|x| x.physical_type() == stats[0].physical_type());
    if !same_type {
        return Err(ParquetError::OutOfSpec(
            "The statistics do not have the same data_type".to_string(),
        ));
    };
    Ok(match stats[0].physical_type() {
        PhysicalType::Boolean => {
            let stats = stats.iter().map(|x| x.as_any().downcast_ref().unwrap());
            Some(Arc::new(reduce_boolean(stats)))
        }
        PhysicalType::Int32 => {
            let stats = stats.iter().map(|x| x.as_any().downcast_ref().unwrap());
            Some(Arc::new(reduce_primitive::<i32, _>(stats)))
        }
        PhysicalType::Int64 => {
            let stats = stats.iter().map(|x| x.as_any().downcast_ref().unwrap());
            Some(Arc::new(reduce_primitive::<i64, _>(stats)))
        }
        PhysicalType::Float => {
            let stats = stats.iter().map(|x| x.as_any().downcast_ref().unwrap());
            Some(Arc::new(reduce_primitive::<f32, _>(stats)))
        }
        PhysicalType::Double => {
            let stats = stats.iter().map(|x| x.as_any().downcast_ref().unwrap());
            Some(Arc::new(reduce_primitive::<f64, _>(stats)))
        }
        PhysicalType::ByteArray => {
            let stats = stats.iter().map(|x| x.as_any().downcast_ref().unwrap());
            Some(Arc::new(reduce_binary(stats)))
        }
        _ => todo!(),
    })
}

#[allow(dead_code)]
fn reduce_binary<'a, I: Iterator<Item = &'a BinaryStatistics>>(mut stats: I) -> BinaryStatistics {
    let initial = stats.next().unwrap().clone();
    stats.fold(initial, |mut acc, new| {
        acc.min_value = match (acc.min_value, &new.min_value) {
            (None, None) => None,
            (Some(x), None) => Some(x),
            (None, Some(x)) => Some(x.clone()),
            (Some(x), Some(y)) => Some(ord_binary(x, y.clone(), false)),
        };
        acc.max_value = match (acc.max_value, &new.max_value) {
            (None, None) => None,
            (Some(x), None) => Some(x),
            (None, Some(x)) => Some(x.clone()),
            (Some(x), Some(y)) => Some(ord_binary(x, y.clone(), true)),
        };
        acc.null_count = match (acc.null_count, &new.null_count) {
            (None, None) => None,
            (Some(x), None) => Some(x),
            (None, Some(x)) => Some(*x),
            (Some(x), Some(y)) => Some(x + *y),
        };
        acc.distinct_count = None;
        acc
    })
}

#[allow(dead_code)]
fn ord_binary(a: Vec<u8>, b: Vec<u8>, max: bool) -> Vec<u8> {
    for (v1, v2) in a.iter().zip(b.iter()) {
        match v1.cmp(v2) {
            std::cmp::Ordering::Greater => {
                if max {
                    return a;
                } else {
                    return b;
                }
            }
            std::cmp::Ordering::Less => {
                if max {
                    return b;
                } else {
                    return a;
                }
            }
            _ => {}
        }
    }
    a
}

#[allow(dead_code)]
fn reduce_boolean<'a, I: Iterator<Item = &'a BooleanStatistics>>(
    mut stats: I,
) -> BooleanStatistics {
    let initial = stats.next().unwrap().clone();
    stats.fold(initial, |mut acc, new| {
        acc.min_value = match (acc.min_value, &new.min_value) {
            (None, None) => None,
            (Some(x), None) => Some(x),
            (None, Some(x)) => Some(*x),
            (Some(x), Some(y)) => Some(if x & !(*y) { *y } else { x }),
        };
        acc.max_value = match (acc.max_value, &new.max_value) {
            (None, None) => None,
            (Some(x), None) => Some(x),
            (None, Some(x)) => Some(*x),
            (Some(x), Some(y)) => Some(if x & !(*y) { x } else { *y }),
        };
        acc.null_count = match (acc.null_count, &new.null_count) {
            (None, None) => None,
            (Some(x), None) => Some(x),
            (None, Some(x)) => Some(*x),
            (Some(x), Some(y)) => Some(x + *y),
        };
        acc.distinct_count = None;
        acc
    })
}

#[allow(dead_code)]
fn reduce_primitive<
    'a,
    T: NativeType + std::cmp::PartialOrd,
    I: Iterator<Item = &'a PrimitiveStatistics<T>>,
>(
    mut stats: I,
) -> PrimitiveStatistics<T> {
    let initial = stats.next().unwrap().clone();
    stats.fold(initial, |mut acc, new| {
        acc.min_value = match (acc.min_value, &new.min_value) {
            (None, None) => None,
            (Some(x), None) => Some(x),
            (None, Some(x)) => Some(*x),
            (Some(x), Some(y)) => Some(if x > *y { *y } else { x }),
        };
        acc.max_value = match (acc.max_value, &new.max_value) {
            (None, None) => None,
            (Some(x), None) => Some(x),
            (None, Some(x)) => Some(*x),
            (Some(x), Some(y)) => Some(if x < *y { x } else { *y }),
        };
        acc.null_count = match (acc.null_count, &new.null_count) {
            (None, None) => None,
            (Some(x), None) => Some(x),
            (None, Some(x)) => Some(*x),
            (Some(x), Some(y)) => Some(x + *y),
        };
        acc.distinct_count = None;
        acc
    })
}
