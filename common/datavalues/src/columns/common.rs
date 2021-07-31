// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::cmp::Ordering;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::compute::concat;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;
pub struct DataColumnCommon;

impl DataColumnCommon {
    pub fn concat(columns: &[DataColumn]) -> Result<DataColumn> {
        let arrays = columns
            .iter()
            .map(|s| s.get_array_ref())
            .collect::<Result<Vec<_>>>()?;

        let dyn_arrays: Vec<&dyn Array> = arrays.iter().map(|arr| arr.as_ref()).collect();

        let array:Arc<dyn Array> = Arc::from(concat::concatenate(&dyn_arrays)?);
        Ok(array.into())
    }
}

impl DataColumn {
    #[inline]
    pub fn serialize(&self, vec: &mut Vec<Vec<u8>>) -> Result<()> {
        let size = self.len();
        let (col, row) = match self {
            DataColumn::Array(array) => (Ok(array.clone()), None),
            DataColumn::Constant(v, _) => (v.to_series_with_size(1), Some(0_usize)),
        };
        let col = col?;

        match col.data_type() {
            DataType::Boolean => {
                let array = col.bool()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&[array.value(row.unwrap_or(i)) as u8]);
                }
            }
            DataType::Float32 => {
                let array = col.f32()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Float64 => {
                let array = col.f64()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::UInt8 => {
                let array = col.u8()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::UInt16 => {
                let array = col.u16()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::UInt32 => {
                let array = col.u32()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::UInt64 => {
                let array = col.u64()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Int8 => {
                let array = col.i8()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Int16 => {
                let array = col.i16()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Int32 => {
                let array = col.i32()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Int64 => {
                let array = col.i64()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }
            DataType::Utf8 => {
                let array = col.utf8()?.downcast_ref();

                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    let value = array.value(row.unwrap_or(i));
                    // store the size
                    v.extend_from_slice(&value.len().to_le_bytes());
                    // store the string value
                    v.extend_from_slice(value.as_bytes());
                }
            }
            DataType::Date32 => {
                let array = col.date32()?.downcast_ref();
                for (i, v) in vec.iter_mut().enumerate().take(size) {
                    v.extend_from_slice(&array.value(row.unwrap_or(i)).to_le_bytes());
                }
            }

            _ => {
                // This is internal because we should have caught this before.
                return Result::Err(ErrorCode::BadDataValueType(format!(
                    "Unsupported the col type creating key {}",
                    col.data_type()
                )));
            }
        }
        Ok(())
    }
}
