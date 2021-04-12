// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::Date32Array;
use common_arrow::arrow::array::DictionaryArray;
use common_arrow::arrow::array::Float32Array;
use common_arrow::arrow::array::Float64Array;
use common_arrow::arrow::array::Int16Array;
use common_arrow::arrow::array::Int32Array;
use common_arrow::arrow::array::Int64Array;
use common_arrow::arrow::array::Int8Array;
use common_arrow::arrow::array::StringArray;
use common_arrow::arrow::array::TimestampMicrosecondArray;
use common_arrow::arrow::array::TimestampMillisecondArray;
use common_arrow::arrow::array::TimestampNanosecondArray;
use common_arrow::arrow::array::UInt16Array;
use common_arrow::arrow::array::UInt32Array;
use common_arrow::arrow::array::UInt64Array;
use common_arrow::arrow::array::UInt8Array;
use common_arrow::arrow::datatypes::ArrowDictionaryKeyType;
use common_arrow::arrow::datatypes::ArrowNativeType;
use common_arrow::arrow::datatypes::DataType;
use common_arrow::arrow::datatypes::Int16Type;
use common_arrow::arrow::datatypes::Int32Type;
use common_arrow::arrow::datatypes::Int64Type;
use common_arrow::arrow::datatypes::Int8Type;
use common_arrow::arrow::datatypes::TimeUnit;
use common_arrow::arrow::datatypes::UInt16Type;
use common_arrow::arrow::datatypes::UInt32Type;
use common_arrow::arrow::datatypes::UInt64Type;
use common_arrow::arrow::datatypes::UInt8Type;

// Borrow from apache/arrow/rust/datafusion/
// See notice.md

/// Appends a sequence of [u8] bytes for the value in `col[row]` to
/// `vec` to be used as a key into the hash map
/// Used for group key generated.
pub fn create_key_for_col(col: &ArrayRef, row: usize, vec: &mut Vec<u8>) -> Result<()> {
    match col.data_type() {
        DataType::Boolean => {
            let array = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            vec.extend_from_slice(&[array.value(row) as u8]);
        }
        DataType::Float32 => {
            let array = col.as_any().downcast_ref::<Float32Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Float64 => {
            let array = col.as_any().downcast_ref::<Float64Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::UInt8 => {
            let array = col.as_any().downcast_ref::<UInt8Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::UInt16 => {
            let array = col.as_any().downcast_ref::<UInt16Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::UInt32 => {
            let array = col.as_any().downcast_ref::<UInt32Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::UInt64 => {
            let array = col.as_any().downcast_ref::<UInt64Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Int8 => {
            let array = col.as_any().downcast_ref::<Int8Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Int16 => {
            let array = col.as_any().downcast_ref::<Int16Array>().unwrap();
            vec.extend(array.value(row).to_le_bytes().iter());
        }
        DataType::Int32 => {
            let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Int64 => {
            let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            let array = col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let array = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let array = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Utf8 => {
            let array = col.as_any().downcast_ref::<StringArray>().unwrap();
            let value = array.value(row);
            // store the size
            vec.extend_from_slice(&value.len().to_le_bytes());
            // store the string value
            vec.extend_from_slice(value.as_bytes());
        }
        DataType::Date32 => {
            let array = col.as_any().downcast_ref::<Date32Array>().unwrap();
            vec.extend_from_slice(&array.value(row).to_le_bytes());
        }
        DataType::Dictionary(index_type, _) => match **index_type {
            DataType::Int8 => {
                dictionary_create_key_for_col::<Int8Type>(col, row, vec)?;
            }
            DataType::Int16 => {
                dictionary_create_key_for_col::<Int16Type>(col, row, vec)?;
            }
            DataType::Int32 => {
                dictionary_create_key_for_col::<Int32Type>(col, row, vec)?;
            }
            DataType::Int64 => {
                dictionary_create_key_for_col::<Int64Type>(col, row, vec)?;
            }
            DataType::UInt8 => {
                dictionary_create_key_for_col::<UInt8Type>(col, row, vec)?;
            }
            DataType::UInt16 => {
                dictionary_create_key_for_col::<UInt16Type>(col, row, vec)?;
            }
            DataType::UInt32 => {
                dictionary_create_key_for_col::<UInt32Type>(col, row, vec)?;
            }
            DataType::UInt64 => {
                dictionary_create_key_for_col::<UInt64Type>(col, row, vec)?;
            }
            _ => {
                bail!(
                    "Unsupported key type (dictionary index type not supported creating key) {}",
                    col.data_type(),
                )
            }
        },
        _ => {
            // This is internal because we should have caught this before.
            bail!("Unsupported the col type creating key {}", col.data_type(),);
        }
    }
    Ok(())
}

fn dictionary_create_key_for_col<K: ArrowDictionaryKeyType>(
    col: &ArrayRef,
    row: usize,
    vec: &mut Vec<u8>,
) -> Result<()> {
    let dict_col = col.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();

    // look up the index in the values dictionary
    let keys_col = dict_col.keys_array();
    let values_index = keys_col.value(row).to_usize().ok_or_else(|| {
        anyhow!(
            "Can not convert index to usize in dictionary of type creating group by value {:?}",
            keys_col.data_type()
        )
    })?;

    create_key_for_col(&dict_col.values(), values_index, vec)
}
