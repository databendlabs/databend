// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::*;
use common_arrow::arrow::datatypes::*;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::DataArrayRef;
use crate::DataColumnarValue;
use crate::DataValue;

impl DataValue {
    /// Appends a sequence of [u8] bytes for the value in `col[row]` to
    /// `vec` to be used as a key into the hash map
    /// Used for group key generated, for example:
    /// A, B
    /// 1, a
    /// 2, b
    ///
    /// key-0: u8[1, 'a']
    /// key-1: u8[2, 'b']
    #[inline]
    pub fn concat_row_to_one_key(
        col: &DataColumnarValue,
        row: usize,
        vec: &mut Vec<u8>
    ) -> Result<()> {
        let (col, row) = match col {
            DataColumnarValue::Array(array) => (Ok(array.clone()), row),
            DataColumnarValue::Constant(v, _) => (v.to_array_with_size(1), 0)
        };
        let col = col?;

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
                    Self::dictionary_create_key_for_col::<Int8Type>(&col, row, vec)?;
                }
                DataType::Int16 => {
                    Self::dictionary_create_key_for_col::<Int16Type>(&col, row, vec)?;
                }
                DataType::Int32 => {
                    Self::dictionary_create_key_for_col::<Int32Type>(&col, row, vec)?;
                }
                DataType::Int64 => {
                    Self::dictionary_create_key_for_col::<Int64Type>(&col, row, vec)?;
                }
                DataType::UInt8 => {
                    Self::dictionary_create_key_for_col::<UInt8Type>(&col, row, vec)?;
                }
                DataType::UInt16 => {
                    Self::dictionary_create_key_for_col::<UInt16Type>(&col, row, vec)?;
                }
                DataType::UInt32 => {
                    Self::dictionary_create_key_for_col::<UInt32Type>(&col, row, vec)?;
                }
                DataType::UInt64 => {
                    Self::dictionary_create_key_for_col::<UInt64Type>(&col, row, vec)?;
                }
                _ => {
                    return Result::Err(ErrorCodes::BadDataValueType(
                        format!(
                            "Unsupported key type (dictionary index type not supported creating key) {}",
                            col.data_type(),
                        )
                    ));
                }
            },
            _ => {
                // This is internal because we should have caught this before.
                return Result::Err(ErrorCodes::BadDataValueType(format!(
                    "Unsupported the col type creating key {}",
                    col.data_type()
                )));
            }
        }
        Ok(())
    }

    #[inline]
    fn dictionary_create_key_for_col<K: ArrowDictionaryKeyType>(
        col: &ArrayRef,
        row: usize,
        vec: &mut Vec<u8>
    ) -> Result<()> {
        let dict_col = col.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();

        // look up the index in the values dictionary
        let keys_col = dict_col.keys_array();
        let values_index = keys_col.value(row).to_usize().ok_or_else(|| {
            ErrorCodes::BadDataValueType(format!(
                "Can not convert index to usize in dictionary of type creating group by value {:?}",
                keys_col.data_type()
            ))
        })?;

        let col = DataColumnarValue::Array(dict_col.values());
        Self::concat_row_to_one_key(&col, values_index, vec)
    }

    /// Convert data value vectors to data array.
    pub fn try_into_data_array(values: &[DataValue]) -> Result<DataArrayRef> {
        match values[0].data_type() {
            DataType::Int8 => try_build_array!(Int8Builder, Int8, values),
            DataType::Int16 => try_build_array!(Int16Builder, Int16, values),
            DataType::Int32 => try_build_array!(Int32Builder, Int32, values),
            DataType::Int64 => try_build_array!(Int64Builder, Int64, values),
            DataType::UInt8 => try_build_array!(UInt8Builder, UInt8, values),
            DataType::UInt16 => try_build_array!(UInt16Builder, UInt16, values),
            DataType::UInt32 => try_build_array!(UInt32Builder, UInt32, values),
            DataType::UInt64 => try_build_array!(UInt64Builder, UInt64, values),
            DataType::Float32 => try_build_array!(Float32Builder, Float32, values),
            DataType::Float64 => try_build_array!(Float64Builder, Float64, values),
            DataType::Utf8 => try_build_array!(StringBuilder, Utf8, values),
            other => Result::Err(ErrorCodes::BadDataValueType(format!(
                "Unexpected type:{} for DataValue List",
                other
            )))
        }
    }

    #[inline]
    pub fn try_from_column(column: &DataColumnarValue, index: usize) -> Result<DataValue> {
        match column {
            DataColumnarValue::Constant(scalar, _) => Ok(scalar.clone()),
            DataColumnarValue::Array(array) => DataValue::try_from_array(array, index)
        }
    }

    #[inline]
    /// Converts a value in `array` at `index` into a ScalarValue
    pub fn try_from_array(array: &DataArrayRef, index: usize) -> Result<DataValue> {
        match array.data_type() {
            DataType::Boolean => {
                typed_cast_from_array_to_data_value!(array, index, BooleanArray, Boolean)
            }
            DataType::Float64 => {
                typed_cast_from_array_to_data_value!(array, index, Float64Array, Float64)
            }
            DataType::Float32 => {
                typed_cast_from_array_to_data_value!(array, index, Float32Array, Float32)
            }
            DataType::UInt64 => {
                typed_cast_from_array_to_data_value!(array, index, UInt64Array, UInt64)
            }
            DataType::UInt32 => {
                typed_cast_from_array_to_data_value!(array, index, UInt32Array, UInt32)
            }
            DataType::UInt16 => {
                typed_cast_from_array_to_data_value!(array, index, UInt16Array, UInt16)
            }
            DataType::UInt8 => {
                typed_cast_from_array_to_data_value!(array, index, UInt8Array, UInt8)
            }
            DataType::Int64 => {
                typed_cast_from_array_to_data_value!(array, index, Int64Array, Int64)
            }
            DataType::Int32 => {
                typed_cast_from_array_to_data_value!(array, index, Int32Array, Int32)
            }
            DataType::Int16 => {
                typed_cast_from_array_to_data_value!(array, index, Int16Array, Int16)
            }
            DataType::Int8 => typed_cast_from_array_to_data_value!(array, index, Int8Array, Int8),
            DataType::Utf8 => {
                typed_cast_from_array_to_data_value!(array, index, StringArray, Utf8)
            }
            DataType::Binary => {
                typed_cast_from_array_to_data_value!(array, index, BinaryArray, Binary)
            }
            DataType::Date32 => {
                typed_cast_from_array_to_data_value!(array, index, Date32Array, Date32)
            }
            DataType::Date64 => {
                typed_cast_from_array_to_data_value!(array, index, Date64Array, Date64)
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                typed_cast_from_array_to_data_value!(
                    array,
                    index,
                    TimestampSecondArray,
                    TimestampSecond
                )
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                typed_cast_from_array_to_data_value!(
                    array,
                    index,
                    TimestampMillisecondArray,
                    TimestampMillisecond
                )
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                typed_cast_from_array_to_data_value!(
                    array,
                    index,
                    TimestampMicrosecondArray,
                    TimestampMicrosecond
                )
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                typed_cast_from_array_to_data_value!(
                    array,
                    index,
                    TimestampNanosecondArray,
                    TimestampNanosecond
                )
            }
            DataType::List(nested_type) => {
                let list_array = array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| ErrorCodes::LogicalError("Failed to downcast ListArray"))?;
                let value = match list_array.is_null(index) {
                    true => None,
                    false => {
                        let nested_array = list_array.value(index);
                        let scalar_vec = (0..nested_array.len())
                            .map(|i| Self::try_from_array(&nested_array, i))
                            .collect::<Result<Vec<_>>>()?;
                        Some(scalar_vec)
                    }
                };
                Ok(DataValue::List(value, nested_type.data_type().clone()))
            }
            DataType::Struct(_) => {
                let strut_array = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| ErrorCodes::LogicalError("Failed to downcast StructArray"))?;
                let nested_array = strut_array.column(index);
                let scalar_vec = (0..nested_array.len())
                    .map(|i| Self::try_from_array(&nested_array, i))
                    .collect::<Result<Vec<_>>>()?;
                Ok(DataValue::Struct(scalar_vec))
            }
            other => Result::Err(ErrorCodes::BadDataValueType(format!(
                "DataValue Error: Can't create a functions of array of type \"{:?}\"",
                other
            )))
        }
    }

    pub fn try_from_literal(literal: &str) -> Result<DataValue> {
        match literal.parse::<i64>() {
            Ok(n) => {
                if n >= 0 {
                    Ok(DataValue::UInt64(Some(n as u64)))
                } else {
                    Ok(DataValue::Int64(Some(n)))
                }
            }
            Err(_) => Ok(DataValue::Float64(Some(literal.parse::<f64>()?)))
        }
    }
}
