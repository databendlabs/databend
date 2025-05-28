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

use arrow_array::cast::as_boolean_array;
use arrow_array::cast::as_primitive_array;
use arrow_array::cast::AsArray;
use arrow_array::types::Date32Type;
use arrow_array::types::Date64Type;
use arrow_array::types::Decimal128Type;
use arrow_array::types::Decimal256Type;
use arrow_array::types::DurationMicrosecondType;
use arrow_array::types::DurationMillisecondType;
use arrow_array::types::DurationNanosecondType;
use arrow_array::types::DurationSecondType;
use arrow_array::types::Float16Type;
use arrow_array::types::Float32Type;
use arrow_array::types::Float64Type;
use arrow_array::types::Int16Type;
use arrow_array::types::Int32Type;
use arrow_array::types::Int64Type;
use arrow_array::types::Int8Type;
use arrow_array::types::IntervalDayTimeType;
use arrow_array::types::IntervalMonthDayNanoType;
use arrow_array::types::IntervalYearMonthType;
use arrow_array::types::TimestampMicrosecondType;
use arrow_array::types::TimestampMillisecondType;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::types::TimestampSecondType;
use arrow_array::types::UInt16Type;
use arrow_array::types::UInt32Type;
use arrow_array::types::UInt64Type;
use arrow_array::types::UInt8Type;
use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;
use arrow_schema::IntervalUnit;
use arrow_schema::TimeUnit;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::i256;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Value;

fn array_to_jsonb<'a>(array: &'a ArrayRef, row: usize) -> Result<jsonb::Value<'a>> {
    match array.data_type() {
        DataType::Boolean => {
            let array = as_boolean_array(array);
            let v = array.value(row);
            Ok(jsonb::Value::Bool(v))
        }
        DataType::Int8 => Ok(jsonb::Value::from(
            as_primitive_array::<Int8Type>(array).value(row),
        )),
        DataType::Int16 => Ok(jsonb::Value::from(
            as_primitive_array::<Int16Type>(array).value(row),
        )),
        DataType::Int32 => Ok(jsonb::Value::from(
            as_primitive_array::<Int32Type>(array).value(row),
        )),
        DataType::Int64 => Ok(jsonb::Value::from(
            as_primitive_array::<Int64Type>(array).value(row),
        )),
        DataType::UInt8 => Ok(jsonb::Value::from(
            as_primitive_array::<UInt8Type>(array).value(row),
        )),
        DataType::UInt16 => Ok(jsonb::Value::from(
            as_primitive_array::<UInt16Type>(array).value(row),
        )),
        DataType::UInt32 => Ok(jsonb::Value::from(
            as_primitive_array::<UInt32Type>(array).value(row),
        )),
        DataType::UInt64 => Ok(jsonb::Value::from(
            as_primitive_array::<UInt64Type>(array).value(row),
        )),
        DataType::Float16 => Ok(jsonb::Value::from(
            as_primitive_array::<Float16Type>(array).value(row).to_f64(),
        )),
        DataType::Float32 => Ok(jsonb::Value::from(
            as_primitive_array::<Float32Type>(array).value(row),
        )),
        DataType::Float64 => Ok(jsonb::Value::from(
            as_primitive_array::<Float64Type>(array).value(row),
        )),
        DataType::Timestamp(unit, tz) => {
            if let Some(tz) = tz {
                return Err(ErrorCode::BadDataValueType(format!(
                    "can not convert to variant, Timestamp with tz = {} not supported yet",
                    tz
                )));
            }
            let value = match unit {
                TimeUnit::Second => {
                    let value = as_primitive_array::<TimestampSecondType>(array).value(row);
                    value * 1000000
                }
                TimeUnit::Millisecond => {
                    let value = as_primitive_array::<TimestampMillisecondType>(array).value(row);
                    value * 1000
                }
                TimeUnit::Microsecond => {
                    let value = as_primitive_array::<TimestampMicrosecondType>(array).value(row);
                    value
                }
                TimeUnit::Nanosecond => {
                    let value = as_primitive_array::<TimestampNanosecondType>(array).value(row);
                    value / 1000
                }
            };
            Ok(jsonb::Value::Timestamp(jsonb::Timestamp { value }))
        }
        DataType::Date32 => {
            let value = as_primitive_array::<Date32Type>(array).value(row);
            Ok(jsonb::Value::Date(jsonb::Date { value }))
        }
        DataType::Date64 => {
            let value = as_primitive_array::<Date64Type>(array).value(row);
            Ok(jsonb::Value::Date(jsonb::Date {
                value: (value / 86400) as i32,
            }))
        }
        DataType::Duration(unit) => {
            let value = match unit {
                TimeUnit::Second => {
                    let value = as_primitive_array::<DurationSecondType>(array).value(row);
                    value * 1000000
                }
                TimeUnit::Millisecond => {
                    let value = as_primitive_array::<DurationMillisecondType>(array).value(row);
                    value * 1000
                }
                TimeUnit::Microsecond => {
                    let value = as_primitive_array::<DurationMicrosecondType>(array).value(row);
                    value
                }
                TimeUnit::Nanosecond => {
                    let value = as_primitive_array::<DurationNanosecondType>(array).value(row);
                    value / 1000
                }
            };
            Ok(jsonb::Value::Timestamp(jsonb::Timestamp { value }))
        }
        DataType::Interval(unit) => {
            let value = match unit {
                IntervalUnit::YearMonth => {
                    let value = as_primitive_array::<IntervalYearMonthType>(array).value(row);
                    jsonb::Interval {
                        months: value,
                        days: 0,
                        micros: 0,
                    }
                }
                IntervalUnit::DayTime => {
                    let value = as_primitive_array::<IntervalDayTimeType>(array).value(row);
                    jsonb::Interval {
                        months: value.days / 12,
                        days: value.days % 12,
                        micros: value.milliseconds as i64 * 1000,
                    }
                }
                IntervalUnit::MonthDayNano => {
                    let value = as_primitive_array::<IntervalMonthDayNanoType>(array).value(row);
                    jsonb::Interval {
                        months: value.months,
                        days: value.days,
                        micros: value.nanoseconds / 1000,
                    }
                }
            };
            Ok(jsonb::Value::Interval(value))
        }
        DataType::Binary => {
            let array = array.as_binary::<i32>();
            let v = array.value(row);
            Ok(jsonb::Value::Binary(v))
        }
        DataType::FixedSizeBinary(_) => {
            let array = array.as_fixed_size_binary();
            let v = array.value(row);
            Ok(jsonb::Value::Binary(v))
        }
        DataType::LargeBinary => {
            let array = array.as_binary::<i64>();
            let v = array.value(row);
            Ok(jsonb::Value::Binary(v))
        }
        DataType::BinaryView => {
            let array = array.as_binary_view();
            let v = array.value(row);
            Ok(jsonb::Value::Binary(v))
        }
        DataType::Utf8 => {
            let array = array.as_string::<i32>();
            let v = array.value(row);
            Ok(jsonb::Value::from(v))
        }
        DataType::LargeUtf8 => {
            let array = array.as_string::<i64>();
            let v = array.value(row);
            Ok(jsonb::Value::from(v))
        }
        DataType::Utf8View => {
            let array = array.as_string_view();
            let v = array.value(row);
            Ok(jsonb::Value::from(v))
        }
        DataType::List(_) => {
            let array = array.as_list::<i32>();
            let end = array.value_offsets()[row + 1].as_usize();
            let start = array.value_offsets()[row].as_usize();
            let mut values = Vec::with_capacity(end - start);
            for i in start..end {
                values.push(array_to_jsonb(array.values(), i)?);
            }
            Ok(jsonb::Value::Array(values))
        }
        DataType::ListView(_) => {
            let array = array.as_list_view::<i32>();
            let end = array.value_offsets()[row + 1].as_usize();
            let start = array.value_offsets()[row].as_usize();
            let mut values = Vec::with_capacity(end - start);
            for i in start..end {
                values.push(array_to_jsonb(array.values(), i)?);
            }
            Ok(jsonb::Value::Array(values))
        }
        DataType::FixedSizeList(_, len) => {
            let array = array.as_fixed_size_list();
            let mut values = Vec::with_capacity(*len as usize);
            let len = *len as usize;
            let start = row * len;
            for i in start..(start + len) {
                values.push(array_to_jsonb(array.values(), i)?);
            }
            Ok(jsonb::Value::Array(values))
        }
        DataType::LargeList(_) => {
            let array = array.as_list::<i64>();
            let end = array.value_offsets()[row + 1].as_usize();
            let start = array.value_offsets()[row].as_usize();
            let mut values = Vec::with_capacity(end - start);
            for i in start..end {
                values.push(array_to_jsonb(array.values(), i)?);
            }
            Ok(jsonb::Value::Array(values))
        }
        DataType::LargeListView(_) => {
            let array = array.as_list_view::<i64>();
            let end = array.value_offsets()[row + 1].as_usize();
            let start = array.value_offsets()[row].as_usize();
            let mut values = Vec::with_capacity(end - start);
            for i in start..end {
                values.push(array_to_jsonb(array.values(), i)?);
            }
            Ok(jsonb::Value::Array(values))
        }
        DataType::Decimal128(p, s) if *s >= 0 => {
            let array = array.as_primitive::<Decimal128Type>();
            let value = array.value(row);
            Ok(jsonb::Value::Number(jsonb::Number::Decimal128(
                jsonb::Decimal128 {
                    precision: *p,
                    scale: *s as u8,
                    value,
                },
            )))
        }
        DataType::Decimal256(p, s) if *s > 0 => {
            let array = array.as_primitive::<Decimal256Type>();
            let value = array.value(row).to_be_bytes();
            Ok(jsonb::Value::Number(jsonb::Number::Decimal256(
                jsonb::Decimal256 {
                    precision: *p,
                    scale: *s as u8,
                    value: i256::from_be_bytes(value).0,
                },
            )))
        }
        DataType::Struct(fields) => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            arrays_to_jsonb(fields, array.columns(), row)
        }
        DataType::Map(_, _) => {
            let array = array.as_ref().as_map();
            let [key_array, value_array] = array.entries().columns() else {
                return Err(ErrorCode::BadDataValueType(
                    "arrow map array expect 2 children",
                ));
            };
            let key_type = key_array.data_type();
            let mut obj = jsonb::Object::default();
            match key_type {
                DataType::Utf8 => {
                    let key_array = key_array.as_string::<i32>();
                    for i in 0..key_array.len() {
                        obj.insert(
                            key_array.value(i).to_string(),
                            array_to_jsonb(value_array, i)?,
                        );
                    }
                }
                DataType::Utf8View => {
                    let key_array = key_array.as_string_view();
                    for i in 0..key_array.len() {
                        obj.insert(
                            key_array.value(i).to_string(),
                            array_to_jsonb(value_array, i)?,
                        );
                    }
                }
                _ => {
                    return Err(ErrorCode::BadDataValueType(format!(
                        "can not convert to variant, key must be string, got {key_type}"
                    )))
                }
            }
            Ok(jsonb::Value::Object(obj))
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "unsupported arrow array type: {}",
            array.data_type()
        ))),
    }
}

fn arrays_to_jsonb<'a>(
    fields: &[arrow_schema::FieldRef],
    arrays: &'a [ArrayRef],
    row: usize,
) -> Result<jsonb::Value<'a>> {
    let mut obj = jsonb::Object::default();
    for (field, array) in fields.iter().zip(arrays) {
        obj.insert(field.name().to_string(), array_to_jsonb(array, row)?);
    }
    Ok(jsonb::Value::Object(obj))
}

pub fn read_record_batch(batch: RecordBatch, builder: &mut BinaryColumnBuilder) -> Result<()> {
    let batch = batch;
    let schema = batch.schema();
    let fields = schema.fields();
    for i in 0..batch.num_rows() {
        let value = arrays_to_jsonb(fields, batch.columns(), i)?;
        value.write_to_vec(&mut builder.data);
        builder.commit_row()
    }
    Ok(())
}

pub fn batch_to_block(batch: RecordBatch) -> Result<DataBlock> {
    let mut builder =
        BinaryColumnBuilder::with_capacity(batch.num_rows(), batch.get_array_memory_size());
    read_record_batch(batch, &mut builder)?;
    let column = builder.build();
    let num_rows = column.len();
    let entry = BlockEntry::new(
        databend_common_expression::types::DataType::Variant,
        Value::Column(Column::Variant(column)),
    );
    Ok(DataBlock::new(vec![entry], num_rows))
}
