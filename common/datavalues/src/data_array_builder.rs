use common_arrow::arrow::array::*;
use common_arrow::arrow::datatypes::*;

// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

/// Returns a builder with capacity `capacity` that corresponds to the datatype `DataType`
/// This function is useful to construct arrays from an arbitrary vectors with known/expected
/// schema.
pub fn make_builder(datatype: &DataType, capacity: usize) -> Box<ArrayBuilder> {
    match datatype {
        DataType::Boolean => Box::new(BooleanBuilder::new(capacity)),
        DataType::Int8 => Box::new(Int8Builder::new(capacity)),
        DataType::Int16 => Box::new(Int16Builder::new(capacity)),
        DataType::Int32 => Box::new(Int32Builder::new(capacity)),
        DataType::Int64 => Box::new(Int64Builder::new(capacity)),
        DataType::UInt8 => Box::new(UInt8Builder::new(capacity)),
        DataType::UInt16 => Box::new(UInt16Builder::new(capacity)),
        DataType::UInt32 => Box::new(UInt32Builder::new(capacity)),
        DataType::UInt64 => Box::new(UInt64Builder::new(capacity)),
        DataType::Float32 => Box::new(Float32Builder::new(capacity)),
        DataType::Float64 => Box::new(Float64Builder::new(capacity)),
        DataType::Binary => Box::new(BinaryBuilder::new(capacity)),
        DataType::FixedSizeBinary(len) => Box::new(FixedSizeBinaryBuilder::new(capacity, *len)),
        DataType::Decimal(precision, scale) => {
            Box::new(DecimalBuilder::new(capacity, *precision, *scale))
        }
        DataType::Utf8 => Box::new(StringBuilder::new(capacity)),
        DataType::Date32 => Box::new(Date32Builder::new(capacity)),
        DataType::Date64 => Box::new(Date64Builder::new(capacity)),
        DataType::Time32(TimeUnit::Second) => Box::new(Time32SecondBuilder::new(capacity)),
        DataType::Time32(TimeUnit::Millisecond) => {
            Box::new(Time32MillisecondBuilder::new(capacity))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            Box::new(Time64MicrosecondBuilder::new(capacity))
        }
        DataType::Time64(TimeUnit::Nanosecond) => Box::new(Time64NanosecondBuilder::new(capacity)),
        DataType::Timestamp(TimeUnit::Second, _) => Box::new(TimestampSecondBuilder::new(capacity)),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Box::new(TimestampMillisecondBuilder::new(capacity))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Box::new(TimestampMicrosecondBuilder::new(capacity))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Box::new(TimestampNanosecondBuilder::new(capacity))
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            Box::new(IntervalYearMonthBuilder::new(capacity))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            Box::new(IntervalDayTimeBuilder::new(capacity))
        }
        DataType::Duration(TimeUnit::Second) => Box::new(DurationSecondBuilder::new(capacity)),
        DataType::Duration(TimeUnit::Millisecond) => {
            Box::new(DurationMillisecondBuilder::new(capacity))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            Box::new(DurationMicrosecondBuilder::new(capacity))
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            Box::new(DurationNanosecondBuilder::new(capacity))
        }
        DataType::Struct(fields) => Box::new(StructBuilder::from_fields(fields.clone(), capacity)),

        t => panic!("Data type {:?} is not currently supported", t)
    }
}
