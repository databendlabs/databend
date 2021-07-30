// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::datatypes::IntervalUnit;
use common_arrow::arrow::datatypes::TimeUnit;
use common_arrow::arrow::types::NativeType;

use crate::arrays::DataArray;
use crate::DataField;
use crate::DataType;

pub trait DFDataType: Send + Sync {
    fn data_type() -> DataType;
}

macro_rules! impl_df_datatype {
    ($ca:ident, $variant:ident) => {
        pub struct $ca {}

        impl DFDataType for $ca {
            fn data_type() -> DataType {
                DataType::$variant
            }
        }
    };
}

impl_df_datatype!(UInt8Type, UInt8);
impl_df_datatype!(UInt16Type, UInt16);
impl_df_datatype!(UInt32Type, UInt32);
impl_df_datatype!(UInt64Type, UInt64);
impl_df_datatype!(Int8Type, Int8);
impl_df_datatype!(Int16Type, Int16);
impl_df_datatype!(Int32Type, Int32);
impl_df_datatype!(Int64Type, Int64);
impl_df_datatype!(Float32Type, Float32);
impl_df_datatype!(Float64Type, Float64);
impl_df_datatype!(BooleanType, Boolean);
impl_df_datatype!(Date32Type, Date32);
impl_df_datatype!(Date64Type, Date64);

impl_df_datatype!(Utf8Type, Utf8);
impl_df_datatype!(NullType, Null);
impl_df_datatype!(BinaryType, Binary);

pub struct TimestampSecondType;
impl DFDataType for TimestampSecondType {
    fn data_type() -> DataType {
        DataType::Timestamp(TimeUnit::Second, None)
    }
}

pub struct TimestampMillisecondType;
impl DFDataType for TimestampMillisecondType {
    fn data_type() -> DataType {
        DataType::Timestamp(TimeUnit::Millisecond, None)
    }
}

pub struct TimestampMicrosecondType;
impl DFDataType for TimestampMicrosecondType {
    fn data_type() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    }
}

pub struct TimestampNanosecondType;
impl DFDataType for TimestampNanosecondType {
    fn data_type() -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    }
}

pub struct IntervalYearMonthType;
impl DFDataType for IntervalYearMonthType {
    fn data_type() -> DataType {
        DataType::Interval(IntervalUnit::YearMonth)
    }
}
pub struct IntervalDayTimeType;
impl DFDataType for IntervalDayTimeType {
    fn data_type() -> DataType {
        DataType::Interval(IntervalUnit::DayTime)
    }
}

pub struct ListType;
impl DFDataType for ListType {
    fn data_type() -> DataType {
        // null as we cannot no anything without self.
        DataType::List(Box::new(DataField::new("", DataType::Null, true)))
    }
}

pub struct StructType;
impl DFDataType for StructType {
    fn data_type() -> DataType {
        // null as we cannot no anything without self.
        DataType::Struct(vec![DataField::new("", DataType::Null, true)])
    }
}

pub type DFNullArray = DataArray<NullType>;
pub type DFInt8Array = DataArray<Int8Type>;
pub type DFUInt8Array = DataArray<UInt8Type>;
pub type DFInt16Array = DataArray<Int16Type>;
pub type DFUInt16Array = DataArray<UInt16Type>;
pub type DFInt32Array = DataArray<Int32Type>;
pub type DFUInt32Array = DataArray<UInt32Type>;
pub type DFInt64Array = DataArray<Int64Type>;
pub type DFUInt64Array = DataArray<UInt64Type>;

pub type DFBooleanArray = DataArray<BooleanType>;

pub type DFFloat32Array = DataArray<Float32Type>;
pub type DFFloat64Array = DataArray<Float64Type>;

pub type DFUtf8Array = DataArray<Utf8Type>;
pub type DFListArray = DataArray<ListType>;
pub type DFStructArray = DataArray<StructType>;
pub type DFBinaryArray = DataArray<BinaryType>;

pub type DFDate32Array = DataArray<Date32Type>;
pub type DFDate64Array = DataArray<Date64Type>;

pub type DFTimestampSecondArray = DataArray<TimestampSecondType>;
pub type DFTimestampMillisecondArray = DataArray<TimestampMillisecondType>;
pub type DFTimestampMicrosecondArray = DataArray<TimestampMicrosecondType>;
pub type DFTimestampNanosecondArray = DataArray<TimestampNanosecondType>;
pub type DFIntervalYearMonthArray = DataArray<IntervalYearMonthType>;
pub type DFIntervalDayTimeArray = DataArray<IntervalDayTimeType>;

pub trait DFPrimitiveType: Send + Sync + DFDataType + 'static {
    type Native: NativeType;
}

macro_rules! impl_primitive {
    ($ca:ident, $native:ident) => {
        impl DFPrimitiveType for $ca {
            type Native = $native;
        }
    };
}

impl_primitive!(UInt8Type, u8);
impl_primitive!(UInt16Type, u16);
impl_primitive!(UInt32Type, u32);
impl_primitive!(UInt64Type, u64);
impl_primitive!(Int8Type, i8);
impl_primitive!(Int16Type, i16);
impl_primitive!(Int32Type, i32);
impl_primitive!(Int64Type, i64);
impl_primitive!(Float32Type, f32);
impl_primitive!(Float64Type, f64);

impl_primitive!(Date32Type, i32);
impl_primitive!(Date64Type, i64);

impl_primitive!(TimestampSecondType, i64);
impl_primitive!(TimestampMillisecondType, i64);
impl_primitive!(TimestampMicrosecondType, i32);
impl_primitive!(TimestampNanosecondType, i64);

impl_primitive!(IntervalYearMonthType, i32);
impl_primitive!(IntervalDayTimeType, i64);

pub trait DFNumericType: DFPrimitiveType {}

macro_rules! impl_numeric {
    ($ca:ident, $native:ident) => {
        impl DFNumericType for $ca {}
    };
}

impl_numeric!(UInt8Type, u8);
impl_numeric!(UInt16Type, u16);
impl_numeric!(UInt32Type, u32);
impl_numeric!(UInt64Type, u64);
impl_numeric!(Int8Type, i8);
impl_numeric!(Int16Type, i16);
impl_numeric!(Int32Type, i32);
impl_numeric!(Int64Type, i64);
impl_numeric!(Float32Type, f32);
impl_numeric!(Float64Type, f64);

impl_numeric!(Date32Type, i32);
impl_numeric!(Date64Type, i64);

impl_numeric!(TimestampSecondType, i64);
impl_numeric!(TimestampMillisecondType, i64);
impl_numeric!(TimestampMicrosecondType, i32);
impl_numeric!(TimestampNanosecondType, i64);

impl_numeric!(IntervalYearMonthType, i32);
impl_numeric!(IntervalDayTimeType, i64);

pub trait DFFloatType: DFNumericType {}
impl DFFloatType for Float32Type {}
impl DFFloatType for Float64Type {}
