// Copyright 2020 Datafuse Labs.
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

use common_arrow::arrow::types::NativeType;

use super::data_type::*;
use crate::DataField;

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
impl_df_datatype!(StringType, String);

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
impl_primitive!(TimestampMicrosecondType, i64);
impl_primitive!(TimestampNanosecondType, i64);

impl_primitive!(IntervalYearMonthType, i32);
impl_primitive!(IntervalDayTimeType, i64);

pub trait DFNumericType: DFPrimitiveType {
    type LargestType: DFNumericType;
    const SIGN: bool;
    const FLOATING: bool;
    const SIZE: usize;
}

macro_rules! impl_numeric {
    ($ca:ident, $lg: ident, $sign: expr, $floating: expr, $size: expr) => {
        impl DFNumericType for $ca {
            type LargestType = $lg;
            const SIGN: bool = $sign;
            const FLOATING: bool = $floating;
            const SIZE: usize = $size;
        }
    };
}

impl_numeric!(UInt8Type, UInt64Type, false, false, 1);
impl_numeric!(UInt16Type, UInt64Type, false, false, 2);
impl_numeric!(UInt32Type, UInt64Type, false, false, 4);
impl_numeric!(UInt64Type, UInt64Type, false, false, 8);
impl_numeric!(Int8Type, Int64Type, true, false, 1);
impl_numeric!(Int16Type, Int64Type, true, false, 2);
impl_numeric!(Int32Type, Int64Type, true, false, 4);
impl_numeric!(Int64Type, Int64Type, true, false, 8);
impl_numeric!(Float32Type, Float64Type, true, true, 4);
impl_numeric!(Float64Type, Float64Type, true, true, 8);

impl_numeric!(Date32Type, Int64Type, true, false, 4);
impl_numeric!(Date64Type, Int64Type, true, false, 8);

impl_numeric!(TimestampSecondType, Int64Type, true, false, 8);
impl_numeric!(TimestampMillisecondType, Int64Type, true, false, 8);
impl_numeric!(TimestampMicrosecondType, Int64Type, true, false, 8);
impl_numeric!(TimestampNanosecondType, Int64Type, true, false, 8);

impl_numeric!(IntervalYearMonthType, Int64Type, true, false, 4);
impl_numeric!(IntervalDayTimeType, Int64Type, true, false, 8);

pub trait DFIntegerType: DFNumericType {}

macro_rules! impl_integer {
    ($ca:ident, $native:ident) => {
        impl DFIntegerType for $ca {}
    };
}

impl_integer!(UInt8Type, u8);
impl_integer!(UInt16Type, u16);
impl_integer!(UInt32Type, u32);
impl_integer!(UInt64Type, u64);
impl_integer!(Int8Type, i8);
impl_integer!(Int16Type, i16);
impl_integer!(Int32Type, i32);
impl_integer!(Int64Type, i64);

impl_integer!(Date32Type, i32);
impl_integer!(Date64Type, i64);

impl_integer!(TimestampSecondType, i64);
impl_integer!(TimestampMillisecondType, i64);
impl_integer!(TimestampMicrosecondType, i64);
impl_integer!(TimestampNanosecondType, i64);

impl_integer!(IntervalYearMonthType, i32);
impl_integer!(IntervalDayTimeType, i64);

pub trait DFFloatType: DFNumericType {}
impl DFFloatType for Float32Type {}
impl DFFloatType for Float64Type {}
