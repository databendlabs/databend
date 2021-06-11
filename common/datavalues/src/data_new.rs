use common_arrow::arrow::datatypes::ArrowNumericType;
use common_arrow::arrow::datatypes::ArrowPrimitiveType;
use common_arrow::arrow::datatypes::DataType;
use common_arrow::arrow::datatypes::TimeUnit;

pub trait DFDataType: Send + Sync {
    fn get_dtype() -> DataType;
}

macro_rules! impl_df_datatype {
    ($ca:ident, $variant:ident) => {
        impl DFDataType for $ca {
            fn get_dtype() -> DataType {
                DataType::$variant
            }
        }
    };
}

pub type UInt8Type = common_arrow::arrow::datatypes::UInt8Type;
pub type UInt16Type = common_arrow::arrow::datatypes::UInt16Type;
pub type UInt32Type = common_arrow::arrow::datatypes::UInt32Type;
pub type UInt64Type = common_arrow::arrow::datatypes::UInt64Type;
pub type Int8Type = common_arrow::arrow::datatypes::Int8Type;
pub type Int16Type = common_arrow::arrow::datatypes::Int16Type;
pub type Int32Type = common_arrow::arrow::datatypes::Int32Type;
pub type Int64Type = common_arrow::arrow::datatypes::Int64Type;
pub type Float32Type = common_arrow::arrow::datatypes::Float32Type;
pub type Float64Type = common_arrow::arrow::datatypes::Float64Type;
pub type BooleanType = common_arrow::arrow::datatypes::BooleanType;
pub type Date32Type = common_arrow::arrow::datatypes::Date32Type;
pub type Date64Type = common_arrow::arrow::datatypes::Date64Type;
pub type Time64NanosecondType = common_arrow::arrow::datatypes::Time64NanosecondType;
pub type DurationNanosecondType = common_arrow::arrow::datatypes::DurationNanosecondType;
pub type DurationMillisecondType = common_arrow::arrow::datatypes::DurationMillisecondType;

pub struct Utf8Type {}
pub struct ListType {}

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

impl DFDataType for Time64NanosecondType {
    fn get_dtype() -> DataType {
        DataType::Time64(TimeUnit::Nanosecond)
    }
}

impl DFDataType for DurationNanosecondType {
    fn get_dtype() -> DataType {
        DataType::Duration(TimeUnit::Nanosecond)
    }
}

impl DFDataType for DurationMillisecondType {
    fn get_dtype() -> DataType {
        DataType::Duration(TimeUnit::Millisecond)
    }
}

impl DFDataType for Utf8Type {
    fn get_dtype() -> DataType {
        DataType::Utf8
    }
}

impl DFDataType for ListType {
    fn get_dtype() -> DataType {
        // null as we cannot no anything without self.
        DataType::List(Box::new(DataType::Null))
    }
}

pub trait DFPrimitiveType: ArrowPrimitiveType + Send + Sync + DFDataType {}
impl DFPrimitiveType for UInt8Type {}
impl DFPrimitiveType for UInt16Type {}
impl DFPrimitiveType for UInt32Type {}
impl DFPrimitiveType for UInt64Type {}
impl DFPrimitiveType for Int8Type {}
impl DFPrimitiveType for Int16Type {}
impl DFPrimitiveType for Int32Type {}
impl DFPrimitiveType for Int64Type {}
impl DFPrimitiveType for Float32Type {}
impl DFPrimitiveType for Float64Type {}
impl DFPrimitiveType for Date32Type {}
impl DFPrimitiveType for Date64Type {}
impl DFPrimitiveType for Time64NanosecondType {}
impl DFPrimitiveType for DurationNanosecondType {}
impl DFPrimitiveType for DurationMillisecondType {}

pub trait DFNumericType: DFPrimitiveType + ArrowNumericType {}
impl DFNumericType for UInt8Type {}
impl DFNumericType for UInt16Type {}
impl DFNumericType for UInt32Type {}
impl DFNumericType for UInt64Type {}
impl DFNumericType for Int8Type {}
impl DFNumericType for Int16Type {}
impl DFNumericType for Int32Type {}
impl DFNumericType for Int64Type {}
impl DFNumericType for Float32Type {}
impl DFNumericType for Float64Type {}
impl DFNumericType for Date32Type {}
impl DFNumericType for Date64Type {}
impl DFNumericType for Time64NanosecondType {}
impl DFNumericType for DurationNanosecondType {}
impl DFNumericType for DurationMillisecondType {}

pub trait DFIntegerType: DFNumericType {}
impl DFIntegerType for UInt8Type {}
impl DFIntegerType for UInt16Type {}
impl DFIntegerType for UInt32Type {}
impl DFIntegerType for UInt64Type {}
impl DFIntegerType for Int8Type {}
impl DFIntegerType for Int16Type {}
impl DFIntegerType for Int32Type {}
impl DFIntegerType for Int64Type {}
impl DFIntegerType for Date32Type {}
impl DFIntegerType for Date64Type {}
impl DFIntegerType for Time64NanosecondType {}
impl DFIntegerType for DurationNanosecondType {}
impl DFIntegerType for DurationMillisecondType {}

pub trait DFFloatType: DFNumericType {}
impl DFFloatType for Float32Type {}
impl DFFloatType for Float64Type {}
