// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

macro_rules! downcast_array {
    ($ARRAY:expr, $TYPE:ident) => {
        $ARRAY.as_any().downcast_ref::<$TYPE>().expect(
            format!(
                "Unsupported downcast_array() for datatype: {:?} item to: {}",
                ($ARRAY).data_type(),
                stringify!($TYPE)
            )
            .as_str(),
        );
    };
}

macro_rules! arithmetic_compute {
    ($LEFT:expr, $RIGHT:expr, $FUNC:ident) => {
        match ($LEFT).data_type() {
            DataType::Int8 => {
                return Ok(Arc::new(arrow::compute::$FUNC(
                    downcast_array!($LEFT, Int8Array),
                    downcast_array!($RIGHT, Int8Array),
                )?));
            }
            DataType::Int16 => {
                return Ok(Arc::new(arrow::compute::$FUNC(
                    downcast_array!($LEFT, Int16Array),
                    downcast_array!($RIGHT, Int16Array),
                )?));
            }
            DataType::Int32 => {
                return Ok(Arc::new(arrow::compute::$FUNC(
                    downcast_array!($LEFT, Int32Array),
                    downcast_array!($RIGHT, Int32Array),
                )?));
            }
            DataType::Int64 => {
                return Ok(Arc::new(arrow::compute::$FUNC(
                    downcast_array!($LEFT, Int64Array),
                    downcast_array!($RIGHT, Int64Array),
                )?));
            }

            DataType::UInt8 => {
                return Ok(Arc::new(arrow::compute::$FUNC(
                    downcast_array!($LEFT, UInt8Array),
                    downcast_array!($RIGHT, UInt8Array),
                )?));
            }
            DataType::UInt16 => {
                return Ok(Arc::new(arrow::compute::$FUNC(
                    downcast_array!($LEFT, UInt16Array),
                    downcast_array!($RIGHT, UInt16Array),
                )?));
            }
            DataType::UInt32 => {
                return Ok(Arc::new(arrow::compute::$FUNC(
                    downcast_array!($LEFT, UInt32Array),
                    downcast_array!($RIGHT, UInt32Array),
                )?));
            }
            DataType::UInt64 => {
                return Ok(Arc::new(arrow::compute::$FUNC(
                    downcast_array!($LEFT, UInt64Array),
                    downcast_array!($RIGHT, UInt64Array),
                )?));
            }
            DataType::Float32 => {
                return Ok(Arc::new(arrow::compute::$FUNC(
                    downcast_array!($LEFT, Float32Array),
                    downcast_array!($RIGHT, Float32Array),
                )?));
            }
            DataType::Float64 => {
                return Ok(Arc::new(arrow::compute::$FUNC(
                    downcast_array!($LEFT, Float64Array),
                    downcast_array!($RIGHT, Float64Array),
                )?));
            }
            _ => Err(FuseQueryError::Unsupported(format!(
                "Unsupported arithmetic_compute::{} for data type: {:?}",
                stringify!($FUNC),
                ($LEFT).data_type(),
            ))),
        }
    };
}

macro_rules! typed_data_value_add {
    ($OLD_VALUE:expr, $DELTA:expr, $SCALAR:ident, $TYPE:ident) => {{
        DataValue::$SCALAR(match ($OLD_VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone() as $TYPE),
            (Some(a), Some(b)) => Some(a + (*b as $TYPE)),
        })
    }};
}

macro_rules! typed_data_value_min_max {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident) => {{
        DataValue::$SCALAR(match ($VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (Some(a), Some(b)) => Some((*a).$OP(*b)),
        })
    }};
}

macro_rules! typed_array_sum_to_data_value {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        let array = $VALUES.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let delta = compute::sum(array);
        DataValue::$SCALAR(delta)
    }};
}

macro_rules! typed_array_min_max_to_data_value {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = $VALUES.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let value = compute::$OP(array);
        DataValue::$SCALAR(value)
    }};
}

macro_rules! format_data_value_with_option {
    ($F:expr, $EXPR:expr) => {{
        match $EXPR {
            Some(e) => write!($F, "{}", e),
            None => write!($F, "NULL"),
        }
    }};
}
