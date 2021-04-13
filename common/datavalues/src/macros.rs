// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

macro_rules! downcast_array {
    ($ARRAY:expr, $TYPE:ident) => {
        if let Some(v) = $ARRAY.as_any().downcast_ref::<$TYPE>() {
            Ok(v)
        } else {
            Err(anyhow::Error::msg(format!(
                "DataValue Error: Cannot downcast_array from datatype:{:?} item to:{}",
                ($ARRAY).data_type(),
                stringify!($TYPE)
            )))
        }
    };
}

/// Invoke a compute kernel on a pair of arrays
macro_rules! compute_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = downcast_array!($LEFT, $DT)?;
        let rr = downcast_array!($RIGHT, $DT)?;
        Ok(Arc::new(common_arrow::arrow::compute::$OP(&ll, &rr)?))
    }};
}

/// Invoke a compute kernel on a pair of binary data arrays
macro_rules! compute_utf8_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = downcast_array!($LEFT, $DT)?;
        let rr = downcast_array!($RIGHT, $DT)?;
        Ok(Arc::new(
            paste::expr! {common_arrow::arrow::compute::[<$OP _utf8>]}(&ll, &rr)?,
        ))
    }};
}

/// Invoke a self defined compute kernel on a pair of arrays
macro_rules! compute_self_defined_op {
    ($LEFT:expr, $RIGHT:expr, $OP:tt, $DT:ident) => {{
        let ll = downcast_array!($LEFT, $DT)?;
        let rr = downcast_array!($RIGHT, $DT)?;
        Ok(Arc::new(common_arrow::arrow::compute::math_op(
            &ll, &rr, $OP,
        )?))
    }};
}

/// Invoke a compute kernel on a pair of arrays
/// The arrow_primitive_array_op macro only evaluates for primitive types
/// like integers and floats.
macro_rules! arrow_primitive_array_op {
    ($LEFT:expr, $RIGHT:expr, $RESULT:expr, $OP:ident) => {
        match $RESULT {
            DataType::Int8 => compute_op!($LEFT, $RIGHT, $OP, Int8Array),
            DataType::Int16 => compute_op!($LEFT, $RIGHT, $OP, Int16Array),
            DataType::Int32 => compute_op!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::UInt8 => compute_op!($LEFT, $RIGHT, $OP, UInt8Array),
            DataType::UInt16 => compute_op!($LEFT, $RIGHT, $OP, UInt16Array),
            DataType::UInt32 => compute_op!($LEFT, $RIGHT, $OP, UInt32Array),
            DataType::UInt64 => compute_op!($LEFT, $RIGHT, $OP, UInt64Array),
            DataType::Float32 => compute_op!($LEFT, $RIGHT, $OP, Float32Array),
            DataType::Float64 => compute_op!($LEFT, $RIGHT, $OP, Float64Array),
            _ => anyhow::bail!(format!(
                "Unsupported arithmetic_compute::{} for data type: {:?}",
                stringify!($OP),
                ($LEFT).data_type(),
            )),
        }
    };
}

/// Invoke a compute kernel on a pair of arrays
/// The arrow_primitive_array_self_defined_op macro only evaluates for primitive types
/// like integers and floats.
macro_rules! arrow_primitive_array_self_defined_op {
    ($LEFT:expr, $RIGHT:expr, $RESULT:expr, $OP:tt) => {
        match $RESULT {
            DataType::Int8 => compute_self_defined_op!($LEFT, $RIGHT, $OP, Int8Array),
            DataType::Int16 => compute_self_defined_op!($LEFT, $RIGHT, $OP, Int16Array),
            DataType::Int32 => compute_self_defined_op!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_self_defined_op!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::UInt8 => compute_self_defined_op!($LEFT, $RIGHT, $OP, UInt8Array),
            DataType::UInt16 => compute_self_defined_op!($LEFT, $RIGHT, $OP, UInt16Array),
            DataType::UInt32 => compute_self_defined_op!($LEFT, $RIGHT, $OP, UInt32Array),
            DataType::UInt64 => compute_self_defined_op!($LEFT, $RIGHT, $OP, UInt64Array),
            DataType::Float32 => compute_self_defined_op!($LEFT, $RIGHT, $OP, Float32Array),
            DataType::Float64 => compute_self_defined_op!($LEFT, $RIGHT, $OP, Float64Array),
            _ => anyhow::bail!(format!(
                "Unsupported arithmetic_compute::math_op for data type: {:?}",
                ($LEFT).data_type(),
            )),
        }
    };
}

/// The arrow_array_op macro includes types that extend beyond the primitive,
/// such as Utf8 strings.
macro_rules! arrow_array_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {
        match ($LEFT).data_type() {
            DataType::Int8 => compute_op!($LEFT, $RIGHT, $OP, Int8Array),
            DataType::Int16 => compute_op!($LEFT, $RIGHT, $OP, Int16Array),
            DataType::Int32 => compute_op!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::UInt8 => compute_op!($LEFT, $RIGHT, $OP, UInt8Array),
            DataType::UInt16 => compute_op!($LEFT, $RIGHT, $OP, UInt16Array),
            DataType::UInt32 => compute_op!($LEFT, $RIGHT, $OP, UInt32Array),
            DataType::UInt64 => compute_op!($LEFT, $RIGHT, $OP, UInt64Array),
            DataType::Float32 => compute_op!($LEFT, $RIGHT, $OP, Float32Array),
            DataType::Float64 => compute_op!($LEFT, $RIGHT, $OP, Float64Array),
            DataType::Utf8 => compute_utf8_op!($LEFT, $RIGHT, $OP, StringArray),
            _ => anyhow::bail!(format!(
                "Unsupported arithmetic_compute::{} for data type: {:?}",
                stringify!($OP),
                ($LEFT).data_type(),
            )),
        }
    };
}

/// Invoke a compute kernel on a data array and a scalar value
macro_rules! compute_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        use std::convert::TryInto;

        let ll = downcast_array!($LEFT, $DT)?;
        Ok(Arc::new(
            paste::expr! {common_arrow::arrow::compute::[<$OP _scalar>]}(&ll, $RIGHT.try_into()?)?,
        ))
    }};
}

/// Invoke a compute kernel on a data array and a scalar value
macro_rules! compute_utf8_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = downcast_array!($LEFT, $DT)?;
        if let crate::DataValue::Utf8(Some(string_value)) = $RIGHT {
            Ok(Arc::new(
                paste::expr! {common_arrow::arrow::compute::[<$OP _utf8_scalar>]}(
                    &ll,
                    &string_value,
                )?,
            ))
        } else {
            anyhow::bail!(format!(
                "compute_utf8_op_scalar failed to cast literal value {}",
                $RIGHT
            ));
        }
    }};
}

/// The arrow_array_op_scalar macro includes types that extend beyond the primitive,
/// such as Utf8 strings.
macro_rules! arrow_array_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        let result = match $LEFT.data_type() {
            DataType::Int8 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int8Array),
            DataType::Int16 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int16Array),
            DataType::Int32 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::UInt8 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt8Array),
            DataType::UInt16 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt16Array),
            DataType::UInt32 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt32Array),
            DataType::UInt64 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt64Array),
            DataType::Float32 => compute_op_scalar!($LEFT, $RIGHT, $OP, Float32Array),
            DataType::Float64 => compute_op_scalar!($LEFT, $RIGHT, $OP, Float64Array),
            DataType::Utf8 => compute_utf8_op_scalar!($LEFT, $RIGHT, $OP, StringArray),
            other => Err(anyhow::Error::msg(format!(
                "DataValue Error: Unsupported data type {:?}",
                other
            ))),
        };
        Ok(result?)
    }};
}

macro_rules! typed_array_sum_to_data_value {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        let array = downcast_array!($VALUES, $ARRAYTYPE)?;
        let delta = common_arrow::arrow::compute::sum(array);
        DataValue::$SCALAR(delta)
    }};
}

macro_rules! typed_array_min_max_to_data_value {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = downcast_array!($VALUES, $ARRAYTYPE)?;
        let value = common_arrow::arrow::compute::$OP(array);
        DataValue::$SCALAR(value)
    }};
}

macro_rules! typed_array_min_max_string_to_data_value {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = downcast_array!($VALUES, $ARRAYTYPE)?;
        let value = common_arrow::arrow::compute::$OP(array);
        let value = value.and_then(|e| Some(e.to_string()));
        DataValue::$SCALAR(value)
    }};
}
// returns the sum of two data values, including coercion into $TYPE.
macro_rules! typed_data_value_add {
    ($OLD_VALUE:expr, $DELTA:expr, $SCALAR:ident, $TYPE:ident) => {{
        DataValue::$SCALAR(match ($OLD_VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone() as $TYPE),
            (None, Some(b)) => Some(b.clone() as $TYPE),
            (Some(a), Some(b)) => Some((*a as $TYPE) + (*b as $TYPE)),
        })
    }};
}

// returns the sub of two data values, including coercion into $TYPE.
macro_rules! typed_data_value_sub {
    ($OLD_VALUE:expr, $DELTA:expr, $SCALAR:ident, $TYPE:ident) => {{
        DataValue::$SCALAR(match ($OLD_VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone() as $TYPE),
            (None, Some(b)) => Some(b.clone() as $TYPE),
            (Some(a), Some(b)) => Some((*a as $TYPE) - (*b as $TYPE)),
        })
    }};
}

// returns the mul of two data values, including coercion into $TYPE.
macro_rules! typed_data_value_mul {
    ($OLD_VALUE:expr, $DELTA:expr, $SCALAR:ident, $TYPE:ident) => {{
        DataValue::$SCALAR(match ($OLD_VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone() as $TYPE),
            (None, Some(b)) => Some(b.clone() as $TYPE),
            (Some(a), Some(b)) => Some((*a as $TYPE) * (*b as $TYPE)),
        })
    }};
}

// returns the div of two data values, including coercion into $TYPE.
macro_rules! typed_data_value_div {
    ($OLD_VALUE:expr, $DELTA:expr, $SCALAR:ident, $TYPE:ident) => {{
        DataValue::$SCALAR(match ($OLD_VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone() as f64),
            (None, Some(b)) => Some(b.clone() as f64),
            (Some(a), Some(b)) => Some((*a as f64) / (*b as f64)),
        })
    }};
}

// returns the modulo of two data values, including coercion into $TYPE.
macro_rules! typed_data_value_modulo {
    ($OLD_VALUE:expr, $DELTA:expr, $SCALAR:ident, $TYPE:ident) => {{
        DataValue::$SCALAR(match ($OLD_VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone() as $TYPE),
            (None, Some(b)) => Some(b.clone() as $TYPE),
            (Some(a), Some(b)) => Some((*a as $TYPE) % (*b as $TYPE)),
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

// min/max of two scalar string values.
macro_rules! typed_data_value_min_max_string {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident) => {{
        DataValue::$SCALAR(match ($VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (Some(a), Some(b)) => Some((a).$OP(b).clone()),
        })
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

/// Invoke a boolean kernel on a pair of arrays
macro_rules! array_boolean_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = downcast_array!($LEFT, $DT)?;
        let rr = downcast_array!($RIGHT, $DT)?;
        Ok(Arc::new(common_arrow::arrow::compute::$OP(&ll, &rr)?))
    }};
}

macro_rules! typed_cast_from_array_to_data_value {
    ($array:expr, $index:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        use common_arrow::arrow::array::*;
        let array = downcast_array!($array, $ARRAYTYPE)?;
        DataValue::$SCALAR(match array.is_null($index) {
            true => None,
            false => Some(array.value($index).into()),
        })
    }};
}

macro_rules! typed_cast_from_data_value_to_std {
    ($SCALAR:ident, $NATIVE:ident) => {
        impl TryFrom<DataValue> for $NATIVE {
            type Error = anyhow::Error;

            fn try_from(value: DataValue) -> anyhow::Result<Self> {
                match value {
                    DataValue::$SCALAR(Some(inner_value)) => Ok(inner_value),
                    _ => anyhow::bail!(format!(
                        "DataValue Error:  Cannot convert {:?} to {}",
                        value,
                        std::any::type_name::<Self>()
                    )),
                }
            }
        }
    };
}

macro_rules! build_list {
    ($VALUE_BUILDER_TY:ident, $SCALAR_TY:ident, $VALUES:expr, $SIZE:expr) => {{
        match $VALUES {
            // the return on the macro is necessary, to short-circuit and return ArrayRef
            None => {
                return Ok(common_arrow::arrow::array::new_null_array(
                    &DataType::List(Box::new(DataField::new("item", DataType::$SCALAR_TY, true))),
                    $SIZE,
                ))
            }
            Some(values) => {
                let mut builder = ListBuilder::new($VALUE_BUILDER_TY::new(values.len()));

                for _ in 0..$SIZE {
                    for scalar_value in values {
                        match scalar_value {
                            DataValue::$SCALAR_TY(Some(v)) => {
                                builder.values().append_value(v.clone()).unwrap()
                            }
                            DataValue::$SCALAR_TY(None) => {
                                builder.values().append_null().unwrap();
                            }
                            _ => panic!("Incompatible DataValue for list"),
                        };
                    }
                    builder.append(true).unwrap();
                }
                builder.finish()
            }
        }
    }};
}
