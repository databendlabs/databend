// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::arrays::ops::agg::ArrayAgg;
use crate::prelude::*;
use crate::DFBooleanArray;
use crate::DFUInt16Array;
use crate::DFUtf8Array;

#[test]
fn test_array_agg() -> Result<()> {
    let array = DFUInt16Array::new_from_iter(1u16..4u16);
    let value = [
        array.sum()?,
        array.max()?,
        array.min()?,
        array.arg_min()?,
        array.arg_max()?,
    ];

    let expected = [
        DataValue::UInt16(Some(6)),
        DataValue::UInt16(Some(3)),
        DataValue::UInt16(Some(1)),
        DataValue::Struct(vec![DataValue::UInt64(Some(0)), DataValue::UInt16(Some(1))]),
        DataValue::Struct(vec![DataValue::UInt64(Some(2)), DataValue::UInt16(Some(3))]),
    ];

    let len = value.len();
    for i in 0..len {
        assert_eq!(value[i], expected[i]);
    }
    Ok(())
}

#[test]
fn test_boolean_array_agg() -> Result<()> {
    let array = DFBooleanArray::new_from_slice(&vec![true, false, true]);

    let value = [
        array.sum()?,
        array.max()?,
        array.min()?,
        array.arg_min()?,
        array.arg_max()?,
    ];

    let expected = [
        DataValue::UInt32(Some(2)),
        DataValue::UInt32(Some(1)),
        DataValue::UInt32(Some(0)),
        DataValue::Struct(vec![
            DataValue::UInt64(Some(1)),
            DataValue::Boolean(Some(false)),
        ]),
        DataValue::Struct(vec![
            DataValue::UInt64(Some(0)),
            DataValue::Boolean(Some(true)),
        ]),
    ];
    let len = value.len();
    for i in 0..len {
        assert_eq!(value[i], expected[i]);
    }
    Ok(())
}

#[test]
fn test_utf8_array_agg() -> Result<()> {
    let array = DFUtf8Array::new_from_slice(&vec!["h", "e", "l", "o"]);

    let value = [
        array.max()?,
        array.min()?,
        array.arg_min()?,
        array.arg_max()?,
    ];

    let expected = [
        DataValue::Utf8(Some("o".to_string())),
        DataValue::Utf8(Some("e".to_string())),
        DataValue::Struct(vec![
            DataValue::UInt64(Some(1)),
            DataValue::Utf8(Some("e".to_string())),
        ]),
        DataValue::Struct(vec![
            DataValue::UInt64(Some(3)),
            DataValue::Utf8(Some("o".to_string())),
        ]),
    ];
    let len = value.len();
    for i in 0..len {
        assert_eq!(value[i], expected[i]);
    }
    Ok(())
}
