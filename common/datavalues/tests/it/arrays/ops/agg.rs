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

use common_datavalues::prelude::*;
use common_exception::Result;

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
        DataValue::UInt64(Some(6)),
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
    let array = DFBooleanArray::new_from_slice(&[true, false, true]);

    let value = [
        array.sum()?,
        array.max()?,
        array.min()?,
        array.arg_min()?,
        array.arg_max()?,
    ];

    let expected = [
        DataValue::UInt64(Some(2)),
        DataValue::Boolean(Some(true)),
        DataValue::Boolean(Some(false)),
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
        assert_eq!(value[i], expected[i], "in test_{}", i);
    }
    Ok(())
}

#[test]
fn test_string_array_agg() -> Result<()> {
    let array = DFStringArray::new_from_slice(&["h", "e", "l", "o"]);

    let value = [
        array.max()?,
        array.min()?,
        array.arg_min()?,
        array.arg_max()?,
    ];

    let expected = [
        DataValue::String(Some("o".as_bytes().to_vec())),
        DataValue::String(Some("e".as_bytes().to_vec())),
        DataValue::Struct(vec![
            DataValue::UInt64(Some(1)),
            DataValue::String(Some("e".as_bytes().to_vec())),
        ]),
        DataValue::Struct(vec![
            DataValue::UInt64(Some(3)),
            DataValue::String(Some("o".as_bytes().to_vec())),
        ]),
    ];
    let len = value.len();
    for i in 0..len {
        assert_eq!(value[i], expected[i]);
    }
    Ok(())
}
