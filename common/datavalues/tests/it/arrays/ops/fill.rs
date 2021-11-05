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
fn test_array_fill() -> Result<()> {
    // Test full for PrimitiveArray
    let mut df_uint16_array = DFUInt16Array::full(5u16, 3);
    let arrow_uint16_array = df_uint16_array.inner();
    assert_eq!(&[5u16, 5u16, 5u16], &arrow_uint16_array.values().as_slice());
    // Test full_null for PrimitiveArray
    df_uint16_array = DFUInt16Array::full_null(3);
    assert_eq!(3, df_uint16_array.null_count());
    assert!(df_uint16_array.is_null(0));
    assert!(df_uint16_array.is_null(1));
    assert!(df_uint16_array.is_null(2));

    // Test full for BooleanArray
    let mut df_boolean_array = DFBooleanArray::full(true, 3);
    assert_eq!(0, df_boolean_array.null_count());

    // Test full_null for BooleanArray
    df_boolean_array = DFBooleanArray::full_null(3);
    assert_eq!(3, df_boolean_array.null_count());
    assert!(df_boolean_array.is_null(0));
    assert!(df_boolean_array.is_null(1));
    assert!(df_boolean_array.is_null(2));

    // Test full for StringArray
    let mut df_string_array = DFStringArray::full("ab".as_bytes(), 3);
    assert_eq!(0, df_string_array.null_count());
    assert!(!df_string_array.is_null(0));
    assert!(!df_string_array.is_null(1));
    assert!(!df_string_array.is_null(2));
    assert_eq!("ab".as_bytes(), df_string_array.inner().value(0));
    assert_eq!("ab".as_bytes(), df_string_array.inner().value(1));
    assert_eq!("ab".as_bytes(), df_string_array.inner().value(2));

    // Test full_null for StringArray
    df_string_array = DFStringArray::full_null(3);
    assert_eq!(3, df_string_array.null_count());
    assert!(df_string_array.is_null(0));
    assert!(df_string_array.is_null(1));
    assert!(df_string_array.is_null(2));

    Ok(())
}
