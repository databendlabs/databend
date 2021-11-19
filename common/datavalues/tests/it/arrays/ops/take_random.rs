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

//use common_arrow::arrow::array::ListArray;
use common_datavalues::arrays::get_list_builder;
use common_datavalues::prelude::*;
use common_exception::Result;

#[test]
fn test_take_random() -> Result<()> {
    // Test DFUint16Array
    let df_uint16_array = &DFUInt16Array::new_from_iter(1u16..4u16);
    // Create TakeRandBranch for the array
    let taker = df_uint16_array.take_rand();
    // Call APIs defined in trait TakeRandom
    assert_eq!(Some(1u16), taker.get(0));
    let unsafe_val = unsafe { taker.get_unchecked(0) };
    assert_eq!(1u16, unsafe_val);

    // Test BooleanArray
    let df_bool_array = &DFBooleanArray::new_from_slice(&[true, false, true, false]);
    // Create TakeRandBranch for the array
    let taker = df_bool_array.take_rand();
    assert_eq!(Some(true), taker.get(2));
    let unsafe_val = unsafe { taker.get_unchecked(3) };
    assert!(!unsafe_val);

    // Test ListArray
    let mut builder = get_list_builder(&DataType::UInt16, 12, 3);
    builder.append_series(&Series::new(vec![1_u16, 2, 3]));
    builder.append_series(&Series::new(vec![7_u16, 8, 9]));
    let df_list = &builder.finish();
    // Create TakeRandBranch for the array
    let taker = df_list.take_rand();
    let result = taker.get(1).unwrap();
    let expected = Series::new(vec![7_u16, 8, 9]);
    assert!(result.series_equal(&expected));
    // Test get_unchecked
    let result = unsafe { taker.get_unchecked(0) };
    let expected = Series::new(vec![1_u16, 2, 3]);
    assert!(result.series_equal(&expected));

    // Test DFStringArray
    let mut string_builder = StringArrayBuilder::with_capacity(3);
    string_builder.append_value("1a");
    string_builder.append_value("2b");
    string_builder.append_value("3c");
    let df_string_array = &string_builder.finish();
    // Create TakeRandBranch for the array
    let taker = df_string_array.take_rand();
    assert_eq!(Some("1a".as_bytes()), taker.get(0));
    // Test get_unchecked
    let result = unsafe { taker.get_unchecked(1) };
    assert_eq!(b"2b", result);

    Ok(())
}
