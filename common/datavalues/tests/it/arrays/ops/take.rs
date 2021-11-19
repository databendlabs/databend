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
fn test_take() -> Result<()> {
    // Test DFUint16Array
    let df_uint16_array = &DFUInt16Array::new_from_iter(1u16..6u16);
    let index = TakeIdx::from(vec![0, 1].into_iter());
    let take_res = df_uint16_array.take(index)?;
    let values = take_res.collect_values();
    assert_eq!(&[Some(1u16), Some(2u16)], values.as_slice());
    let index = TakeIdx::from(vec![2, 4].into_iter());
    let take_res = unsafe { df_uint16_array.take_unchecked(index) };
    let values = take_res?.collect_values();
    assert_eq!(&[Some(3u16), Some(5u16)], values.as_slice());

    // Test BooleanArray
    let df_bool_array = &DFBooleanArray::new_from_slice(&[true, false, true, false]);
    let index = TakeIdx::from(vec![0, 1].into_iter());
    let take_res = df_bool_array.take(index)?;
    let values = take_res.collect_values();
    assert_eq!(&[Some(true), Some(false)], values.as_slice());
    let index = TakeIdx::from(vec![2, 3].into_iter());
    let take_res = unsafe { df_bool_array.take_unchecked(index) };
    let values = take_res?.collect_values();
    assert_eq!(&[Some(true), Some(false)], values.as_slice());

    // Test ListArray
    let mut builder = get_list_builder(&DataType::UInt16, 12, 3);
    builder.append_series(&Series::new(vec![1_u16, 2, 3]));
    builder.append_series(&Series::new(vec![7_u16, 8, 9]));
    let df_list = &builder.finish();
    let index = TakeIdx::from(vec![0].into_iter());
    let take_res = df_list.take(index)?;
    let vs: Vec<_> = take_res.into_no_null_iter().collect();
    let expected = Series::new(vec![1_u16, 2, 3]);
    assert!(vs[0].series_equal(&expected));

    let index = TakeIdx::from(vec![1].into_iter());
    let take_res = unsafe { df_list.take_unchecked(index)? };
    let vs: Vec<_> = take_res.into_no_null_iter().collect();
    let expected = Series::new(vec![7_u16, 8, 9]);
    assert!(vs[0].series_equal(&expected));

    // Test DFStringArray
    let mut string_builder = StringArrayBuilder::with_capacity(3);
    string_builder.append_value("1a");
    string_builder.append_value("2b");
    string_builder.append_value("3c");
    let df_string_array = &string_builder.finish();
    let index = TakeIdx::from(vec![0, 1].into_iter());
    let take_res = df_string_array.take(index)?;
    let vs: Vec<_> = take_res.into_no_null_iter().collect();
    assert_eq!(&vs, &[b"1a", b"2b"]);

    let index = TakeIdx::from(vec![2, 1].into_iter());
    let take_res = unsafe { df_string_array.take_unchecked(index)? };
    let vs: Vec<_> = take_res.into_no_null_iter().collect();
    assert_eq!(&vs, &[b"3c", b"2b"]);

    Ok(())
}
