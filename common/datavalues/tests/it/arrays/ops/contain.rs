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
fn test_contain() -> Result<()> {
    // Create DFUint16Array
    let df_uint16_array = &DFUInt16Array::new_from_iter(1u16..4u16);
    // Create ListArray
    let mut builder = get_list_builder(&DataType::UInt16, 3, 1);
    builder.append_series(&Series::new(vec![1_u16, 2, 5]));
    builder.append_series(&Series::new(vec![0_u16, 3]));
    builder.append_series(&Series::new(vec![3_u16, 4]));
    let df_list = builder.finish();

    let boolean = df_uint16_array.contain(&df_list);
    let values = boolean?.collect_values();
    assert_eq!(&[Some(true), Some(false), Some(true)], values.as_slice());

    // Test DFStringArray
    let mut string_builder = StringArrayBuilder::with_capacity(3);
    string_builder.append_value("1a");
    string_builder.append_value("2b");
    string_builder.append_value("3c");
    string_builder.append_value("4d");
    let df_string_array = string_builder.finish();

    let mut builder = get_list_builder(&DataType::String, 12, 1);
    builder.append_series(&Series::new(vec!["2b", "4d"]));
    builder.append_series(&Series::new(vec!["2b", "4d"]));
    builder.append_series(&Series::new(vec!["2b", "4d"]));
    builder.append_series(&Series::new(vec!["2b", "4d"]));
    let df_list = builder.finish();

    let boolean = df_string_array.contain(&df_list);
    let values = boolean?.collect_values();
    assert_eq!(
        &[Some(false), Some(true), Some(false), Some(true)],
        values.as_slice()
    );

    Ok(())
}
