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

use common_datavalues::arrays::get_list_builder;
use common_datavalues::prelude::*;
use common_exception::Result;

#[test]
fn test_scatter() -> Result<()> {
    // Test DFUint16Array
    let df_uint16_array = DFUInt16Array::new_from_iter(1u16..11u16);
    // Create the indice array
    let indices = vec![1, 2, 3, 1, 3, 2, 0, 3, 1, 0];
    // The number of rows should be equal to the length of indices
    assert_eq!(df_uint16_array.len(), indices.len());

    let array_vec = unsafe { df_uint16_array.scatter_unchecked(&mut indices.into_iter(), 4)? };
    assert_eq!(&[7u16, 10], array_vec[0].inner().values().as_slice());
    assert_eq!(&[1u16, 4, 9], array_vec[1].inner().values().as_slice());
    assert_eq!(&[2u16, 6], array_vec[2].inner().values().as_slice());
    assert_eq!(&[3u16, 5, 8], array_vec[3].inner().values().as_slice());

    // Test DFStringArray
    let df_string_array = DFStringArray::new_from_slice(&["a", "b", "c", "d"]);
    let indices = vec![1, 0, 1, 1];
    assert_eq!(df_string_array.len(), indices.len());

    let array_vec = unsafe { df_string_array.scatter_unchecked(&mut indices.into_iter(), 2)? };
    let v1: Vec<&[u8]> = array_vec[0].into_no_null_iter().collect();
    let v2: Vec<&[u8]> = array_vec[1].into_no_null_iter().collect();
    assert_eq!(vec![b"b"], v1);
    assert_eq!(vec![b"a", b"c", b"d"], v2);

    // Test BooleanArray
    let df_bool_array = DFBooleanArray::new_from_slice(&[true, false, true, false]);
    let indices = vec![1, 0, 0, 1];
    assert_eq!(df_bool_array.len(), indices.len());

    let array_vec = unsafe { df_bool_array.scatter_unchecked(&mut indices.into_iter(), 2)? };
    assert_eq!(
        vec![false, true],
        array_vec[0].into_no_null_iter().collect::<Vec<bool>>()
    );
    assert_eq!(
        vec![true, false],
        array_vec[1].into_no_null_iter().collect::<Vec<bool>>()
    );

    // Test StringArray
    let mut string_builder = StringArrayBuilder::with_capacity(8);
    string_builder.append_value(&"12");
    string_builder.append_value(&"ab");
    string_builder.append_value(&"c1");
    string_builder.append_value(&"32");
    let df_string_array = string_builder.finish();
    let indices = vec![1, 0, 0, 1];
    let array_vec = unsafe { df_string_array.scatter_unchecked(&mut indices.into_iter(), 2)? };

    let values: Vec<Vec<u8>> = (0..array_vec[0].len())
        .map(|idx| array_vec[0].inner().value(idx).to_vec())
        .collect();

    assert_eq!(vec![b"ab".to_vec(), b"c1".to_vec()], values);

    let values: Vec<Vec<u8>> = (0..array_vec[1].len())
        .map(|idx| array_vec[1].inner().value(idx).to_vec())
        .collect();
    assert_eq!(vec![b"12".to_vec(), b"32".to_vec()], values);

    // Test LargeListArray
    let mut builder = get_list_builder(&DataType::UInt16, 12, 3);
    builder.append_series(&Series::new(vec![1_u16, 2, 3]));
    builder.append_series(&Series::new(vec![7_u16, 8, 9]));
    builder.append_series(&Series::new(vec![10_u16, 11, 12]));
    builder.append_series(&Series::new(vec![4_u16, 5, 6]));
    let df_list = builder.finish();

    let indices = vec![1, 0, 0, 1];
    let array_vec = unsafe { df_list.scatter_unchecked(&mut indices.into_iter(), 2)? };

    let c0 = array_vec[0].inner().value(0);
    let c0 = DFUInt16Array::from_arrow_array(c0.as_ref());
    let c1 = array_vec[1].inner().value(0);
    let c1 = DFUInt16Array::from_arrow_array(c1.as_ref());

    assert_eq!(&[7, 8, 9], c0.inner().values().as_slice());
    assert_eq!(&[1, 2, 3], c1.inner().values().as_slice());

    let c0 = array_vec[0].inner().value(1);
    let c0 = DFUInt16Array::from_arrow_array(c0.as_ref());
    let c1 = array_vec[1].inner().value(1);
    let c1 = DFUInt16Array::from_arrow_array(c1.as_ref());

    assert_eq!(&[10, 11, 12], c0.inner().values().as_slice());
    assert_eq!(&[4, 5, 6], c1.inner().values().as_slice());

    Ok(())
}
