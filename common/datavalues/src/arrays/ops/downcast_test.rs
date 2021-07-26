// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::BinaryArray;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::Int32Array;
use common_arrow::arrow::array::ListArray;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::array::StringArray;
use common_arrow::arrow::array::StructArray;
use common_arrow::arrow::datatypes::DataType;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::record_batch::RecordBatch;
use common_exception::Result;

use crate::arrays::builders::*;
use crate::prelude::*;
use crate::DFBinaryArray;
use crate::DFBooleanArray;
use crate::DFListArray;
use crate::DFStructArray;
use crate::DFUInt16Array;
use crate::DFUtf8Array;
use crate::Int32Type;
use crate::UInt16Type;

#[test]
fn test_array_as_ref() -> Result<()> {
    // Test DFUint16Array
    let df_uint16_array = DFUInt16Array::new_from_iter(1u16..4u16);
    let arrow_uint16_array: &PrimitiveArray<UInt16Type> = df_uint16_array.as_ref();
    assert_eq!(&[1u16, 2, 3], &arrow_uint16_array.values());

    // Test DFBooleanArray
    let mut boolean_builder = BooleanArrayBuilder::new(3);
    boolean_builder.append_value(true);
    boolean_builder.append_value(false);
    boolean_builder.append_value(true);
    let df_boolean_array = boolean_builder.finish();
    let arrow_boolean_array: &BooleanArray = df_boolean_array.as_ref();
    // 5 means 0b_101
    assert_eq!(&[5], &arrow_boolean_array.values().as_slice());

    // Test DFUtf8Array
    let mut utf8_builder = Utf8ArrayBuilder::new(3, 2);
    utf8_builder.append_value("1a");
    utf8_builder.append_value("2b");
    utf8_builder.append_value("3c");
    let df_utf8_array = utf8_builder.finish();
    let arrow_string_array = df_utf8_array.as_ref();
    assert_eq!(
        &"1a2b3c".as_bytes(),
        &arrow_string_array.value_data().as_slice()
    );

    // Test DFListArray
    let mut list_value_builder = ArrowPrimitiveArrayBuilder::<UInt16Type>::new(3);
    list_value_builder.append_slice(&[1u16, 2u16, 3u16]);
    let mut list_builder = ListPrimitiveArrayBuilder::<UInt16Type>::new(list_value_builder, 3);
    let df_list = list_builder.finish();
    let arrow_list = df_list.as_ref();
    let expected = "PrimitiveArray<UInt16>\n[\n  1,\n  2,\n  3,\n]";
    assert_eq!(expected, format!("{:?}", arrow_list.values()));

    // Test DFBinaryArray
    let mut binary_builder = BinaryArrayBuilder::new(8);
    binary_builder.append_value(&"123");
    let df_binary_array = binary_builder.finish();
    let arrow_binary_array = df_binary_array.as_ref();
    assert_eq!(
        &[0x31, 0x32, 0x33],
        &arrow_binary_array.value_data().as_slice()
    );

    // TODO: Test DFStructArray

    Ok(())
}

#[test]
// Test from_arrow_array() and collect_values() which calls downcast_iter()
fn test_array_downcast() -> Result<()> {
    // Test PrimitiveArray
    let vec_uint16 = vec![1u16, 2u16, 3u16];
    let arrow_array = PrimitiveArray::<UInt16Type>::from_iter_values(vec_uint16.into_iter());
    let df_array = DFUInt16Array::from_arrow_array(arrow_array);
    let values = df_array.collect_values();
    assert_eq!(&[Some(1u16), Some(2u16), Some(3u16)], values.as_slice());

    // Test BooleanArray
    let vec_bool = vec![true, false, true];
    let arrow_bool_array = BooleanArray::from(vec_bool);
    let df_bool_array = DFBooleanArray::from_arrow_array(arrow_bool_array);
    let values = df_bool_array.collect_values();
    assert_eq!(&[Some(true), Some(false), Some(true)], values.as_slice());

    // Test Utf8Array
    let vec_str = vec![Some("foo"), None, Some("bar")];
    let arrow_str_array = StringArray::from(vec_str);
    let df_bool_array = DFUtf8Array::from_arrow_array(arrow_str_array);
    let values = df_bool_array.collect_values();
    assert_eq!(&[Some("foo"), None, Some("bar")], values.as_slice());

    // Test ListArray
    let data = vec![
        Some(vec![Some(0), Some(1), Some(2)]),
        Some(Vec::<Option<i32>>::new()),
        Some(vec![Some(3), None, Some(5)]),
        Some(vec![Some(6), Some(7)]),
    ];
    let arrow_list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);
    let df_list_array = DFListArray::from_arrow_array(arrow_list_array);
    let values: Vec<Option<Series>> = df_list_array.downcast_iter().collect();

    let expected = vec![
        Series::new(vec![Some(0), Some(1), Some(2)]),
        Series::new(Vec::<Option<i32>>::new()),
        Series::new(vec![Some(3), None, Some(5)]),
        Series::new(vec![Some(6), Some(7)]),
    ];
    for i in 0..expected.len() {
        assert_eq!(
            &values[i].as_ref().unwrap().to_values()?,
            &expected[i].to_values()?
        );
    }

    // Test BinaryArray
    let arrow_binary_array = BinaryArray::from_opt_vec(vec![Some(b"1a"), None, Some(b"2b")]);
    let df_binary_array = DFBinaryArray::from_arrow_array(arrow_binary_array);
    let downcast_array = df_binary_array.downcast_ref();
    assert_eq!(
        &[0x31, 0x61, 0x32, 0x62],
        downcast_array.value_data().as_slice()
    );

    // Test StructArray
    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)])?;

    let arrow_struct_array = StructArray::from(batch);
    let df_struct_array = DFStructArray::from_arrow_array(arrow_struct_array);
    let downcast_struct_array = df_struct_array.downcast_ref();
    let expected = "StructArray\n[\n-- child 0: \"id\" (Int32)\nPrimitiveArray<Int32>\n[\n  1,\n  2,\n  3,\n  4,\n  5,\n]\n]";
    assert_eq!(expected, format!("{:?}", downcast_struct_array));

    Ok(())
}
