// Copyright 2020-2022 Jorge C. Leitão
// Copyright 2021 Datafuse Labs
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

use arrow_array::ArrayRef;
use arrow_data::ArrayDataBuilder;
use databend_common_arrow::arrow::array::*;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::datatypes::DataType;
use databend_common_arrow::arrow::datatypes::Field;
use databend_common_arrow::arrow::datatypes::IntegerType;
use databend_common_arrow::arrow::datatypes::TimeUnit;
use databend_common_arrow::arrow::datatypes::UnionMode;
use databend_common_arrow::arrow::offset::Offsets;
use proptest::num::i32;

fn test_arrow2_roundtrip(array: &dyn arrow_array::Array) {
    let arrow2 = Box::<dyn Array>::from(array);
    assert_eq!(arrow2.len(), array.len());

    let back = ArrayRef::from(arrow2);
    assert_eq!(back.len(), array.len());

    match array.data_type() {
        d @ arrow_schema::DataType::Union(_, arrow_schema::UnionMode::Sparse) => {
            // Temporary workaround https://github.com/apache/arrow-rs/issues/4044
            let data = array.to_data();
            let type_ids = data.buffers()[0].slice_with_length(data.offset(), data.len());
            let child_data = data
                .child_data()
                .iter()
                .map(|x| x.slice(data.offset(), data.len()))
                .collect();

            let data = ArrayDataBuilder::new(d.clone())
                .len(data.len())
                .buffers(vec![type_ids])
                .child_data(child_data)
                .build()
                .unwrap();

            assert_eq!(back.to_data(), data);
        }
        _ => assert_eq!(array, back.as_ref()),
    }
    assert_eq!(array.data_type(), back.data_type());
}

fn test_arrow_roundtrip(array: &dyn Array) {
    let arrow = ArrayRef::from(array);
    assert_eq!(arrow.len(), array.len());

    let back = Box::<dyn Array>::from(arrow);
    assert_eq!(back.len(), array.len());

    assert_eq!(array, back.as_ref());
    assert_eq!(array.data_type(), back.data_type());
}

fn test_conversion(array: &dyn Array) {
    test_arrow_roundtrip(array);
    let to_arrow = ArrayRef::from(array);
    test_arrow2_roundtrip(to_arrow.as_ref());

    if !array.is_empty() {
        let sliced = array.sliced(1, array.len() - 1);
        test_arrow_roundtrip(sliced.as_ref());

        let sliced = to_arrow.slice(1, array.len() - 1);
        test_arrow2_roundtrip(sliced.as_ref());

        let sliced = array.sliced(0, array.len() - 1);
        test_arrow_roundtrip(sliced.as_ref());

        let sliced = to_arrow.slice(0, array.len() - 1);
        test_arrow2_roundtrip(sliced.as_ref());
    }

    if array.len() > 2 {
        let sliced = array.sliced(1, array.len() - 2);
        test_arrow_roundtrip(sliced.as_ref());

        let sliced = to_arrow.slice(1, array.len() - 2);
        test_arrow2_roundtrip(sliced.as_ref());
    }
}

#[test]
fn test_null() {
    let data_type = DataType::Null;
    let array = NullArray::new(data_type, 7);
    test_conversion(&array);
}

#[test]
fn test_primitive() {
    let data_type = DataType::Int32;
    let array = PrimitiveArray::new(data_type, vec![1, 2, 3].into(), None);
    test_conversion(&array);

    let data_type = DataType::Timestamp(TimeUnit::Second, Some("UTC".into()));
    let nulls = Bitmap::from_iter([true, true, false]);
    let array = PrimitiveArray::new(data_type, vec![1_i64, 24, 0].into(), Some(nulls));
    test_conversion(&array);
}

#[test]
fn test_boolean() {
    let data_type = DataType::Boolean;
    let values = [false, false, true, true, true].into_iter().collect();
    let validity = [false, true, true, false, false].into_iter().collect();
    let array = BooleanArray::new(data_type, values, Some(validity));
    test_conversion(&array);
}

#[test]
fn test_utf8() {
    let array = Utf8Array::<i32>::from_iter([Some("asd\0"), None, Some("45\0848"), Some("")]);
    test_conversion(&array);

    let array = Utf8Array::<i64>::from_iter([Some("asd"), None, Some("45\n848"), Some("")]);
    test_conversion(&array);

    let array = Utf8Array::<i32>::new_empty(DataType::Utf8);
    test_conversion(&array);
}

#[test]
fn test_binary() {
    let array = BinaryArray::<i32>::from_iter([Some("s".as_bytes()), Some(b"sd\xFFfk\x23"), None]);
    test_conversion(&array);

    let array = BinaryArray::<i64>::from_iter([Some("45848".as_bytes()), Some(b"\x03\xFF"), None]);
    test_conversion(&array);

    let array = BinaryArray::<i32>::new_empty(DataType::Binary);
    test_conversion(&array);
}

/// Returns a 3 element struct array
fn make_struct() -> StructArray {
    let a1 = BinaryArray::<i32>::from_iter([Some("s".as_bytes()), Some(b"sd\xFFfk\x23"), None]);
    let a2 = BinaryArray::<i64>::from_iter([Some("45848".as_bytes()), Some(b"\x03\xFF"), None]);

    let data_type = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
    let nulls = Bitmap::from_iter([true, true, false]);
    let a3 = PrimitiveArray::new(data_type, vec![1_i64, 24, 0].into(), Some(nulls));

    let nulls = [true, true, false].into_iter().collect();
    StructArray::new(
        DataType::Struct(vec![
            Field::new("a1", a1.data_type().clone(), true),
            Field::new("a2", a2.data_type().clone(), true),
            Field::new("a3", a3.data_type().clone(), true),
        ]),
        vec![Box::new(a1), Box::new(a2), Box::new(a3)],
        Some(nulls),
    )
}

#[test]
fn test_struct() {
    let array = make_struct();
    test_conversion(&array);
}

#[test]
fn test_list() {
    let values = Utf8Array::<i32>::from_iter([
        Some("asd\0"),
        None,
        Some("45\0848"),
        Some(""),
        Some("335"),
        Some("test"),
    ]);

    let validity = [true, true, false, false, true].into_iter().collect();
    let offsets = Offsets::try_from_iter(vec![0, 2, 2, 2, 0]).unwrap();
    let data_type = DataType::List(Box::new(Field::new("element", DataType::Utf8, true)));
    let list = ListArray::<i32>::new(
        data_type.clone(),
        offsets.into(),
        Box::new(values.clone()),
        Some(validity),
    );

    test_conversion(&list);

    let list = ListArray::<i32>::new_empty(data_type);
    test_conversion(&list);

    let validity = [true, true, false, false, true].into_iter().collect();
    let offsets = Offsets::try_from_iter(vec![0, 2, 2, 2, 0]).unwrap();
    let data_type = DataType::LargeList(Box::new(Field::new("element", DataType::Utf8, true)));
    let list = ListArray::<i64>::new(
        data_type.clone(),
        offsets.into(),
        Box::new(values),
        Some(validity),
    );

    test_conversion(&list);

    let list = ListArray::<i64>::new_empty(data_type);
    test_conversion(&list);
}

#[test]
fn test_list_struct() {
    let values = make_struct();
    let validity = [true, true, false, true].into_iter().collect();
    let offsets = Offsets::try_from_iter(vec![0, 1, 0, 2]).unwrap();
    let list = ListArray::<i32>::new(
        DataType::List(Box::new(Field::new(
            "element",
            values.data_type().clone(),
            true,
        ))),
        offsets.into(),
        Box::new(values),
        Some(validity),
    );

    test_conversion(&list);
}

#[test]
fn test_dictionary() {
    let nulls = [true, false, true, true, true].into_iter().collect();
    let keys = PrimitiveArray::new(DataType::Int16, vec![1_i16, 1, 0, 2, 2].into(), Some(nulls));
    let values = make_struct();
    let dictionary = DictionaryArray::try_new(
        DataType::Dictionary(
            IntegerType::Int16,
            Box::new(values.data_type().clone()),
            false,
        ),
        keys,
        Box::new(values),
    )
    .unwrap();

    test_conversion(&dictionary);
}

#[test]
fn test_fixed_size_binary() {
    let data = (0_u8..16).collect::<Vec<_>>();
    let nulls = [false, false, true, true, true, false, false, true]
        .into_iter()
        .collect();

    let array = FixedSizeBinaryArray::new(DataType::FixedSizeBinary(2), data.into(), Some(nulls));
    test_conversion(&array);
}

#[test]
fn test_fixed_size_list() {
    let values = vec![1_i64, 2, 3, 4, 5, 6, 7, 8];
    let nulls = [false, false, true, true, true, true, false, false]
        .into_iter()
        .collect();
    let values = PrimitiveArray::new(DataType::Int64, values.into(), Some(nulls));

    let nulls = [true, true, false, true].into_iter().collect();
    let array = FixedSizeListArray::new(
        DataType::FixedSizeList(Box::new(Field::new("element", DataType::Int64, true)), 2),
        Box::new(values),
        Some(nulls),
    );

    test_conversion(&array);
}

#[test]
fn test_map() {
    let keys = Utf8Array::<i32>::from_iter(
        ["key1", "key2", "key3", "key1", "key2"]
            .into_iter()
            .map(Some),
    );
    let values = PrimitiveArray::<i32>::from_iter([Some(1), None, Some(3), Some(1), None]);
    let fields = StructArray::new(
        DataType::Struct(vec![
            Field::new("keys", DataType::Utf8, false), // Cannot be nullable
            Field::new("values", DataType::Int32, true),
        ]),
        vec![Box::new(keys), Box::new(values)],
        None, // Cannot be nullable
    );

    let validity = [true, true, false, false].into_iter().collect();
    let offsets = Offsets::try_from_iter(vec![0, 2, 0, 2]).unwrap();
    let data_type = DataType::Map(
        Box::new(Field::new("entries", fields.data_type().clone(), true)),
        false,
    );
    let map = MapArray::new(
        data_type.clone(),
        offsets.into(),
        Box::new(fields),
        Some(validity),
    );

    test_conversion(&map);

    let map = MapArray::new_empty(data_type);
    test_conversion(&map);
}

#[test]
fn test_dense_union() {
    let fields = vec![
        Field::new("a1", DataType::Int32, true),
        Field::new("a2", DataType::Int64, true),
    ];

    let a1 = PrimitiveArray::from_iter([Some(2), None]);
    let a2 = PrimitiveArray::from_iter([Some(2_i64), None, Some(3)]);

    let types = vec![1, 0, 0, 1, 1];
    let offsets = vec![0, 0, 1, 1, 2];
    let union = UnionArray::new(
        DataType::Union(fields.clone(), Some(vec![0, 1]), UnionMode::Dense),
        types.into(),
        vec![Box::new(a1.clone()), Box::new(a2.clone())],
        Some(offsets.into()),
    );

    test_conversion(&union);

    let types = vec![1, 4, 4, 1, 1];
    let offsets = vec![0, 0, 1, 1, 2];
    let union = UnionArray::new(
        DataType::Union(fields, Some(vec![4, 1]), UnionMode::Dense),
        types.into(),
        vec![Box::new(a1), Box::new(a2)],
        Some(offsets.into()),
    );

    test_conversion(&union);
}

#[test]
fn test_sparse_union() {
    let fields = vec![
        Field::new("a1", DataType::Int32, true),
        Field::new("a2", DataType::Int64, true),
    ];

    let a1 = PrimitiveArray::from_iter([None, Some(2), None, None, None]);
    let a2 = PrimitiveArray::from_iter([Some(2_i64), None, None, None, Some(3)]);

    let types = vec![1, 0, 0, 1, 1];
    let union = UnionArray::new(
        DataType::Union(fields.clone(), Some(vec![0, 1]), UnionMode::Sparse),
        types.into(),
        vec![Box::new(a1.clone()), Box::new(a2.clone())],
        None,
    );

    test_conversion(&union);

    let types = vec![1, 4, 4, 1, 1];
    let union = UnionArray::new(
        DataType::Union(fields, Some(vec![4, 1]), UnionMode::Sparse),
        types.into(),
        vec![Box::new(a1), Box::new(a2)],
        None,
    );

    test_conversion(&union);
}
