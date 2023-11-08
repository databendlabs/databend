mod binary;
mod boolean;
mod dictionary;
mod fixed_binary;
mod fixed_size_list;
mod list;
mod map;
mod null;
mod primitive;
mod struct_;
mod union;
mod utf8;

use arrow2::array::growable::make_growable;
use arrow2::array::*;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;

#[test]
fn test_make_growable() {
    let array = Int32Array::from_slice([1, 2]);
    make_growable(&[&array], false, 2);

    let array = Utf8Array::<i32>::from_slice(["a", "aa"]);
    make_growable(&[&array], false, 2);

    let array = Utf8Array::<i64>::from_slice(["a", "aa"]);
    make_growable(&[&array], false, 2);

    let array = BinaryArray::<i32>::from_slice([b"a".as_ref(), b"aa".as_ref()]);
    make_growable(&[&array], false, 2);

    let array = BinaryArray::<i64>::from_slice([b"a".as_ref(), b"aa".as_ref()]);
    make_growable(&[&array], false, 2);

    let array = BinaryArray::<i64>::from_slice([b"a".as_ref(), b"aa".as_ref()]);
    make_growable(&[&array], false, 2);

    let array =
        FixedSizeBinaryArray::new(DataType::FixedSizeBinary(2), b"abcd".to_vec().into(), None);
    make_growable(&[&array], false, 2);
}

#[test]
fn test_make_growable_extension() {
    let array = DictionaryArray::try_from_keys(
        Int32Array::from_slice([1, 0]),
        Int32Array::from_slice([1, 2]).boxed(),
    )
    .unwrap();
    make_growable(&[&array], false, 2);

    let data_type = DataType::Extension("ext".to_owned(), Box::new(DataType::Int32), None);
    let array = Int32Array::from_slice([1, 2]).to(data_type.clone());
    let array_grown = make_growable(&[&array], false, 2).as_box();
    assert_eq!(array_grown.data_type(), &data_type);

    let data_type = DataType::Extension(
        "ext".to_owned(),
        Box::new(DataType::Struct(vec![Field::new(
            "a",
            DataType::Int32,
            false,
        )])),
        None,
    );
    let array = StructArray::new(
        data_type.clone(),
        vec![Int32Array::from_slice([1, 2]).boxed()],
        None,
    );
    let array_grown = make_growable(&[&array], false, 2).as_box();
    assert_eq!(array_grown.data_type(), &data_type);
}
