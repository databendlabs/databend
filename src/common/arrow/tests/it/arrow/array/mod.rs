mod binary;
mod boolean;
mod dictionary;
mod equal;
mod fixed_size_binary;
mod fixed_size_list;
mod growable;
mod list;
mod map;
mod ord;
mod primitive;
mod struct_;
mod union;
mod utf8;

use arrow2::array::clone;
use arrow2::array::new_empty_array;
use arrow2::array::new_null_array;
use arrow2::array::Array;
use arrow2::array::PrimitiveArray;
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;
use arrow2::datatypes::UnionMode;

#[test]
fn nulls() {
    let datatypes = vec![
        DataType::Int32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Binary,
        DataType::List(Box::new(Field::new("a", DataType::Binary, true))),
    ];
    let a = datatypes
        .into_iter()
        .all(|x| new_null_array(x, 10).null_count() == 10);
    assert!(a);

    // unions' null count is always 0
    let datatypes = vec![
        DataType::Union(
            vec![Field::new("a", DataType::Binary, true)],
            None,
            UnionMode::Dense,
        ),
        DataType::Union(
            vec![Field::new("a", DataType::Binary, true)],
            None,
            UnionMode::Sparse,
        ),
    ];
    let a = datatypes
        .into_iter()
        .all(|x| new_null_array(x, 10).null_count() == 0);
    assert!(a);
}

#[test]
fn empty() {
    let datatypes = vec![
        DataType::Int32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Binary,
        DataType::List(Box::new(Field::new("a", DataType::Binary, true))),
        DataType::List(Box::new(Field::new(
            "a",
            DataType::Extension("ext".to_owned(), Box::new(DataType::Int32), None),
            true,
        ))),
        DataType::Union(
            vec![Field::new("a", DataType::Binary, true)],
            None,
            UnionMode::Sparse,
        ),
        DataType::Union(
            vec![Field::new("a", DataType::Binary, true)],
            None,
            UnionMode::Dense,
        ),
        DataType::Struct(vec![Field::new("a", DataType::Int32, true)]),
    ];
    let a = datatypes.into_iter().all(|x| new_empty_array(x).len() == 0);
    assert!(a);
}

#[test]
fn empty_extension() {
    let datatypes = vec![
        DataType::Int32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Binary,
        DataType::List(Box::new(Field::new("a", DataType::Binary, true))),
        DataType::Union(
            vec![Field::new("a", DataType::Binary, true)],
            None,
            UnionMode::Sparse,
        ),
        DataType::Union(
            vec![Field::new("a", DataType::Binary, true)],
            None,
            UnionMode::Dense,
        ),
        DataType::Struct(vec![Field::new("a", DataType::Int32, true)]),
    ];
    let a = datatypes
        .into_iter()
        .map(|dt| DataType::Extension("ext".to_owned(), Box::new(dt), None))
        .all(|x| {
            let a = new_empty_array(x);
            a.len() == 0 && matches!(a.data_type(), DataType::Extension(_, _, _))
        });
    assert!(a);
}

#[test]
fn test_clone() {
    let datatypes = vec![
        DataType::Int32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Binary,
        DataType::List(Box::new(Field::new("a", DataType::Binary, true))),
    ];
    let a = datatypes
        .into_iter()
        .all(|x| clone(new_null_array(x.clone(), 10).as_ref()) == new_null_array(x, 10));
    assert!(a);
}

#[test]
fn test_with_validity() {
    let arr = PrimitiveArray::from_slice([1i32, 2, 3]);
    let validity = Bitmap::from(&[true, false, true]);
    let arr = arr.with_validity(Some(validity));
    let arr_ref = arr.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();

    let expected = PrimitiveArray::from(&[Some(1i32), None, Some(3)]);
    assert_eq!(arr_ref, &expected);
}

// check that we ca derive stuff
#[derive(PartialEq, Clone, Debug)]
struct A {
    array: Box<dyn Array>,
}
