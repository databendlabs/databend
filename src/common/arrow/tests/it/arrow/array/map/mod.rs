use arrow2::array::*;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;

#[test]
fn basics() {
    let dt = DataType::Struct(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    let data_type = DataType::Map(Box::new(Field::new("a", dt.clone(), true)), false);

    let field = StructArray::new(
        dt.clone(),
        vec![
            Box::new(Utf8Array::<i32>::from_slice(["a", "aa", "aaa"])) as _,
            Box::new(Utf8Array::<i32>::from_slice(["b", "bb", "bbb"])),
        ],
        None,
    );

    let array = MapArray::new(
        data_type,
        vec![0, 1, 2].try_into().unwrap(),
        Box::new(field),
        None,
    );

    assert_eq!(
        array.value(0),
        Box::new(StructArray::new(
            dt.clone(),
            vec![
                Box::new(Utf8Array::<i32>::from_slice(["a"])) as _,
                Box::new(Utf8Array::<i32>::from_slice(["b"])),
            ],
            None,
        )) as Box<dyn Array>
    );

    let sliced = array.sliced(1, 1);
    assert_eq!(
        sliced.value(0),
        Box::new(StructArray::new(
            dt,
            vec![
                Box::new(Utf8Array::<i32>::from_slice(["aa"])) as _,
                Box::new(Utf8Array::<i32>::from_slice(["bb"])),
            ],
            None,
        )) as Box<dyn Array>
    );
}
