use arrow2::array::*;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;

#[test]
fn push() {
    let c1 = Box::new(MutablePrimitiveArray::<i32>::new()) as Box<dyn MutableArray>;
    let values = vec![c1];
    let data_type = DataType::Struct(vec![Field::new("f1", DataType::Int32, true)]);
    let mut a = MutableStructArray::new(data_type, values);

    a.value::<MutablePrimitiveArray<i32>>(0)
        .unwrap()
        .push(Some(1));
    a.push(true);
    a.value::<MutablePrimitiveArray<i32>>(0).unwrap().push(None);
    a.push(false);
    a.value::<MutablePrimitiveArray<i32>>(0)
        .unwrap()
        .push(Some(2));
    a.push(true);

    assert_eq!(a.len(), 3);
    assert!(a.is_valid(0));
    assert!(!a.is_valid(1));
    assert!(a.is_valid(2));

    assert_eq!(
        a.value::<MutablePrimitiveArray<i32>>(0).unwrap().values(),
        &Vec::from([1, 0, 2])
    );
}
