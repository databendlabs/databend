use arrow2::array::growable::Growable;
use arrow2::array::growable::GrowableNull;
use arrow2::array::NullArray;
use arrow2::datatypes::DataType;

#[test]
fn null() {
    let mut mutable = GrowableNull::default();

    mutable.extend(0, 1, 2);
    mutable.extend(1, 0, 1);
    assert_eq!(mutable.len(), 3);

    let result: NullArray = mutable.into();

    let expected = NullArray::new(DataType::Null, 3);
    assert_eq!(result, expected);
}
