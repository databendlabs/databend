use arrow2::datatypes::DataType;
use arrow2::scalar::BinaryScalar;
use arrow2::scalar::Scalar;

#[allow(clippy::eq_op)]
#[test]
fn equal() {
    let a = BinaryScalar::<i32>::from(Some("a"));
    let b = BinaryScalar::<i32>::from(None::<&str>);
    assert_eq!(a, a);
    assert_eq!(b, b);
    assert!(a != b);
    let b = BinaryScalar::<i32>::from(Some("b"));
    assert!(a != b);
    assert_eq!(b, b);
}

#[test]
fn basics() {
    let a = BinaryScalar::<i32>::from(Some("a"));

    assert_eq!(a.value(), Some(b"a".as_ref()));
    assert_eq!(a.data_type(), &DataType::Binary);
    assert!(a.is_valid());

    let a = BinaryScalar::<i64>::from(None::<&str>);

    assert_eq!(a.data_type(), &DataType::LargeBinary);
    assert!(!a.is_valid());

    let _: &dyn std::any::Any = a.as_any();
}
