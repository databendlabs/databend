use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;
use arrow2::scalar::BooleanScalar;
use arrow2::scalar::Scalar;
use arrow2::scalar::StructScalar;

#[allow(clippy::eq_op)]
#[test]
fn equal() {
    let dt = DataType::Struct(vec![Field::new("a", DataType::Boolean, true)]);
    let a = StructScalar::new(
        dt.clone(),
        Some(vec![
            Box::new(BooleanScalar::from(Some(true))) as Box<dyn Scalar>
        ]),
    );
    let b = StructScalar::new(dt.clone(), None);
    assert_eq!(a, a);
    assert_eq!(b, b);
    assert!(a != b);
    let b = StructScalar::new(
        dt,
        Some(vec![
            Box::new(BooleanScalar::from(Some(false))) as Box<dyn Scalar>
        ]),
    );
    assert!(a != b);
    assert_eq!(b, b);
}

#[test]
fn basics() {
    let dt = DataType::Struct(vec![Field::new("a", DataType::Boolean, true)]);

    let values = vec![Box::new(BooleanScalar::from(Some(true))) as Box<dyn Scalar>];

    let a = StructScalar::new(dt.clone(), Some(values.clone()));

    assert_eq!(a.values(), &values);
    assert_eq!(a.data_type(), &dt);
    assert!(a.is_valid());

    let _: &dyn std::any::Any = a.as_any();
}
